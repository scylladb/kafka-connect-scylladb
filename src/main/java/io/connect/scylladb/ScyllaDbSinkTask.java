package io.connect.scylladb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import io.connect.scylladb.utils.VersionUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task class for ScyllaDB Sink Connector.
 */
public class ScyllaDbSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSinkTask.class);

  private ScyllaDbSinkConnectorConfig config;
  private Map<TopicPartition, OffsetAndMetadata> topicOffsets;
  private ErrantRecordReporter reporter;
  private final List<Future<Void>> reportedRecords = new ArrayList<>();
  private int consecutiveFailures;
  // Visible for testing
  ScyllaDbSession session;

  /**
   * Starts the sink task. 
   * If <code>scylladb.offset.storage.table.enable</code> is set to true, 
   * the task will load offsets for each Kafka topic-partition from
   * ScyllaDB offset table into task context.
   */
  @Override
  public void start(Map<String, String> settings) {
    this.config = new ScyllaDbSinkConnectorConfig(settings);
    this.reporter = errantRecordReporter();
    this.consecutiveFailures = 0;
    if (config.isOffsetEnabledInScyllaDb()) {
      Set<TopicPartition> assignment = context.assignment();
      this.session = getValidSession();
      Map<TopicPartition, Long> offsets = session.loadOffsets(assignment);
      if (!offsets.isEmpty()) {
        context.offset(offsets);
      }
    }
    topicOffsets = new HashMap<>();
  }

  private ErrantRecordReporter errantRecordReporter() {
    try {
      return context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError ex) {
      // Connect runtimes older than 2.6 have no errant record reporter.
      log.warn("Errant record reporter is not available, using behavior.on.error.");
      return null;
    }
  }

  /*
   * Returns a ScyllaDB session. 
   * Creates a session, if not already exists.
   * In case the when session is not valid, 
   * it closes the existing session and creates a new one.
   */
  private ScyllaDbSession getValidSession() {
    
    ScyllaDbSessionFactory sessionFactory = new ScyllaDbSessionFactory();

    if (session == null) {
      log.info("Creating ScyllaDb Session.");
      session = sessionFactory.newSession(this.config);
    } 
    
    if (!session.isValid()) {
      log.warn("ScyllaDb Session is invalid. Closing and creating new.");
      close();
      session = sessionFactory.newSession(this.config);
    }
    return session;
  }

  /**
   * <ol>
   * <li>Validates the kafka records.
   * <li>Writes or deletes records from Kafka topic into ScyllaDB. 
   * <li>Requests to commit the records when the scyllaDB operations are successful.
   * </ol>
   */
  @Override
  public void put(Collection<SinkRecord> records) {
    final List<SinkRecord> written = new ArrayList<>(records.size());
    final List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>(records.size());
    final List<SinkRecord> failedRecords = new ArrayList<>();
    final List<Throwable> failures = new ArrayList<>();
    reportedRecords.clear();
    // The first failure that ends the batch; a fatal one found later takes its place.
    Throwable batchError = null;

    for (SinkRecord record : records) {
      final ScyllaDbSession validSession;
      try {
        validSession = getValidSession();
      } catch (RuntimeException ex) {
        batchError = ex;
        break;
      }
      try {
        ScyllaDbSinkTaskHelper scyllaDbSinkTaskHelper = new ScyllaDbSinkTaskHelper(config, validSession);
        scyllaDbSinkTaskHelper.validateRecord(record);

        BoundStatement boundStatement = scyllaDbSinkTaskHelper.getBoundStatementForRecord(record);
        if (boundStatement == null) {
          topicOffsets.put(new TopicPartition(record.topic(), record.kafkaPartition()),
                  new OffsetAndMetadata(record.kafkaOffset() + 1));
          continue;
        }
        log.trace("put() - Executing Bound Statement {} for {}:{}:{}",
                boundStatement.getPreparedStatement().getQuery(),
                record.topic(),
                record.kafkaPartition(),
                record.kafkaOffset()
        );

        // Commit offset in the case of successful processing of sink record
        topicOffsets.put(new TopicPartition(record.topic(), record.kafkaPartition()),
                new OffsetAndMetadata(record.kafkaOffset() + 1));

        futures.add(validSession.executeStatementAsync(boundStatement));
        written.add(record);
      } catch (RuntimeException ex) {
        if (WriteErrors.classifyConversion(ex) != WriteErrors.Kind.RECORD) {
          // Writes already sent may have failed for a fatal reason; check them before giving up.
          batchError = ex;
          break;
        }
        failedRecords.add(record);
        failures.add(ex);
      }
    }

    log.debug("put() - Checking future(s)");
    for (int i = 0; i < futures.size(); i++) {
      try {
        futures.get(i).toCompletableFuture().get(this.config.statementTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ex) {
        // The worker is stopping the task; this is not a failed write.
        Thread.currentThread().interrupt();
        throw new RetriableException("put() - Interrupted while writing to ScyllaDB.", ex);
      } catch (ExecutionException | TimeoutException ex) {
        if (WriteErrors.classify(ex) == WriteErrors.Kind.RECORD) {
          failedRecords.add(written.get(i));
          failures.add(ex);
        } else if (batchError == null || WriteErrors.classify(ex) == WriteErrors.Kind.FATAL) {
          batchError = ex;
        }
      }
    }
    // Record errors are reported only once the whole batch is written, so a retried batch
    // does not report them twice.
    if (batchError != null) {
      throw failBatch(batchError);
    }
    for (int i = 0; i < failedRecords.size(); i++) {
      handleRecordError(failedRecords.get(i), failures.get(i));
    }
    awaitReportedRecords();
    consecutiveFailures = 0;
    if (!futures.isEmpty()) {
      context.requestCommit();
    }
  }

  /**
   * Builds the exception that ends a batch that failed as a whole: a {@link RetriableException}
   * after asking the framework to back off when the failure is transient, otherwise (rejected
   * credentials, invalid configuration, an unexpected error, or {@code scylladb.max.retries}
   * used up) a {@link ConnectException}.
   */
  private ConnectException failBatch(Throwable error) {
    Throwable cause = WriteErrors.unwrap(error);
    if (WriteErrors.classify(cause) != WriteErrors.Kind.TRANSIENT) {
      return new ConnectException("put() - Unrecoverable error writing to ScyllaDB.", cause);
    }
    if (cause instanceof AllNodesFailedException && session != null) {
      log.debug("put() - Setting clusterValid = false", cause);
      session.setInvalid();
    }
    consecutiveFailures++;
    if (config.maxRetries >= 0 && consecutiveFailures > config.maxRetries) {
      return new ConnectException(String.format(
              "put() - Giving up after %d retries of a transient write failure.", config.maxRetries), cause);
    }
    log.warn("put() - Transient write failure, retry {} in {} ms.",
            consecutiveFailures, config.retryBackoffMs, cause);
    context.timeout(config.retryBackoffMs);
    return new RetriableException(cause);
  }

  /**
   * If <code>scylladb.offset.storage.table.enable</code> is set to true, 
   * updates offsets in ScyllaDB table. 
   * Else, assumes all the records in previous @put call were successfully 
   * written in to ScyllaDB and returns the same offsets.
   */
  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets
  ) {
    if (config.isOffsetEnabledInScyllaDb()) {
      try {
        log.debug("flush() - Flushing offsets to {}", this.config.offsetStorageTable);
        List<CompletionStage<AsyncResultSet>> insertFutures = currentOffsets.entrySet().stream()
          .map(e -> this.getValidSession().getInsertOffsetStatement(e.getKey(), e.getValue()))
          .map(s -> getValidSession().executeStatementAsync(s))
          .collect(Collectors.toList());

        for (CompletionStage<AsyncResultSet> future : insertFutures) {
          future.toCompletableFuture().get(this.config.statementTimeoutMs, TimeUnit.MILLISECONDS);
        }
      } catch (AllNodesFailedException ex) {
        log.debug("put() - Setting clusterValid = false", ex);
        getValidSession().setInvalid();
        throw new RetriableException(ex);
      } catch (Exception ex) {
        log.error("put() - Unknown exception. Setting clusterValid = false", ex);
        getValidSession().setInvalid();
        throw new RetriableException(ex);
      }
    }
    return currentOffsets;
  }

  /**
   * Handles a record that cannot be written, however often it is retried. It goes to the
   * errant record reporter (dead letter queue) when the worker provides one, otherwise
   * <code>behavior.on.error</code> decides.
   * Call it only once the rest of the batch is written: a batch that is retried is delivered
   * again with the same record, which would then be reported twice.
   */
  private void handleRecordError(SinkRecord record, Throwable error) {
    Throwable ex = WriteErrors.unwrap(error);
    if (reporter != null) {
      log.debug("Reporting record {}:{}:{} to the errant record reporter.",
              record.topic(), record.kafkaPartition(), record.kafkaOffset(), ex);
      reportedRecords.add(reporter.report(record, ex));
    } else if (config.behaviourOnError == ScyllaDbSinkConnectorConfig.BehaviorOnError.FAIL) {
      throw new ConnectException("Exception occurred while "
              + "processing records from Kafka Sink Records.", ex);
    } else if (config.behaviourOnError == ScyllaDbSinkConnectorConfig.BehaviorOnError.LOG) {
      log.warn("Exception occurred while processing records from Kafka Sink Records, "
              + "ignoring and processing next set of records.", ex);
    } else {
      log.trace("Exception occurred while processing records from Kafka Sink Records, "
              + "ignoring and processing next set of records.", ex);
    }
    // The record is handled; move past it like a written one.
    topicOffsets.put(new TopicPartition(record.topic(), record.kafkaPartition()),
            new OffsetAndMetadata(record.kafkaOffset() + 1));
  }

  /*
   * Waits until the reported records are stored, so a failed report fails this put() rather than
   * the next offset commit. The dead letter queue producer's delivery.timeout.ms bounds the wait.
   */
  private void awaitReportedRecords() {
    try {
      for (Future<Void> future : reportedRecords) {
        future.get();
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while reporting failed records.", ex);
    } catch (ExecutionException ex) {
      throw new ConnectException("Failed to report failed records.", ex.getCause());
    } finally {
      reportedRecords.clear();
    }
  }

  /**
   * Closes the ScyllaDB session and proceeds to closing sink task.
   */
  @Override
  public void stop() {
    close();
  }
  
  // Visible for testing
  void close() {
    if (null != this.session) {
      log.info("Closing getValidSession");
      try {
        this.session.close();
      } catch (IOException ex) {
        log.error("Exception thrown while closing ScyllaDB session.", ex);
      }
      this.session = null;
    }
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }
}