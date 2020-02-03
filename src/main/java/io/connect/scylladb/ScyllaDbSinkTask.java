package io.connect.scylladb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.TransportException;

/**
 * Task class for ScyllaDB Sink Connector.
 */
public class ScyllaDbSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSinkTask.class);

  private ScyllaDbSinkConnectorConfig config;
  ScyllaDbSession session;

  /**
   * Starts the sink task. 
   * If <code>scylladb.offset.storage.table.enable</code> is set to true, 
   * the task will load offsets for each Kafka topic-parition from 
   * ScyllaDB offset table into task context.
   */
  @Override
  public void start(Map<String, String> settings) {
    this.config = new ScyllaDbSinkConnectorConfig(settings);
    if (config.isOffsetEnabledInScyllaDb()) {
      Set<TopicPartition> assignment = context.assignment();
      this.session = getValidSession();
      Map<TopicPartition, Long> offsets = session.loadOffsets(assignment);
      if (!offsets.isEmpty()) {
        context.offset(offsets);
      }
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
    int count = 0;
    final List<ResultSetFuture> futures = new ArrayList<>(records.size());

    //create a map containing topic, partition number and list of records
    Map<TopicPartition, List<BatchStatement>> batchesPerTopicPartition = new HashMap<>();

    for (SinkRecord record : records) {
      TopicPartitionerHelper topicPartitionerHandler = new TopicPartitionerHelper(config, getValidSession());
      topicPartitionerHandler.validateRecord(record);

      final String topicName = record.topic();
      final int partition = record.kafkaPartition();

      BoundStatement boundStatement = topicPartitionerHandler.getBoundStatementForRecord(record);

      List<BatchStatement> batchStatementList = batchesPerTopicPartition.containsKey(new TopicPartition(topicName, partition)) ?
              batchesPerTopicPartition.get(new TopicPartition(topicName, partition)) : new ArrayList<>();
      BatchStatement batchStatement = batchStatementList.size() > 0 ?
              batchStatementList.get(batchStatementList.size() - 1) : new BatchStatement(BatchStatement.Type.UNLOGGED);

      int totalBatchSize = (batchStatement.size() > 0 ? statementSize(batchStatement) : 0)
              + statementSize(boundStatement);
      if (totalBatchSize <= (config.maxBatchSize * 1024)) {
        batchStatement.add(boundStatement);
        if (batchStatement.size() == 1) {
          batchStatementList.add(batchStatement);
        }
      } else {
        BatchStatement newBatchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        newBatchStatement.add(boundStatement);
        batchStatementList.add(newBatchStatement);
      }

      log.trace("put() - Adding Bound Statement for {}:{}:{}",
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset()
      );

      batchesPerTopicPartition.put(new TopicPartition(topicName, partition), batchStatementList);

    }
    
    for (List<BatchStatement> batchStatementList : batchesPerTopicPartition.values()) {
      for (BatchStatement batchStatement : batchStatementList) {
        log.trace("put() - Executing Batch Statement {} of size {}",
                batchStatement, batchStatement.size());
        ResultSetFuture resultSetFuture = this.getValidSession().executeStatementAsync(batchStatement);
        futures.add(resultSetFuture);
        count++;
      }
    }

    if (count > 0) {
      try {
        log.debug("put() - Checking future(s)");
        for (ResultSetFuture future : futures) {
          ResultSet resultSet =
                  future.getUninterruptibly(this.config.statementTimeoutMs, TimeUnit.MILLISECONDS);
        }
        context.requestCommit();
        // TODO : Log the records that fail in Queue/Kafka Topic.
      } catch (TransportException ex) {
        log.debug("put() - Setting clusterValid = false", ex);
        getValidSession().setInvalid();
        throw new RetriableException(ex);
      } catch (TimeoutException ex) {
        log.error("put() - TimeoutException.", ex);
        throw new RetriableException(ex);
      } catch (Exception ex) {
        log.error("put() - Unknown exception. Setting clusterValid = false", ex);
        getValidSession().setInvalid();
        throw new RetriableException(ex);
      }
    }
  }

  private static int statementSize(Statement statement) {
    return statement.requestSizeInBytes(ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE);
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
      BatchStatement batch = new BatchStatement();
      this.getValidSession().addOffsetsToBatch(batch, currentOffsets);

      try {
        log.debug("flush() - Flushing offsets to {}", this.config.offsetStorageTable);
        getValidSession().executeStatement(batch);
      } catch (TransportException ex) {
        log.debug("put() - Setting clusterValid = false", ex);
        getValidSession().setInvalid();
        throw new RetriableException(ex);
      } catch (Exception ex) {
        log.error("put() - Unknown exception. Setting clusterValid = false", ex);
        getValidSession().setInvalid();
        throw new RetriableException(ex);
      }
    }
    return super.preCommit(currentOffsets);
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