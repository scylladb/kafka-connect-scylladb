package io.connect.scylladb;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.confluent.common.utils.IntegrationTest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.connect.scylladb.integration.SinkRecordUtil.write;
import static io.connect.scylladb.integration.TestDataUtil.struct;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(IntegrationTest.class)
public class ScyllaDbSinkErrorHandlingIT {

  static final String KEYSPACE = "errorhandling";
  static final int PORT = 9042;
  static final String UNREACHABLE_PORT = "9049";
  static final int MAX_CONNECTION_RETRIES = 60;

  static String contactPoint() {
    return System.getProperty("scylla.docker.hostname", "localhost");
  }

  static Map<String, String> settings() {
    Map<String, String> result = new LinkedHashMap<>();
    result.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, KEYSPACE);
    result.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG, "true");
    result.put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, contactPoint());
    result.put(ScyllaDbSinkConnectorConfig.LOAD_BALANCING_LOCAL_DC_CONFIG, "datacenter1");
    result.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(PORT));
    result.put(ScyllaDbSinkConnectorConfig.KEYSPACE_REPLICATION_FACTOR_CONFIG, "1");
    result.put(ScyllaDbSinkConnectorConfig.ENABLE_OFFSET_STORAGE_TABLE, "false");
    return result;
  }

  static CqlSession cqlSession() {
    return CqlSession.builder()
        .addContactPoint(new InetSocketAddress(contactPoint(), PORT))
        .withLocalDatacenter("datacenter1")
        .build();
  }

  @BeforeAll
  public static void createKeyspace() throws InterruptedException {
    for (int attempt = 1; ; attempt++) {
      try (CqlSession session = cqlSession()) {
        session.execute("SELECT cql_version FROM system.local");
        // Start from an empty keyspace, so rows from an earlier run cannot satisfy the assertions.
        session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        break;
      } catch (AllNodesFailedException ex) {
        if (attempt >= MAX_CONNECTION_RETRIES) {
          throw ex;
        }
        Thread.sleep(1000);
      }
    }
    // The connector, not the task, creates the keyspace.
    ScyllaDbSinkConnector connector = new ScyllaDbSinkConnector();
    connector.start(settings());
    connector.stop();
  }

  ScyllaDbSinkTask task;
  SinkTaskContext context;
  ErrantRecordReporter reporter;

  @BeforeEach
  public void start() {
    task = new ScyllaDbSinkTask();
    context = mock(SinkTaskContext.class);
    reporter = mock(ErrantRecordReporter.class);
    when(reporter.report(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    task.initialize(context);
  }

  @AfterEach
  public void stop() {
    task.stop();
  }

  static SinkRecord row(String topic, String id, String name) {
    Struct key = struct("key", "id", Schema.Type.STRING, true, id);
    Struct value = struct("value",
        "id", Schema.Type.STRING, true, id,
        "name", Schema.Type.STRING, true, name);
    return write(topic, key, value);
  }

  // An empty partition key passes conversion and is rejected by ScyllaDB.
  static SinkRecord poison(String topic) {
    return row(topic, "", "poison");
  }

  static String name(String table, String id) {
    try (CqlSession session = cqlSession()) {
      Row row = session.execute(
          String.format("SELECT name FROM %s.%s WHERE id = ?", KEYSPACE, table), id).one();
      return row == null ? null : row.getString("name");
    }
  }

  static CompletableFuture<AsyncResultSet> failed(Throwable error) {
    CompletableFuture<AsyncResultSet> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }

  @Test
  public void poisonRecordGoesToReporter() {
    String topic = "poison_reported";
    when(context.errantRecordReporter()).thenReturn(reporter);
    task.start(settings());
    SinkRecord poison = poison(topic);

    task.put(ImmutableList.of(row(topic, "a", "first"), poison, row(topic, "b", "second")));

    verify(reporter, times(1)).report(eq(poison), isA(InvalidQueryException.class));
    verify(reporter, times(1)).report(any(), any());
    verify(context, never()).timeout(anyLong());
    verify(context).requestCommit();
    assertEquals("first", name(topic, "a"));
    assertEquals("second", name(topic, "b"));
  }

  @Test
  public void poisonRecordFailsTaskWithoutReporter() {
    String topic = "poison_fail";
    task.start(settings());

    ConnectException ex = assertThrows(ConnectException.class,
        () -> task.put(ImmutableList.of(row(topic, "a", "first"), poison(topic))));

    assertEquals(ConnectException.class, ex.getClass());
    assertInstanceOf(InvalidQueryException.class, ex.getCause());
    assertEquals("first", name(topic, "a"));
  }

  @Test
  public void poisonRecordSkippedWithoutReporterWhenLogging() {
    String topic = "poison_log";
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.BEHAVIOR_ON_ERROR_CONFIG, "LOG");
    task.start(settings);

    task.put(ImmutableList.of(row(topic, "a", "first"), poison(topic)));

    verify(context).requestCommit();
    assertEquals("first", name(topic, "a"));
  }

  @Test
  public void poisonRecordSkippedWithoutReporterWhenIgnoring() {
    String topic = "poison_ignore";
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.BEHAVIOR_ON_ERROR_CONFIG, "IGNORE");
    task.start(settings);

    task.put(ImmutableList.of(row(topic, "a", "first"), poison(topic)));

    verify(context).requestCommit();
    assertEquals("first", name(topic, "a"));
  }

  @Test
  public void typeMismatchGoesToReporter() {
    String topic = "type_mismatch";
    try (CqlSession session = cqlSession()) {
      session.execute(String.format(
          "CREATE TABLE IF NOT EXISTS %s.%s (id text PRIMARY KEY, name int)", KEYSPACE, topic));
    }
    when(context.errantRecordReporter()).thenReturn(reporter);
    task.start(settings());
    SinkRecord mismatch = row(topic, "a", "not a number");

    task.put(ImmutableList.of(mismatch));

    ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
    verify(reporter, times(1)).report(eq(mismatch), error.capture());
    assertInstanceOf(DataException.class, error.getValue());
    verify(context, never()).timeout(anyLong());
  }

  @Test
  public void reporterFailureFailsTask() {
    String topic = "poison_not_tolerated";
    when(context.errantRecordReporter()).thenReturn(reporter);
    when(reporter.report(any(), any())).thenThrow(new ConnectException("Tolerance exceeded"));
    task.start(settings());
    SinkRecord poison = poison(topic);

    ConnectException ex = assertThrows(ConnectException.class, () -> task.put(ImmutableList.of(poison)));

    assertEquals(ConnectException.class, ex.getClass());
    verify(reporter).report(eq(poison), isA(InvalidQueryException.class));
  }

  @Test
  public void failedReportFailsTask() {
    String topic = "poison_report_failed";
    when(context.errantRecordReporter()).thenReturn(reporter);
    CompletableFuture<Void> failedReport = new CompletableFuture<>();
    failedReport.completeExceptionally(new ConnectException("Dead letter queue unavailable"));
    when(reporter.report(any(), any())).thenReturn(failedReport);
    task.start(settings());

    ConnectException ex = assertThrows(ConnectException.class,
        () -> task.put(ImmutableList.of(poison(topic))));

    assertEquals(ConnectException.class, ex.getClass());
    verify(context, never()).requestCommit();
  }

  @Test
  public void missingKeyspaceFailsTask() {
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "errorhandling_missing");
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG, "false");
    when(context.errantRecordReporter()).thenReturn(reporter);
    task.start(settings);

    ConnectException ex = assertThrows(ConnectException.class,
        () -> task.put(ImmutableList.of(row("missing_keyspace", "a", "first"))));

    assertEquals(ConnectException.class, ex.getClass());
    verify(reporter, never()).report(any(), any());
    verify(context, never()).timeout(anyLong());
  }

  @Test
  public void missingTableFailsTask() {
    String topic = "missing_table";
    when(context.errantRecordReporter()).thenReturn(reporter);
    task.start(settings());
    // The connector cannot create a table from a record without a schema.
    Map<String, Object> key = Collections.singletonMap("id", "a");
    SinkRecord schemaless = new SinkRecord(topic, 1, null, key, null, key, 0L);

    ConnectException ex = assertThrows(ConnectException.class,
        () -> task.put(ImmutableList.of(schemaless)));

    assertEquals(ConnectException.class, ex.getClass());
    assertInstanceOf(TableConfigurationException.class, ex.getCause());
    verify(reporter, never()).report(any(), any());
    verify(context, never()).timeout(anyLong());
  }

  @Test
  public void primaryKeyMismatchFailsTask() {
    String topic = "primary_key_mismatch";
    try (CqlSession session = cqlSession()) {
      session.execute(String.format(
          "CREATE TABLE %s.%s (id text, name text, PRIMARY KEY (id, name))", KEYSPACE, topic));
    }
    Map<String, String> settings = settings();
    settings.put(String.format("topic.%s.%s.%s.mapping", topic, KEYSPACE, topic), "id=key.id, name=value.name");
    when(context.errantRecordReporter()).thenReturn(reporter);
    task.start(settings);

    ConnectException ex = assertThrows(ConnectException.class,
        () -> task.put(ImmutableList.of(row(topic, "a", "first"))));

    assertEquals(ConnectException.class, ex.getClass());
    assertInstanceOf(TableConfigurationException.class, ex.getCause());
    verify(reporter, never()).report(any(), any());
    verify(context, never()).timeout(anyLong());
  }

  @Test
  public void missingColumnFailsTaskWithoutTableManagement() {
    String topic = "missing_column";
    try (CqlSession session = cqlSession()) {
      session.execute(String.format("CREATE TABLE %s.%s (id text PRIMARY KEY)", KEYSPACE, topic));
    }
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.TABLE_MANAGE_ENABLED_CONFIG, "false");
    when(context.errantRecordReporter()).thenReturn(reporter);
    task.start(settings);

    ConnectException ex = assertThrows(ConnectException.class,
        () -> task.put(ImmutableList.of(row(topic, "a", "first"))));

    assertEquals(ConnectException.class, ex.getClass());
    assertInstanceOf(TableConfigurationException.class, ex.getCause());
    verify(reporter, never()).report(any(), any());
    verify(context, never()).timeout(anyLong());
  }

  @Test
  public void transientFailureBacksOffAndRecovers() {
    String topic = "transient_recovers";
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.MAX_RETRIES_CONFIG, "2");
    when(context.errantRecordReporter()).thenReturn(reporter);
    ScyllaDbSession session = spySession(settings, topic);
    DriverTimeoutException timeout = new DriverTimeoutException("Query timed out");
    doReturn(failed(timeout)).doReturn(failed(timeout)).doCallRealMethod()
        .doReturn(failed(timeout)).doCallRealMethod()
        .when(session).executeStatementAsync(any());
    List<SinkRecord> batch = ImmutableList.of(row(topic, "b", "second"));

    assertThrows(RetriableException.class, () -> task.put(batch));
    assertThrows(RetriableException.class, () -> task.put(batch));
    task.put(batch);
    // A successful batch resets the count, so a later failure is retried again.
    List<SinkRecord> next = ImmutableList.of(row(topic, "c", "third"));
    assertThrows(RetriableException.class, () -> task.put(next));
    task.put(next);

    verify(context, times(3)).timeout(3000L);
    verify(reporter, never()).report(any(), any());
    assertEquals("second", name(topic, "b"));
    assertEquals("third", name(topic, "c"));
  }

  @Test
  public void unreachableClusterRetriesWithoutLimitByDefault() {
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, UNREACHABLE_PORT);
    task.start(settings);
    List<SinkRecord> batch = ImmutableList.of(row("unreachable_default", "a", "first"));

    for (int i = 0; i < 3; i++) {
      assertThrows(RetriableException.class, () -> task.put(batch));
    }
    verify(context, times(3)).timeout(3000L);
  }

  @Test
  public void unreachableClusterFailsTaskAfterMaxRetries() {
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, UNREACHABLE_PORT);
    settings.put(ScyllaDbSinkConnectorConfig.MAX_RETRIES_CONFIG, "1");
    settings.put(ScyllaDbSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, "10");
    task.start(settings);
    List<SinkRecord> batch = ImmutableList.of(row("unreachable_limited", "a", "first"));

    assertThrows(RetriableException.class, () -> task.put(batch));
    ConnectException ex = assertThrows(ConnectException.class, () -> task.put(batch));

    assertEquals(ConnectException.class, ex.getClass());
    assertInstanceOf(AllNodesFailedException.class, ex.getCause());
    verify(context, times(1)).timeout(10L);
  }

  @Test
  public void unreachableClusterFailsTaskAtOnceWithoutRetries() {
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, UNREACHABLE_PORT);
    settings.put(ScyllaDbSinkConnectorConfig.MAX_RETRIES_CONFIG, "0");
    task.start(settings);

    ConnectException ex = assertThrows(ConnectException.class,
        () -> task.put(ImmutableList.of(row("unreachable_no_retries", "a", "first"))));

    assertEquals(ConnectException.class, ex.getClass());
    verify(context, never()).timeout(anyLong());
  }

  // Starts the task, writes one row to open the session, and returns a spy on that session.
  ScyllaDbSession spySession(Map<String, String> settings, String topic) {
    task.start(settings);
    task.put(ImmutableList.of(row(topic, "a", "first")));
    ScyllaDbSession session = spy(task.session);
    task.session = session;
    return session;
  }

  @Test
  public void clusterLostMidRunInvalidatesSession() {
    String topic = "lost_mid_run";
    ScyllaDbSession session = spySession(settings(), topic);
    AllNodesFailedException lost = AllNodesFailedException.fromErrors(Collections.<Node, Throwable>emptyMap());
    doReturn(failed(lost)).when(session).executeStatementAsync(any());
    List<SinkRecord> batch = ImmutableList.of(row(topic, "b", "second"));

    assertThrows(RetriableException.class, () -> task.put(batch));
    assertFalse(session.isValid());
    task.put(batch);

    assertNotSame(session, task.session);
    assertEquals("second", name(topic, "b"));
  }

  @Test
  public void mixedBatchReportsOnlyOnceWritten() {
    String topic = "mixed_batch";
    when(context.errantRecordReporter()).thenReturn(reporter);
    ScyllaDbSession session = spySession(settings(), topic);
    doReturn(failed(new DriverTimeoutException("Query timed out"))).doCallRealMethod()
        .when(session).executeStatementAsync(any());
    SinkRecord poison = poison(topic);
    List<SinkRecord> batch = ImmutableList.of(row(topic, "b", "second"), poison);

    assertThrows(RetriableException.class, () -> task.put(batch));
    verify(reporter, never()).report(any(), any());
    task.put(batch);

    verify(reporter, times(1)).report(eq(poison), isA(InvalidQueryException.class));
    verify(reporter, times(1)).report(any(), any());
    assertEquals("second", name(topic, "b"));
  }

  @Test
  public void unauthorizedWriteFailsTask() {
    String topic = "unauthorized";
    when(context.errantRecordReporter()).thenReturn(reporter);
    ScyllaDbSession session = spySession(settings(), topic);
    doReturn(failed(new UnauthorizedException(mock(Node.class), "denied")))
        .when(session).executeStatementAsync(any());

    ConnectException ex = assertThrows(ConnectException.class,
        () -> task.put(ImmutableList.of(row(topic, "b", "second"))));

    assertEquals(ConnectException.class, ex.getClass());
    assertInstanceOf(UnauthorizedException.class, ex.getCause());
    verify(context, never()).timeout(anyLong());
    verify(reporter, never()).report(any(), any());
  }

  @Test
  public void fatalWriteOutweighsLaterTransientConversionFailure() {
    String topic = "fatal_then_transient";
    String other = "fatal_then_transient_other";
    ScyllaDbSession session = spySession(settings(), topic);
    doReturn(failed(new UnauthorizedException(mock(Node.class), "denied")))
        .when(session).executeStatementAsync(any());
    doThrow(new DriverTimeoutException("Query timed out"))
        .when(session).createOrAlterTable(anyString(), eq(other), any(), any());

    ConnectException ex = assertThrows(ConnectException.class,
        () -> task.put(ImmutableList.of(row(topic, "b", "second"), row(other, "c", "third"))));

    assertEquals(ConnectException.class, ex.getClass());
    assertInstanceOf(UnauthorizedException.class, ex.getCause());
    verify(context, never()).timeout(anyLong());
  }

  @Test
  public void interruptedWriteIsRetriedWithoutCounting() {
    String topic = "interrupted";
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.MAX_RETRIES_CONFIG, "0");
    ScyllaDbSession session = spySession(settings, topic);
    doReturn(new CompletableFuture<AsyncResultSet>()).doCallRealMethod()
        .when(session).executeStatementAsync(any());
    List<SinkRecord> batch = ImmutableList.of(row(topic, "b", "second"));

    try {
      Thread.currentThread().interrupt();
      assertThrows(RetriableException.class, () -> task.put(batch));
    } finally {
      Thread.interrupted();
    }
    // With no retries allowed, a counted failure would have failed the task instead.
    task.put(batch);

    verify(context, never()).timeout(anyLong());
    assertEquals("second", name(topic, "b"));
  }
}
