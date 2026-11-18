package io.connect.scylladb;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import io.connect.scylladb.utils.ScyllaDbConstants;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OffsetCommitTest {
  private final Map<String, String> settings = new HashMap<>();
  private ScyllaDbSinkTask task;
  private SinkTaskContext context;
  private ScyllaDbSession session;
  private BoundStatement statement;
  private SinkRecord record;

  @Before
  public void before() {
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    settings.put(ScyllaDbSinkConnectorConfig.ENABLE_OFFSET_STORAGE_TABLE, "false");
    task = new ScyllaDbSinkTask();
    context = mock(SinkTaskContext.class);
    session = mock(ScyllaDbSession.class);
    statement = mock(BoundStatement.class, RETURNS_SELF);
    when(statement.getPreparedStatement()).thenReturn(mock(PreparedStatement.class));
    task.initialize(context);
    task.session = session;
    when(session.isValid()).thenReturn(true);
    RecordToBoundStatementConverter converter = mock(RecordToBoundStatementConverter.class);
    when(session.insert(eq("ks"), eq("topic"), isNull())).thenReturn(converter);
    when(converter.convert(any(), isNull(), eq(ScyllaDbConstants.INSERT_OPERATION)))
        .thenReturn(new RecordToBoundStatementConverter.State(statement));
    when(session.executeStatementAsync(statement))
        .thenReturn(CompletableFuture.completedFuture(mock(AsyncResultSet.class)));
    Map<String, Object> row = Collections.singletonMap("id", "a");
    record = new SinkRecord("topic", 0, null, row, null, row, 7L, 1000L,
        TimestampType.CREATE_TIME);
  }

  @Test
  public void shouldUsePeriodicCommitsByDefault() {
    settings.put(ScyllaDbSinkConnectorConfig.ENABLE_OFFSET_STORAGE_TABLE, "true");
    task.start(settings);
    task.put(Collections.singletonList(record));
    verify(session).executeStatementAsync(statement);
    verify(context, never()).requestCommit();
    Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
        new TopicPartition("topic", 0), new OffsetAndMetadata(8));
    BoundStatement offsetStatement = mock(BoundStatement.class);
    when(session.getInsertOffsetStatement(new TopicPartition("topic", 0), new OffsetAndMetadata(8)))
        .thenReturn(offsetStatement);
    when(session.executeStatementAsync(offsetStatement))
        .thenReturn(CompletableFuture.completedFuture(mock(AsyncResultSet.class)));

    assertEquals(offsets, task.preCommit(offsets));

    verify(session).executeStatementAsync(offsetStatement);
    verify(context, never()).requestCommit();
  }

  @Test
  public void shouldAllowExplicitPeriodicCommits() {
    settings.put(ScyllaDbSinkConnectorConfig.OFFSET_COMMIT_AFTER_EVERY_BATCH, "false");
    task.start(settings);
    task.put(Collections.singletonList(record));
    verify(session).executeStatementAsync(statement);
    verify(context, never()).requestCommit();
  }

  @Test
  public void shouldRequestCommitOncePerSuccessfulBatchWhenEnabled() {
    settings.put(ScyllaDbSinkConnectorConfig.OFFSET_COMMIT_AFTER_EVERY_BATCH, "true");
    task.start(settings);
    task.put(Arrays.asList(record, record));
    verify(session, times(2)).executeStatementAsync(statement);
    verify(context).requestCommit();
  }

  @Test
  public void shouldNotRequestCommitForEmptyBatchWhenEnabled() {
    settings.put(ScyllaDbSinkConnectorConfig.OFFSET_COMMIT_AFTER_EVERY_BATCH, "true");
    task.start(settings);
    task.put(Collections.emptyList());
    verify(context, never()).requestCommit();
  }

  @Test
  public void shouldNotRequestCommitForFailedBatchWhenEnabled() {
    settings.put(ScyllaDbSinkConnectorConfig.OFFSET_COMMIT_AFTER_EVERY_BATCH, "true");
    CompletableFuture<AsyncResultSet> failure = new CompletableFuture<>();
    failure.completeExceptionally(new IllegalStateException("failed write"));
    when(session.executeStatementAsync(statement)).thenReturn(failure);
    task.start(settings);
    assertThrows(ConnectException.class, () -> task.put(Collections.singletonList(record)));
    verify(context, never()).requestCommit();
  }
}
