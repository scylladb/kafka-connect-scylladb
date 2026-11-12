package io.connect.scylladb;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScyllaDbSinkTaskTest {

  Map<String, String> settings;
  ScyllaDbSinkTask task;

  @Before
  public void before() {
    settings = new HashMap<>();
    task = new ScyllaDbSinkTask();
    }

  static final String KAFKA_TOPIC = "topic";

  //TODO: failing need to check
  //@Test
  public void shouldReturnNonNullVersion() {
    assertNotNull(task.version());
  }

  @Test
  public void shouldRestoreOffsetsEachTimePartitionsOpen() {
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    SinkTaskContext context = mock(SinkTaskContext.class);
    ScyllaDbSession session = mock(ScyllaDbSession.class);
    task.initialize(context);
    task.session = session;
    when(session.isValid()).thenReturn(true);
    TopicPartition first = new TopicPartition(KAFKA_TOPIC, 0);
    TopicPartition second = new TopicPartition(KAFKA_TOPIC, 1);
    Map<TopicPartition, Long> firstOffsets = Collections.singletonMap(first, 12L);
    Map<TopicPartition, Long> secondOffsets = Collections.singletonMap(second, 34L);
    when(session.loadOffsets(Collections.singleton(first))).thenReturn(firstOffsets);
    when(session.loadOffsets(Collections.singleton(second))).thenReturn(secondOffsets);

    task.start(settings);
    task.open(Collections.emptyList());
    verify(session, never()).loadOffsets(any());
    verify(context, never()).offset(anyMap());

    task.open(Collections.singletonList(first));
    task.open(Collections.singletonList(second));

    verify(context).offset(firstOffsets);
    verify(context).offset(secondOffsets);
    verify(context, never()).assignment();
  }

  // With behavior.on.error=FAIL and no errant record reporter, nothing after a record that cannot be converted is written.
  @Test
  public void shouldStopAtFirstRecordErrorWhenFailing() {
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    settings.put(ScyllaDbSinkConnectorConfig.ENABLE_OFFSET_STORAGE_TABLE, "false");
    task.initialize(mock(SinkTaskContext.class));
    task.start(settings);
    ScyllaDbSession session = mock(ScyllaDbSession.class);
    when(session.isValid()).thenReturn(true);
    task.session = session;
    Map<String, Object> key = Collections.singletonMap("id", "b");
    SinkRecord noKey = new SinkRecord(KAFKA_TOPIC, 1, null, null, null, key, 1L);
    SinkRecord valid = new SinkRecord(KAFKA_TOPIC, 1, null, key, null, key, 2L);

    assertThrows(ConnectException.class, () -> task.put(Arrays.asList(noKey, valid)));

    verify(session, never()).createOrAlterTable(any(), any(), any(), any());
    verify(session, never()).executeStatementAsync(any());
  }

  @Test
  public void shouldStopAndDisconnect() {
    task.stop();
    //TODO: Ensure the task stopped
  }

  //TODO: failing need to check
  //@Test(expected = ConnectException.class)
  public void shouldFailWithInvalidRecord() {
    SinkRecord record = new SinkRecord(
        KAFKA_TOPIC,
        1,
        Schema.STRING_SCHEMA,
        "Sample key",
        Schema.STRING_SCHEMA,
        "Sample value",
        1L
    );

    // Ensure that the exception is translated into a ConnectException
    task.put(Collections.singleton(record));
  }

  //TODO: failing need to check
  //@Test
  public void version() {
    assertNotNull(task.version());
    assertFalse(task.version().equals("0.0.0.0"));
    assertTrue(task.version().matches("^(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)(-\\w+)?$"));
  }
}
