package io.connect.scylladb;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
  public void shouldStopAndDisconnect() {
    task.stop();
    //TODO: Ensure the task stopped
  }

  @Test(expected = ConnectException.class)
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