package io.connect.scylladb.integration;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import io.confluent.common.utils.IntegrationTest;
import io.connect.scylladb.ScyllaDbSinkConnectorConfig;
import io.connect.scylladb.ScyllaDbSinkTask;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Applies recorded change-stream messages (see {@code src/test/resources/cdc/README.md}) whose
 * deletes arrive as records flagged in {@code metadata.msg}, ordered by the change metadata's
 * last-update time.
 */
@Category(IntegrationTest.class)
public class CdcFlaggedDeleteIT {

  private static final String KEYSPACE = "cdc_flagged_delete";
  private static final String TOPIC = "cdc";
  private static final String TABLE = "flagged";
  private static final int PORT = 9042;
  private static final int MAX_CONNECTION_RETRIES = 30;

  private static String contactPoint;

  private ScyllaDbSinkTask task;

  static CqlSessionBuilder sessionBuilder() {
    return CqlSession.builder()
        .addContactPoint(new InetSocketAddress(contactPoint, PORT))
        .withConfigLoader(DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, ProtocolVersion.V4.toString())
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
            .build());
  }

  @BeforeAll
  public static void createTable() throws InterruptedException {
    contactPoint = System.getProperty("scylla.docker.hostname", "localhost");
    for (int attempt = 1; ; attempt++) {
      try (CqlSession session = sessionBuilder().build()) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
            + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");
        // Keyed by the digest, which every message carries, with or without a stored user key.
        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE
            + " (digest text PRIMARY KEY, name text, age bigint, city text)");
        return;
      } catch (AllNodesFailedException e) {
        if (attempt >= MAX_CONNECTION_RETRIES) {
          throw e;
        }
        Thread.sleep(1000);
      }
    }
  }

  @BeforeEach
  public void truncate() {
    try (CqlSession session = sessionBuilder().build()) {
      session.execute("TRUNCATE " + KEYSPACE + "." + TABLE);
    }
  }

  @AfterEach
  public void stop() {
    if (task != null) {
      task.stop();
      task = null;
    }
  }

  @Test
  public void writesAreApplied() {
    // Control: an update without a delete keeps the row, timestamped with the last update time.
    List<SinkRecord> records = load("02-full-update");
    put(records);
    Row row = row(digest(records));
    assertEquals("bob", row.getString("name"));
    assertEquals(31L, row.getLong("age"));
    assertEquals(lut(records.get(1)) * 1000L, row.getLong("name_writetime"));
  }

  @Test
  public void flaggedDeleteRemovesTheRow() {
    for (String caseName : new String[] {"06-durable-delete", "07-delete", "13-expiry-shipped"}) {
      List<SinkRecord> records = load(caseName);
      put(records);
      assertNull(row(digest(records)), caseName);
    }
  }

  @Test
  public void deleteWithoutStoredUserKeyRemovesTheRow() {
    List<SinkRecord> records = load("08-key-not-stored");
    put(records.subList(0, 1));
    assertNotNull(row(digest(records)));
    put(records.subList(1, 2));
    assertNull(row(digest(records)));
  }

  @Test
  public void writeAfterTheDeleteRecreatesTheRow() {
    List<SinkRecord> records = load("06-durable-delete");
    put(records);
    SinkRecord recreate = withLut(records.get(0), lut(records.get(1)) + 1000L, 2);
    put(Collections.singletonList(recreate));
    Row row = row(digest(records));
    assertNotNull(row);
    assertEquals("alice", row.getString("name"));
    assertEquals(lut(recreate) * 1000L, row.getLong("name_writetime"));
  }

  @Test
  public void replayedAndReorderedWritesDoNotResurrectTheRow() {
    List<SinkRecord> records = load("06-durable-delete");
    put(records);
    // The whole sequence again, as after a restart that rewinds the offsets.
    put(records);
    assertNull(row(digest(records)));
    // The write alone, as when a newer delete overtakes it on the topic.
    put(records.subList(0, 1));
    assertNull(row(digest(records)));
  }

  private static List<SinkRecord> load(String caseName) {
    return CdcFixtures.load(CdcFixtures.FLAT_JSON, caseName, TOPIC);
  }

  private static String digest(List<SinkRecord> records) {
    return (String) ((Map<?, ?>) records.get(0).key()).get("digest");
  }

  private static long lut(SinkRecord record) {
    return (Long) metadata(record).get("lut");
  }

  private static Map<?, ?> metadata(SinkRecord record) {
    return (Map<?, ?>) ((Map<?, ?>) record.value()).get("metadata");
  }

  /** A copy of a recorded write with another last-update time, as a later change would carry. */
  private static SinkRecord withLut(SinkRecord write, long lut, long offset) {
    Map<String, Object> metadata = new HashMap<>();
    metadata(write).forEach((k, v) -> metadata.put((String) k, v));
    metadata.put("lut", lut);
    Map<String, Object> value = new HashMap<>();
    ((Map<?, ?>) write.value()).forEach((k, v) -> value.put((String) k, v));
    value.put("metadata", metadata);
    return new SinkRecord(write.topic(), 0, null, write.key(), null, value, offset,
        CdcFixtures.BASE_TIMESTAMP + offset * 1000L, TimestampType.CREATE_TIME);
  }

  private void put(List<SinkRecord> records) {
    if (task == null) {
      task = new ScyllaDbSinkTask();
      task.initialize(mock(SinkTaskContext.class));
      task.start(settings());
    }
    task.put(records);
  }

  private static Map<String, String> settings() {
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, KEYSPACE);
    settings.put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, contactPoint);
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(PORT));
    settings.put(ScyllaDbSinkConnectorConfig.LOAD_BALANCING_LOCAL_DC_CONFIG, "datacenter1");
    settings.put(ScyllaDbSinkConnectorConfig.ENABLE_OFFSET_STORAGE_TABLE, "false");
    settings.put(ScyllaDbSinkConnectorConfig.BEHAVIOR_ON_ERROR_CONFIG, "FAIL");
    settings.put("topic." + TOPIC + "." + KEYSPACE + "." + TABLE + ".mapping",
        String.join(",", Arrays.asList(
            "digest=key.digest", "name=value.name", "age=value.age", "city=value.city")));
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "value.metadata.lut");
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_UNIT_CONFIG, "MILLISECONDS");
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_FIELD_CONFIG, "value.metadata.msg");
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_VALUES_CONFIG, "delete");
    return settings;
  }

  private static Row row(String digest) {
    try (CqlSession session = sessionBuilder().withKeyspace(KEYSPACE).build()) {
      return session.execute("SELECT name, age, writetime(name) AS name_writetime FROM " + TABLE
          + " WHERE digest = ?", digest).one();
    }
  }
}
