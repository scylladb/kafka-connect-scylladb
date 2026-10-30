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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Applies recorded change-stream messages (see {@code src/test/resources/cdc/README.md}) whose
 * absolute expiry, in Unix epoch seconds and 0 for none, is in {@code metadata.exp}. The recorded
 * expiry times have passed, so a write that is to expire later gets one relative to now.
 */
@Category(IntegrationTest.class)
public class CdcRecordExpiryIT {

  private static final String KEYSPACE = "cdc_record_expiry";
  private static final String TOPIC = "cdc";
  private static final String TABLE = "expiring";
  private static final int DEFAULT_TTL_SECONDS = 86_400;
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
        // A table default TTL, to tell a record that never expires from one written without a TTL.
        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE
            + " (digest text PRIMARY KEY, name text) WITH default_time_to_live = " + DEFAULT_TTL_SECONDS);
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
  public void futureExpirySetsTheTtl() {
    List<SinkRecord> records = load("12-expiry-default");
    put(settings(true), withExp(records.get(0), nowSeconds() + 3_600));
    Integer ttl = row(digest(records)).get("name_ttl", Integer.class);
    assertNotNull(ttl);
    assertTrue(ttl > 3_590 && ttl <= 3_600, "TTL " + ttl);
  }

  @Test
  public void zeroExpiryNeverExpires() {
    // After a write that expires, so its TTL must not carry over to the next record.
    List<SinkRecord> expiring = load("12-expiry-default");
    List<SinkRecord> records = load("01-create");
    put(settings(true), withExp(expiring.get(0), nowSeconds() + 3_600), records.get(0));
    Row row = row(digest(records));
    assertEquals("alice", row.getString("name"));
    // TTL 0 overrides the table's default TTL.
    assertNull(row.get("name_ttl", Integer.class));
  }

  @Test
  public void expiredRecordIsSkipped() {
    List<SinkRecord> records = load("12-expiry-default");
    put(settings(true), records.get(0));
    assertNull(row(digest(records)));
  }

  @Test
  public void expiredRecordFailsWhenNotSkipping() {
    List<SinkRecord> records = load("12-expiry-default");
    start(settings(false));
    ConnectException e = assertThrows(ConnectException.class, () -> task.put(records.subList(0, 1)));
    assertTrue(e.getCause() instanceof DataException && e.getCause().getMessage().contains("expired"),
        String.valueOf(e.getCause()));
    assertNull(row(digest(records)));
  }

  @Test
  public void flaggedDeleteNeedsNoExpiry() {
    // The delete carries no expiry, and removes a write that has yet to expire.
    List<SinkRecord> records = load("13-expiry-shipped");
    put(settings(true), withExp(records.get(0), nowSeconds() + 3_600), records.get(1));
    assertNull(row(digest(records)));
  }

  private static List<SinkRecord> load(String caseName) {
    return CdcFixtures.load(CdcFixtures.FLAT_JSON, caseName, TOPIC);
  }

  private static long nowSeconds() {
    return System.currentTimeMillis() / 1000;
  }

  private static String digest(List<SinkRecord> records) {
    return (String) ((Map<?, ?>) records.get(0).key()).get("digest");
  }

  private static Map<?, ?> metadata(SinkRecord record) {
    return (Map<?, ?>) ((Map<?, ?>) record.value()).get("metadata");
  }

  /** A copy of a recorded write that expires at another time. */
  private static SinkRecord withExp(SinkRecord write, long exp) {
    Map<String, Object> metadata = new HashMap<>();
    metadata(write).forEach((k, v) -> metadata.put((String) k, v));
    metadata.put("exp", exp);
    Map<String, Object> value = new HashMap<>();
    ((Map<?, ?>) write.value()).forEach((k, v) -> value.put((String) k, v));
    value.put("metadata", metadata);
    return new SinkRecord(write.topic(), 0, null, write.key(), null, value, write.kafkaOffset(),
        write.timestamp(), TimestampType.CREATE_TIME);
  }

  private void start(Map<String, String> settings) {
    if (task != null) {
      task.stop();
    }
    task = new ScyllaDbSinkTask();
    task.initialize(mock(SinkTaskContext.class));
    task.start(settings);
  }

  private void put(Map<String, String> settings, SinkRecord... records) {
    start(settings);
    task.put(Arrays.asList(records));
  }

  private static Map<String, String> settings(boolean skipExpired) {
    final String topicPrefix = "topic." + TOPIC + "." + KEYSPACE + "." + TABLE;
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, KEYSPACE);
    settings.put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, contactPoint);
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(PORT));
    settings.put(ScyllaDbSinkConnectorConfig.LOAD_BALANCING_LOCAL_DC_CONFIG, "datacenter1");
    settings.put(ScyllaDbSinkConnectorConfig.ENABLE_OFFSET_STORAGE_TABLE, "false");
    settings.put(ScyllaDbSinkConnectorConfig.BEHAVIOR_ON_ERROR_CONFIG, "FAIL");
    settings.put(topicPrefix + ".mapping",
        "digest=key.digest, name=value.name, __expiration=value.metadata.exp");
    settings.put(topicPrefix + ".expirationUnit", "SECONDS");
    settings.put(topicPrefix + ".skipExpired", String.valueOf(skipExpired));
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "value.metadata.lut");
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_UNIT_CONFIG, "MILLISECONDS");
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_FIELD_CONFIG, "value.metadata.msg");
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_VALUES_CONFIG, "delete");
    return settings;
  }

  private static Row row(String digest) {
    try (CqlSession session = sessionBuilder().withKeyspace(KEYSPACE).build()) {
      return session.execute("SELECT name, ttl(name) AS name_ttl FROM " + TABLE
          + " WHERE digest = ?", digest).one();
    }
  }
}
