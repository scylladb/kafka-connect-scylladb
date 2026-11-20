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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Writes to and deletes from tables whose columns have names CQL must quote: mixed-case primary
 * key columns {@code "userKey"} and {@code "itemId"}, ones that need quotes even in lower case,
 * {@code "_userId"} and the reserved word {@code "Order"}, and a regular column {@code "__ttl"}.
 * Tables the connector creates from the record, and tables created beforehand.
 */
@Category(IntegrationTest.class)
public class QuotedColumnNamesIT {

  private static final String KEYSPACE = "quoted_column_names";
  private static final int TOPIC_TTL_SECONDS = 3_600;
  private static final int PORT = 9042;
  private static final int MAX_CONNECTION_RETRIES = 30;
  private static final long BASE_TIMESTAMP = 1_700_000_000_000L;

  private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
      .field("userKey", Schema.STRING_SCHEMA)
      .field("itemId", Schema.INT64_SCHEMA)
      .build();
  // Without a mapping, the value carries the key fields as well.
  private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
      .field("userKey", Schema.STRING_SCHEMA)
      .field("itemId", Schema.INT64_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("op", Schema.OPTIONAL_STRING_SCHEMA)
      .field("exp", Schema.OPTIONAL_INT64_SCHEMA)
      .build();
  private static final Schema RESERVED_KEY_SCHEMA = SchemaBuilder.struct()
      .field("_userId", Schema.STRING_SCHEMA)
      .field("Order", Schema.INT64_SCHEMA)
      .build();
  private static final Schema RESERVED_VALUE_SCHEMA = SchemaBuilder.struct()
      .field("_userId", Schema.STRING_SCHEMA)
      .field("Order", Schema.INT64_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .build();

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
  public static void createKeyspace() throws InterruptedException {
    contactPoint = System.getProperty("scylla.docker.hostname", "localhost");
    for (int attempt = 1; ; attempt++) {
      try (CqlSession session = sessionBuilder().build()) {
        session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        session.execute("CREATE KEYSPACE " + KEYSPACE
            + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");
        return;
      } catch (AllNodesFailedException e) {
        if (attempt >= MAX_CONNECTION_RETRIES) {
          throw e;
        }
        Thread.sleep(1000);
      }
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
  public void nullValueDeletesFromCreatedTable() {
    String table = "created_null_value";
    start(managedSettings());
    put(structRecord(table, "alice", null, null, 0));
    assertNotNull(row(table));
    put(new SinkRecord(table, 0, KEY_SCHEMA, structKey(), null, null, 1,
        BASE_TIMESTAMP + 1000L, TimestampType.CREATE_TIME));
    assertNull(row(table));
  }

  @Test
  public void nullValueDeletesFromExistingTable() {
    String table = "existing_null_value";
    createTable(table);
    start(settings());
    put(mapRecord(table, "alice", null, null, 0));
    assertNotNull(row(table));
    put(new SinkRecord(table, 0, null, mapKey(), null, null, 1,
        BASE_TIMESTAMP + 1000L, TimestampType.CREATE_TIME));
    assertNull(row(table));
  }

  @Test
  public void flaggedDeleteDeletesFromCreatedTable() {
    String table = "created_flagged";
    start(flaggedSettings(managedSettings(), table));
    put(structRecord(table, "alice", null, null, 0));
    assertEquals("alice", row(table).getString("name"));
    put(structRecord(table, "alice", "delete", null, 1));
    assertNull(row(table));
  }

  @Test
  public void flaggedDeleteDeletesFromExistingTable() {
    String table = "existing_flagged";
    createTable(table);
    start(flaggedSettings(settings(), table));
    put(mapRecord(table, "alice", null, null, 0));
    assertEquals("alice", row(table).getString("name"));
    put(mapRecord(table, "alice", "delete", null, 1));
    assertNull(row(table));
  }

  @Test
  public void expiredUpdateDeletesFromCreatedTable() {
    String table = "created_expired";
    start(expirySettings(managedSettings(), table));
    put(structRecord(table, "alice", null, nowSeconds() + 3_600, 0));
    assertEquals("alice", row(table).getString("name"));
    put(structRecord(table, "bob", null, nowSeconds() - 60, 1));
    assertNull(row(table));
  }

  @Test
  public void expiredUpdateDeletesFromExistingTable() {
    String table = "existing_expired";
    createTable(table);
    start(expirySettings(settings(), table));
    put(mapRecord(table, "alice", null, nowSeconds() + 3_600, 0));
    assertEquals("alice", row(table).getString("name"));
    put(mapRecord(table, "bob", null, nowSeconds() - 60, 1));
    assertNull(row(table));
  }

  @Test
  public void keyColumnsQuotedInLowerCaseInCreatedTable() {
    String table = "created_reserved";
    start(managedSettings());
    Struct key = new Struct(RESERVED_KEY_SCHEMA).put("_userId", "user-1").put("Order", 7L);
    Struct value = new Struct(RESERVED_VALUE_SCHEMA).put("_userId", "user-1").put("Order", 7L).put("name", "alice");
    put(new SinkRecord(table, 0, RESERVED_KEY_SCHEMA, key, RESERVED_VALUE_SCHEMA, value, 0,
        BASE_TIMESTAMP, TimestampType.CREATE_TIME));
    assertEquals("alice", reservedRow(table).getString("name"));
    put(new SinkRecord(table, 0, RESERVED_KEY_SCHEMA, key, null, null, 1,
        BASE_TIMESTAMP + 1000L, TimestampType.CREATE_TIME));
    assertNull(reservedRow(table));
  }

  @Test
  public void keyColumnsQuotedInLowerCaseInExistingTable() {
    String table = "existing_reserved";
    try (CqlSession session = sessionBuilder().build()) {
      session.execute("CREATE TABLE " + KEYSPACE + "." + table
          + " (\"_userId\" text, \"Order\" bigint, name text, PRIMARY KEY (\"_userId\", \"Order\"))");
    }
    start(settings());
    Map<String, Object> key = new HashMap<>();
    key.put("_userId", "user-1");
    key.put("Order", 7L);
    Map<String, Object> value = new HashMap<>(key);
    value.put("name", "alice");
    put(new SinkRecord(table, 0, null, key, null, value, 0, BASE_TIMESTAMP, TimestampType.CREATE_TIME));
    assertEquals("alice", reservedRow(table).getString("name"));
    put(new SinkRecord(table, 0, null, key, null, null, 1, BASE_TIMESTAMP + 1000L, TimestampType.CREATE_TIME));
    assertNull(reservedRow(table));
  }

  @Test
  public void columnNamedTtlKeepsItsValueUnderTopicTtl() {
    // The topic's TTL is bound to its own marker, which must not be the column's.
    String table = "existing_ttl_column";
    try (CqlSession session = sessionBuilder().build()) {
      session.execute("CREATE TABLE " + KEYSPACE + "." + table + " (\"userKey\" text, \"itemId\" bigint, "
          + "name text, \"__ttl\" text, PRIMARY KEY (\"userKey\", \"itemId\"))");
    }
    Map<String, String> settings = settings();
    settings.put(topicPrefix(table) + ".ttlSeconds", String.valueOf(TOPIC_TTL_SECONDS));
    start(settings);
    Map<String, Object> value = mapKey();
    value.put("name", "alice");
    value.put("__ttl", "own value");
    put(new SinkRecord(table, 0, null, mapKey(), null, value, 0, BASE_TIMESTAMP, TimestampType.CREATE_TIME));
    try (CqlSession session = sessionBuilder().withKeyspace(KEYSPACE).build()) {
      Row row = session.execute("SELECT \"__ttl\", TTL(name) AS name_ttl, TTL(\"__ttl\") AS column_ttl FROM "
          + table + " WHERE \"userKey\" = ? AND \"itemId\" = ?", "user-1", 7L).one();
      assertNotNull(row);
      assertEquals("own value", row.getString("__ttl"));
      for (String ttl : new String[] {"name_ttl", "column_ttl"}) {
        Integer seconds = row.get(ttl, Integer.class);
        assertNotNull(seconds, ttl);
        assertTrue(seconds > 0 && seconds <= TOPIC_TTL_SECONDS, ttl + " " + seconds);
      }
    }
  }

  @Test
  public void unboundKeyColumnIsNamedAsInTheTable() {
    String table = "existing_unbound";
    createTable(table);
    start(settings());
    Map<String, Object> key = Collections.singletonMap("userKey", "user-1");
    ConnectException e = assertThrows(ConnectException.class, () -> put(new SinkRecord(table, 0, null, key,
        null, null, 0, BASE_TIMESTAMP, TimestampType.CREATE_TIME)));
    assertTrue(e.getCause().getMessage().contains("[\"itemId\"]"), e.getCause().getMessage());
  }

  private static Row reservedRow(String table) {
    try (CqlSession session = sessionBuilder().withKeyspace(KEYSPACE).build()) {
      return session.execute("SELECT name FROM " + table + " WHERE \"_userId\" = ? AND \"Order\" = ?",
          "user-1", 7L).one();
    }
  }

  private static long nowSeconds() {
    return System.currentTimeMillis() / 1000;
  }

  private static void createTable(String table) {
    try (CqlSession session = sessionBuilder().build()) {
      session.execute("CREATE TABLE " + KEYSPACE + "." + table
          + " (\"userKey\" text, \"itemId\" bigint, name text, PRIMARY KEY (\"userKey\", \"itemId\"))");
    }
  }

  private static Struct structKey() {
    return new Struct(KEY_SCHEMA).put("userKey", "user-1").put("itemId", 7L);
  }

  private static Map<String, Object> mapKey() {
    Map<String, Object> key = new HashMap<>();
    key.put("userKey", "user-1");
    key.put("itemId", 7L);
    return key;
  }

  /** A record with a schema, at a Kafka timestamp a second later for each offset. */
  private static SinkRecord structRecord(String topic, String name, String op, Long exp, long offset) {
    Struct value = new Struct(VALUE_SCHEMA).put("userKey", "user-1").put("itemId", 7L)
        .put("name", name).put("op", op).put("exp", exp);
    return new SinkRecord(topic, 0, KEY_SCHEMA, structKey(), VALUE_SCHEMA, value, offset,
        BASE_TIMESTAMP + offset * 1000L, TimestampType.CREATE_TIME);
  }

  /** A record without a schema, at a Kafka timestamp a second later for each offset. */
  private static SinkRecord mapRecord(String topic, String name, String op, Long exp, long offset) {
    Map<String, Object> value = mapKey();
    value.put("name", name);
    if (op != null) {
      value.put("op", op);
    }
    if (exp != null) {
      value.put("exp", exp);
    }
    return new SinkRecord(topic, 0, null, mapKey(), null, value, offset,
        BASE_TIMESTAMP + offset * 1000L, TimestampType.CREATE_TIME);
  }

  private void start(Map<String, String> settings) {
    task = new ScyllaDbSinkTask();
    task.initialize(mock(SinkTaskContext.class));
    task.start(settings);
  }

  private void put(SinkRecord record) {
    task.put(Collections.singletonList(record));
  }

  private static Map<String, String> settings() {
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, KEYSPACE);
    settings.put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, contactPoint);
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(PORT));
    settings.put(ScyllaDbSinkConnectorConfig.LOAD_BALANCING_LOCAL_DC_CONFIG, "datacenter1");
    settings.put(ScyllaDbSinkConnectorConfig.ENABLE_OFFSET_STORAGE_TABLE, "false");
    settings.put(ScyllaDbSinkConnectorConfig.BEHAVIOR_ON_ERROR_CONFIG, "FAIL");
    settings.put(ScyllaDbSinkConnectorConfig.TABLE_MANAGE_ENABLED_CONFIG, "false");
    return settings;
  }

  /** The connector creates the table, named after the topic, from the record key and value. */
  private static Map<String, String> managedSettings() {
    Map<String, String> settings = settings();
    settings.put(ScyllaDbSinkConnectorConfig.TABLE_MANAGE_ENABLED_CONFIG, "true");
    return settings;
  }

  private static Map<String, String> flaggedSettings(Map<String, String> settings, String table) {
    settings.put(mapping(table), "userKey=key.userKey, itemId=key.itemId, name=value.name");
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_FIELD_CONFIG, "value.op");
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_VALUES_CONFIG, "delete");
    return settings;
  }

  private static Map<String, String> expirySettings(Map<String, String> settings, String table) {
    settings.put(mapping(table),
        "userKey=key.userKey, itemId=key.itemId, name=value.name, __expiration=value.exp");
    settings.put(topicPrefix(table) + ".expirationUnit", "SECONDS");
    settings.put(topicPrefix(table) + ".behaviorOnExpired", "DELETE");
    return settings;
  }

  private static String mapping(String table) {
    return topicPrefix(table) + ".mapping";
  }

  /** Each test writes its own topic to a table of the same name. */
  private static String topicPrefix(String table) {
    return "topic." + table + "." + KEYSPACE + "." + table;
  }

  private static Row row(String table) {
    try (CqlSession session = sessionBuilder().withKeyspace(KEYSPACE).build()) {
      return session.execute("SELECT name FROM " + table + " WHERE \"userKey\" = ? AND \"itemId\" = ?",
          "user-1", 7L).one();
    }
  }
}
