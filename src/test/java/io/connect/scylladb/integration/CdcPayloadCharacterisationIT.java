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
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.transforms.Flatten;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Pins what today's sink does with recorded change-stream messages (see
 * {@code src/test/resources/cdc/README.md}). Each test asserts the default behaviour, including the
 * wrong behaviour. Its comment names the change that flips it, or, where the fix is an opt-in
 * setting, that setting and the test that covers it; such a pin keeps describing the default.
 */
@Category(IntegrationTest.class)
public class CdcPayloadCharacterisationIT {

  private static final String KEYSPACE = "cdc_characterisation";
  private static final String TABLE = "cdc";
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
        // One column per field the flattened flat-json cases carry; the raw payloads target it too.
        session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE + " ("
            + "metadata_userkey text PRIMARY KEY, metadata_namespace text, metadata_set text, "
            + "metadata_digest text, metadata_msg text, metadata_durable boolean, "
            + "metadata_gen bigint, metadata_lut bigint, metadata_exp bigint, "
            + "name text, age bigint, city text, tags list<text>, attrs_k1 bigint, attrs_k2 bigint, "
            + "visitors blob, loc_type text, loc_coordinates list<double>, d double, b boolean, "
            + "raw blob)");
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
  public void schemalessRecordIsWritten() {
    // Control: proves the harness writes and reads rows, so the "nothing written" pins mean something.
    Map<String, Object> value = new HashMap<>();
    value.put("metadata_userkey", "control");
    value.put("name", "carol");
    SinkRecord record = new SinkRecord(TABLE, 0, null,
        Collections.singletonMap("metadata_userkey", "control"), null, value, 0,
        CdcFixtures.BASE_TIMESTAMP, TimestampType.CREATE_TIME);

    assertNull(put("FAIL", Collections.singletonList(record)));
    assertEquals("carol", row("control").getString("name"));
  }

  // --- Raw payloads, as captured ---

  @Test
  public void jsonArrayKeyFailsTheTask() {
    // Keys must be a struct or map; the json format needs a record transform (DRIVER-1209, not
    // implemented yet).
    Throwable error = put("FAIL", raw(CdcFixtures.JSON, "01-create"));
    assertNotNull(error);
    assertTrue(causes(error).contains("Key must be a struct or map"), causes(error));
  }

  @Test
  public void jsonPayloadsAreSkippedWhenErrorsAreIgnored() {
    // Nothing is written for any json case; a record transform will map them (DRIVER-1209).
    for (String caseName : CdcFixtures.CASES) {
      assertNull(put("IGNORE", raw(CdcFixtures.JSON, caseName)), caseName);
    }
    assertEquals(0, rowCount());
  }

  @Test
  public void flatJsonNestedMetadataFailsTheTask() {
    // The key fields exist only under `metadata`, which is bound as a column of that name.
    // Field paths into nested values: DRIVER-1209.
    Throwable error = put("FAIL", raw(CdcFixtures.FLAT_JSON, "01-create"));
    assertNotNull(error);
    assertTrue(causes(error).contains("processing field 'metadata'"), causes(error));
  }

  @Test
  public void flatJsonPayloadsAreSkippedWhenErrorsAreIgnored() {
    // Nothing is written for any raw flat-json case either; nested field paths: DRIVER-1209.
    for (String caseName : CdcFixtures.CASES) {
      assertNull(put("IGNORE", raw(CdcFixtures.FLAT_JSON, caseName)), caseName);
    }
    assertEquals(0, rowCount());
  }

  @Test
  public void missingTableFailsWithNullPointer() {
    // Schemaless records create no table, and a missing one dereferences null metadata (#26).
    // DRIVER-1204 turns this into an error naming the table.
    Throwable error = put("FAIL", flattened(CdcFixtures.FLAT_JSON, "01-create", "cdc_missing"));
    assertNotNull(error);
    assertTrue(causes(error).contains("NullPointerException"), causes(error));
  }

  // --- flat-json flattened with the stock Flatten transform ---

  @Test
  public void writeTimestampIsTheKafkaTimestamp() {
    // By default each message overwrites the row, timestamped with Kafka time rather than `lut`
    // (#20). DRIVER-1206 adds the opt-in `scylladb.timestamp.field`; CdcFlaggedDeleteIT covers
    // writes and deletes timestamped from `value.metadata.lut`.
    assertNull(putFlattened("02-full-update"));
    Row row = row("c02");
    assertEquals("bob", row.getString("name"));
    assertEquals(31L, row.getLong("age"));
    assertEquals((CdcFixtures.BASE_TIMESTAMP + 1000L) * 1000L, row.getLong("msg_writetime"));
  }

  @Test
  public void addedFieldIsWritten() {
    assertNull(putFlattened("05-field-added"));
    Row row = row("c05");
    assertEquals("alice", row.getString("name"));
    assertEquals("paris", row.getString("city"));
  }

  @Test
  public void removedFieldKeepsItsOldValue() {
    // A field missing from the message is left unset, so the old value survives whether the source
    // ships whole records or only changed fields. Removed fields: DRIVER-1209, not implemented yet.
    assertNull(putFlattened("03-field-removed"));
    assertNull(putFlattened("04-field-removed-changed-only"));
    // The lut of each case's final message, which removes the field, proves that message was written.
    Map<String, Long> finalLut = new LinkedHashMap<>();
    finalLut.put("c03", 1793046995355L);
    finalLut.put("c04", 1793047009458L);
    for (Map.Entry<String, Long> expected : finalLut.entrySet()) {
      Row row = row(expected.getKey());
      assertEquals(expected.getValue().longValue(), row.getLong("metadata_lut"), expected.getKey());
      assertEquals("bob", row.getString("name"), expected.getKey());
      assertEquals(30L, row.getLong("age"), expected.getKey());
    }
  }

  @Test
  public void deleteFlaggedInTheValueIsWrittenAsARow() {
    // By default deletes arrive as values, not tombstones, so the row stays and records the flag.
    // DRIVER-1207 adds the opt-in `scylladb.deletes.field` and `scylladb.deletes.values`, covered
    // by CdcFlaggedDeleteIT.
    for (String caseName : new String[] {"06-durable-delete", "07-delete", "13-expiry-shipped"}) {
      assertNull(putFlattened(caseName), caseName);
      Row row = row("c" + caseName.substring(0, 2));
      assertNotNull(row, caseName);
      assertEquals("delete", row.getString("metadata_msg"), caseName);
      assertEquals("alice", row.getString("name"), caseName);
    }
  }

  @Test
  public void missingUserKeyFailsTheBatchAsRetriable() {
    // Without a stored user key the primary key is unset; the server rejects the write and the task
    // reports a retriable error, so the batch is redelivered indefinitely. DRIVER-1205 flips this:
    // a rejected write becomes a record error. Key choice: DRIVER-1209, not implemented yet.
    Throwable error = putFlattened("08-key-not-stored");
    assertTrue(error instanceof RetriableException, causes(error));
    assertTrue(causes(error).contains("InvalidQueryException: Unexpected unset value"), causes(error));
    assertEquals(0, rowCount());
  }

  @Test
  public void expiryIsNotApplied() {
    // By default `exp` is carried as a plain column and the row gets no TTL (#75). DRIVER-1208 adds
    // the opt-in `__expiration` mapping, covered by CdcRecordExpiryIT.
    assertNull(putFlattened("12-expiry-default"));
    Row row = row("c12");
    assertTrue(row.getLong("metadata_exp") > 0);
    assertNull(row.getObject("msg_ttl"));
  }

  @Test
  public void truncateLeavesTheRow() {
    // Truncation produces no message, so the row written before it survives.
    List<SinkRecord> records = flattened(CdcFixtures.FLAT_JSON, "14-truncate", TABLE);
    assertEquals(1, records.size());
    assertNull(put("FAIL", records));
    assertNotNull(row("c14"));
  }

  @Test
  public void schemalessListFailsToBind() {
    // Schemaless lists are bound with element type Object, for which no codec exists (#30, #101).
    // DRIVER-1210 maps collection, blob and geo values.
    assertCodecNotFound(putFlattened("09-collection-element-ops"), "tags");
    assertCodecNotFound(putFlattened("11-geojson"), "loc_coordinates");
  }

  @Test
  public void base64StringFailsToBindToBlob() {
    // Bytes arrive base64-encoded and no codec maps a string to a blob column (#30, #101).
    // DRIVER-1210 decodes them.
    assertCodecNotFound(putFlattened("10-hyperloglog"), "visitors");
    assertCodecNotFound(putFlattened("15-scalar-types"), "raw");
  }

  private static void assertCodecNotFound(Throwable error, String field) {
    assertNotNull(error, field);
    String causes = causes(error);
    assertTrue(causes.contains("processing field '" + field + "'"), causes);
    assertTrue(causes.contains("Codec not found"), causes);
    assertEquals(0, rowCount());
  }

  private Throwable putFlattened(String caseName) {
    return put("FAIL", flattened(CdcFixtures.FLAT_JSON, caseName, TABLE));
  }

  private List<SinkRecord> raw(String format, String caseName) {
    return CdcFixtures.load(format, caseName, TABLE);
  }

  private List<SinkRecord> flattened(String format, String caseName, String topic) {
    List<SinkRecord> records = new ArrayList<>();
    try (Flatten.Value<SinkRecord> flatten = new Flatten.Value<>()) {
      flatten.configure(Collections.singletonMap("delimiter", "_"));
      for (SinkRecord record : CdcFixtures.load(format, caseName, topic)) {
        records.add(flatten.apply(record));
      }
    }
    return records;
  }

  /** Starts a task, puts the records and returns what {@code put} threw, or null. */
  private Throwable put(String behaviorOnError, List<SinkRecord> records) {
    if (task == null) {
      task = new ScyllaDbSinkTask();
      task.initialize(mock(SinkTaskContext.class));
      task.start(settings(behaviorOnError));
    }
    try {
      task.put(records);
      return null;
    } catch (Throwable e) {
      return e;
    }
  }

  private static Map<String, String> settings(String behaviorOnError) {
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, KEYSPACE);
    settings.put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, contactPoint);
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(PORT));
    settings.put(ScyllaDbSinkConnectorConfig.LOAD_BALANCING_LOCAL_DC_CONFIG, "datacenter1");
    settings.put(ScyllaDbSinkConnectorConfig.ENABLE_OFFSET_STORAGE_TABLE, "false");
    settings.put(ScyllaDbSinkConnectorConfig.BEHAVIOR_ON_ERROR_CONFIG, behaviorOnError);
    return settings;
  }

  private static String causes(Throwable error) {
    StringBuilder chain = new StringBuilder();
    for (Throwable t = error; t != null; t = t.getCause()) {
      chain.append(t.getClass().getName()).append(": ").append(t.getMessage()).append('\n');
    }
    return chain.toString();
  }

  private static Row row(String userKey) {
    try (CqlSession session = sessionBuilder().withKeyspace(KEYSPACE).build()) {
      return session.execute("SELECT name, age, city, tags, attrs_k1, attrs_k2, visitors, loc_type, "
          + "loc_coordinates, d, b, raw, metadata_msg, metadata_lut, metadata_exp, "
          + "writetime(metadata_msg) AS msg_writetime, ttl(metadata_msg) AS msg_ttl "
          + "FROM " + TABLE + " WHERE metadata_userkey = ?", userKey).one();
    }
  }

  private static long rowCount() {
    try (CqlSession session = sessionBuilder().withKeyspace(KEYSPACE).build()) {
      return session.execute("SELECT COUNT(*) FROM " + TABLE).one().getLong(0);
    }
  }
}
