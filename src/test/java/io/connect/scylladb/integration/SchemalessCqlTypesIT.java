package io.connect.scylladb.integration;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import io.connect.scylladb.ScyllaDbSinkConnectorConfig;
import io.connect.scylladb.ScyllaDbSinkTask;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/** Writes schemaless values into collection, UDT and text columns and reads them back. */
public class SchemalessCqlTypesIT {

  private static final String KEYSPACE = "schemaless_types";
  private static final String TABLE = "typed";
  private static final String STRUCT_TABLE = "typed_struct";
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
  public static void createTables() throws InterruptedException {
    contactPoint = System.getProperty("scylla.docker.hostname", "localhost");
    for (int attempt = 1; ; attempt++) {
      try (CqlSession session = sessionBuilder().build()) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
            + " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}");
        // Recreated on each run, so a container reused from an older run gets the current columns.
        session.execute("DROP TABLE IF EXISTS " + KEYSPACE + "." + TABLE);
        session.execute("DROP TABLE IF EXISTS " + KEYSPACE + "." + STRUCT_TABLE);
        session.execute("DROP TYPE IF EXISTS " + KEYSPACE + ".system_sid");
        session.execute("CREATE TYPE " + KEYSPACE + ".system_sid (sid list<text>, system text)");
        session.execute("CREATE TABLE " + KEYSPACE + "." + TABLE + " ("
            + "id text PRIMARY KEY, tags list<text>, labels set<text>, counts list<int>, "
            + "coordinates list<double>, attrs map<text, bigint>, small map<text, int>, "
            + "systems_sids list<frozen<system_sid>>, notes list<text>, loc text)");
        session.execute("CREATE TABLE " + KEYSPACE + "." + STRUCT_TABLE + " ("
            + "id text PRIMARY KEY, totals list<bigint>, chunks list<blob>, parts map<text, blob>)");
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
  public void start() {
    try (CqlSession session = sessionBuilder().build()) {
      session.execute("TRUNCATE " + KEYSPACE + "." + TABLE);
      session.execute("TRUNCATE " + KEYSPACE + "." + STRUCT_TABLE);
    }
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, KEYSPACE);
    settings.put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, contactPoint);
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(PORT));
    settings.put(ScyllaDbSinkConnectorConfig.LOAD_BALANCING_LOCAL_DC_CONFIG, "datacenter1");
    settings.put(ScyllaDbSinkConnectorConfig.ENABLE_OFFSET_STORAGE_TABLE, "false");
    settings.put(ScyllaDbSinkConnectorConfig.BEHAVIOR_ON_ERROR_CONFIG, "FAIL");
    task = new ScyllaDbSinkTask();
    task.initialize(mock(SinkTaskContext.class));
    task.start(settings);
  }

  @AfterEach
  public void stop() {
    task.stop();
  }

  @Test
  public void collectionsRoundTrip() {
    Map<String, Object> value = value("collections");
    value.put("tags", Arrays.asList("a", "b"));
    value.put("labels", Arrays.asList("x", "y", "x"));
    value.put("Counts", Arrays.asList(1L, 2L));
    value.put("coordinates", Arrays.asList(13L, 52.5));
    value.put("attrs", Collections.singletonMap("k1", 1L));
    value.put("small", Collections.singletonMap("k2", 2L));
    put(value);

    Row row = row(TABLE, "collections");
    assertEquals(Arrays.asList("a", "b"), row.getList("tags", String.class));
    assertEquals(new LinkedHashSet<>(Arrays.asList("x", "y")), row.getSet("labels", String.class));
    assertEquals(Arrays.asList(1, 2), row.getList("counts", Integer.class));
    assertEquals(Arrays.asList(13.0, 52.5), row.getList("coordinates", Double.class));
    assertEquals(Collections.singletonMap("k1", 1L), row.getMap("attrs", String.class, Long.class));
    assertEquals(Collections.singletonMap("k2", 2), row.getMap("small", String.class, Integer.class));
  }

  @Test
  public void listOfObjectsRoundTripsAsUdts() {
    Map<String, Object> first = new LinkedHashMap<>();
    first.put("sid", Collections.emptyList());
    first.put("system", "s1");
    Map<String, Object> second = new LinkedHashMap<>();
    second.put("sid", Arrays.asList("a", "b"));
    second.put("system", "s2");
    Map<String, Object> value = value("udts");
    value.put("systems_sids", Arrays.asList(first, second));
    put(value);

    List<UdtValue> sids = row(TABLE, "udts").getList("systems_sids", UdtValue.class);
    assertEquals(2, sids.size());
    assertEquals("s1", sids.get(0).getString("system"));
    assertEquals(Arrays.asList("a", "b"), sids.get(1).getList("sid", String.class));
  }

  @Test
  public void objectsInTextListRoundTripAsJson() {
    Map<String, Object> sid = new LinkedHashMap<>();
    sid.put("sid", Arrays.asList("a"));
    sid.put("system", "s1");
    Map<String, Object> value = value("notes");
    value.put("notes", Collections.singletonList(sid));
    put(value);

    assertEquals(Collections.singletonList("{\"sid\":[\"a\"],\"system\":\"s1\"}"),
        row(TABLE, "notes").getList("notes", String.class));
  }

  @Test
  public void objectRoundTripsAsJsonText() {
    Map<String, Object> point = new LinkedHashMap<>();
    point.put("type", "Point");
    point.put("coordinates", Arrays.asList(13.4, 52.5));
    Map<String, Object> value = value("geo");
    value.put("loc", point);
    put(value);

    assertEquals("{\"type\":\"Point\",\"coordinates\":[13.4,52.5]}", row(TABLE, "geo").getString("loc"));
  }

  @Test
  public void structArrayRoundTrips() {
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("totals", SchemaBuilder.array(Schema.INT64_SCHEMA).build())
        .build();
    Struct value = new Struct(schema).put("id", "struct").put("totals", Arrays.asList(5L, 6L));
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
    task.put(Collections.singletonList(
        SinkRecordUtil.write(STRUCT_TABLE, new Struct(keySchema).put("id", "struct"), value)));

    assertEquals(Arrays.asList(5L, 6L), row(STRUCT_TABLE, "struct").getList("totals", Long.class));
  }

  @Test
  public void structBytesRoundTripAsBlobs() {
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.STRING_SCHEMA)
        .field("chunks", SchemaBuilder.array(Schema.BYTES_SCHEMA).build())
        .field("parts", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA).build())
        .build();
    Struct value = new Struct(schema)
        .put("id", "bytes")
        .put("chunks", Collections.singletonList(new byte[] {0, -1, 16}))
        .put("parts", Collections.singletonMap("p1", new byte[] {1, 2}));
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
    task.put(Collections.singletonList(
        SinkRecordUtil.write(STRUCT_TABLE, new Struct(keySchema).put("id", "bytes"), value)));

    Row row = row(STRUCT_TABLE, "bytes");
    assertEquals(Collections.singletonList(ByteBuffer.wrap(new byte[] {0, -1, 16})),
        row.getList("chunks", ByteBuffer.class));
    assertEquals(Collections.singletonMap("p1", ByteBuffer.wrap(new byte[] {1, 2})),
        row.getMap("parts", String.class, ByteBuffer.class));
  }

  private static Map<String, Object> value(String id) {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("id", id);
    return value;
  }

  private void put(Map<String, Object> value) {
    task.put(Collections.singletonList(
        SinkRecordUtil.write(TABLE, null, Collections.singletonMap("id", value.get("id")), null, value)));
  }

  private static Row row(String table, String id) {
    try (CqlSession session = sessionBuilder().withKeyspace(KEYSPACE).build()) {
      return session.execute("SELECT * FROM " + table + " WHERE id = ?", id).one();
    }
  }
}
