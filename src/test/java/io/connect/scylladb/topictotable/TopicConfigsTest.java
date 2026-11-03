package io.connect.scylladb.topictotable;

import io.connect.scylladb.ScyllaDbSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TopicConfigsTest {

  private ScyllaDbSinkConnectorConfig connectorConfig;

  @Before
  public void before() {
    Map<String, String> settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
    connectorConfig = new ScyllaDbSinkConnectorConfig(settings);
  }

  @Test
  public void shouldCalculateTtlFromTimestampAndOffset() {
    TopicConfigs configs = topicConfigs("__expiration=value.validUntil", "86400");
    SinkRecord record = timestampRecord(new Date(1_010_000L));

    configs.setTablePartitionAndColumnValues(record);
    configs.setTtlIfAvailable(record, 1_000_000L);

    assertEquals(Integer.valueOf(86_410), configs.getTtl());
  }

  @Test
  public void shouldCalculateTtlFromEpochMillisecondsAndRoundUp() {
    TopicConfigs configs = topicConfigs("__expiration=value.validUntil", "0");
    SinkRecord record = longRecord(1_001_001L);

    configs.setTablePartitionAndColumnValues(record);
    configs.setTtlIfAvailable(record, 1_000_000L);

    assertEquals(Integer.valueOf(2), configs.getTtl());
  }

  @Test
  public void shouldCalculateTtlForSchemalessRecord() {
    TopicConfigs configs = topicConfigs("__expiration=value.validUntil", "10");
    Map<String, Object> key = new HashMap<>();
    key.put("id", 1L);
    Map<String, Object> value = new HashMap<>();
    value.put("validUntil", 1_001_000L);
    SinkRecord record = new SinkRecord("topic", 0, null, key, null, value, 0L, 1_000L, null);

    configs.setTablePartitionAndColumnValues(record);
    configs.setTtlIfAvailable(record, 1_000_000L);

    assertEquals(Integer.valueOf(11), configs.getTtl());
  }

  @Test
  public void shouldKeepExistingRelativeTtlBehavior() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__ttl=value.ttl");
    TopicConfigs configs = new TopicConfigs("ks", "table", settings, connectorConfig);
    Schema valueSchema = SchemaBuilder.struct()
            .field("ttl", Schema.INT32_SCHEMA)
            .build();
    SinkRecord record = record(valueSchema, new Struct(valueSchema).put("ttl", 42));

    configs.setTablePartitionAndColumnValues(record);
    configs.setTtlIfAvailable(record, 1_000_000L);

    assertEquals(Integer.valueOf(42), configs.getTtl());
  }

  @Test
  public void shouldFailRecordWithoutTheTtlField() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__ttl=value.metadata.ttl");
    TopicConfigs configs = new TopicConfigs("ks", "table", settings, connectorConfig);
    Map<String, Object> value = new HashMap<>();
    value.put("metadata", Collections.singletonMap("ttl", 42));
    SinkRecord record = new SinkRecord("topic", 0, null, Collections.singletonMap("id", 1L), null, value, 0L);

    configs.setTablePartitionAndColumnValues(record);
    DataException e = assertThrows(DataException.class, () -> configs.setTtlIfAvailable(record, 1_000_000L));
    assertTrue(e.getMessage(), e.getMessage().contains("'metadata.ttl'"));
  }

  @Test
  public void shouldDeleteExpiredRecordByDefault() {
    TopicConfigs configs = topicConfigs("__expiration=value.validUntil", "0");
    SinkRecord record = longRecord(999_999L);

    configs.setTablePartitionAndColumnValues(record);
    assertFalse(configs.setTtlIfAvailable(record, 1_000_000L));
    assertTrue(configs.deletesExpired());
  }

  @Test
  public void shouldSkipExpiredRecordWhenSetToSkip() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__expiration=value.validUntil");
    settings.put("expirationUnit", "MILLISECONDS");
    settings.put("behaviorOnExpired", "skip");
    TopicConfigs configs = new TopicConfigs("ks", "table", settings, connectorConfig);
    SinkRecord record = longRecord(999_999L);

    configs.setTablePartitionAndColumnValues(record);
    assertFalse(configs.setTtlIfAvailable(record, 1_000_000L));
    assertFalse(configs.deletesExpired());
  }

  @Test
  public void shouldNotDeleteExpiredWithoutExpirationField() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "id=key.id, name=value.name");
    assertFalse(new TopicConfigs("ks", "table", settings, connectorConfig).deletesExpired());
  }

  @Test
  public void shouldFailExpiredRecordWhenSetToFail() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__expiration=value.validUntil");
    settings.put("expirationUnit", "MILLISECONDS");
    settings.put("behaviorOnExpired", "FAIL");
    TopicConfigs configs = new TopicConfigs("ks", "table", settings, connectorConfig);
    SinkRecord record = longRecord(999_999L);

    configs.setTablePartitionAndColumnValues(record);
    DataException e = assertThrows(DataException.class, () -> configs.setTtlIfAvailable(record, 1_000_000L));
    assertTrue(e.getMessage(), e.getMessage().contains("expired"));
  }

  @Test
  public void shouldReadExpirationInSeconds() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__expiration=value.validUntil");
    settings.put("expirationUnit", "SECONDS");
    TopicConfigs configs = new TopicConfigs("ks", "table", settings, connectorConfig);
    SinkRecord record = longRecord(1_100L);

    configs.setTablePartitionAndColumnValues(record);
    assertTrue(configs.setTtlIfAvailable(record, 1_000_000L));

    assertEquals(Integer.valueOf(100), configs.getTtl());
  }

  @Test
  public void shouldReadNestedExpirationNotInMappedColumns() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "id=key.id, name=value.name, __expiration=value.metadata.expiresAt");
    settings.put("expirationUnit", "SECONDS");
    TopicConfigs configs = new TopicConfigs("ks", "table", settings, connectorConfig);
    Schema metadataSchema = SchemaBuilder.struct().field("expiresAt", Schema.INT64_SCHEMA).build();
    Schema valueSchema = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("metadata", metadataSchema)
            .build();
    Struct value = new Struct(valueSchema)
            .put("name", "alice")
            .put("metadata", new Struct(metadataSchema).put("expiresAt", 1_100L));
    SinkRecord record = record(valueSchema, value);

    configs.setTablePartitionAndColumnValues(record);
    assertTrue(configs.setTtlIfAvailable(record, 1_000_000L));

    assertEquals(Integer.valueOf(100), configs.getTtl());
    assertEquals(1, configs.getTableColumnMap().size());
  }

  @Test
  public void shouldRejectInvalidExpirationUnit() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__expiration=value.validUntil");
    settings.put("expirationUnit", "HOURS");
    ConfigException e = assertThrows(ConfigException.class,
            () -> new TopicConfigs("ks", "table", settings, connectorConfig, namesOf(settings)));
    assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.expirationUnit"));
  }

  @Test
  public void shouldRequireExpirationUnitWithExpirationField() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__expiration=value.validUntil");
    ConfigException e = assertThrows(ConfigException.class,
            () -> new TopicConfigs("ks", "table", settings, connectorConfig, namesOf(settings)));
    assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.expirationUnit"));
  }

  @Test
  public void shouldNotRequireExpirationUnitWithoutExpirationField() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "id=key.id, name=value.name");
    new TopicConfigs("ks", "table", settings, connectorConfig);
  }

  @Test
  public void shouldRejectTtlAndExpirationInSameMapping() {
    ConfigException e = assertThrows(ConfigException.class,
            () -> expirationTopicConfigs("__ttl=value.ttl, __expiration=value.validUntil"));
    assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.mapping"));
    assertTrue(e.getMessage(), e.getMessage().contains("both __ttl and __expiration"));
  }

  @Test
  public void shouldRejectExpirationFromKey() {
    ConfigException e = assertThrows(ConfigException.class,
            () -> expirationTopicConfigs("id=key.id, __expiration=key.validUntil"));
    assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.mapping"));
    assertTrue(e.getMessage(), e.getMessage().contains("must map a value field"));
  }

  @Test
  public void shouldRejectMalformedMappingEntryAtStartup() {
    for (String mapping : new String[] {"id=key.id, name", "id=key.id,, name=value.name", "id=key.id, name=value",
        "id=key.id, name=Value.name", "id=key.id, =value.name", " =key.id, name=value.name", ",", ",,,"}) {
      Map<String, String> settings = Collections.singletonMap("mapping", mapping);
      ConfigException e = assertThrows(ConfigException.class,
              () -> new TopicConfigs("ks", "table", settings, connectorConfig, namesOf(settings)));
      assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.mapping"));
      assertTrue(e.getMessage(), e.getMessage().contains("Each entry must have the form"));
    }
  }

  @Test
  public void shouldAcceptTrailingCommaAfterMappingEntry() {
    Map<String, String> settings = Collections.singletonMap("mapping", "id=key.id, name=value.name,");
    new TopicConfigs("ks", "table", settings, connectorConfig, namesOf(settings));
  }

  @Test
  public void shouldRejectTtlOrTimestampOutsideTheValueAtStartup() {
    for (String mapping : new String[] {"id=key.id, __ttl=key.ttl", "id=key.id, __ttl=header.ttl",
        "id=key.id, __timestamp=key.ts", "id=key.id, __timestamp=header.ts"}) {
      Map<String, String> settings = Collections.singletonMap("mapping", mapping);
      ConfigException e = assertThrows(ConfigException.class,
              () -> new TopicConfigs("ks", "table", settings, connectorConfig, namesOf(settings)));
      assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.mapping"));
      assertTrue(e.getMessage(), e.getMessage().contains("must map a value field"));
    }
  }

  @Test
  public void shouldRejectSecondExpirationEntry() {
    ConfigException e = assertThrows(ConfigException.class,
            () -> expirationTopicConfigs("id=key.id, __expiration=value.a, __expiration=value.b"));
    assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.mapping"));
    assertTrue(e.getMessage(), e.getMessage().contains("__expiration only once"));
  }

  @Test
  public void shouldRejectMalformedExpirationPathUnderTheMappingName() {
    for (String path : new String[] {"value.", "value.metadata..expiresAt"}) {
      ConfigException e = assertThrows(ConfigException.class,
              () -> expirationTopicConfigs("id=key.id, __expiration=" + path));
      assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.mapping"));
      assertTrue(e.getMessage(), e.getMessage().contains("must map a value field"));
      assertFalse(e.getMessage(), e.getMessage().contains("header."));
    }
  }

  @Test
  public void shouldMapValueColumnByItsFullDottedName() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "id=key.id, name=value.meta.name");
    TopicConfigs configs = new TopicConfigs("ks", "table", settings, connectorConfig);
    Schema metaSchema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    Schema valueSchema = SchemaBuilder.struct().field("meta", metaSchema).build();
    SinkRecord record = record(valueSchema,
            new Struct(valueSchema).put("meta", new Struct(metaSchema).put("name", "alice")));

    // Not the parent field 'meta': a column mapping names one top-level field, which here does not exist.
    DataException e = assertThrows(DataException.class, () -> configs.setTablePartitionAndColumnValues(record));
    assertTrue(e.getMessage(), e.getMessage().contains("missing field 'meta.name'"));
  }

  @Test
  public void shouldMapHeaderColumnByItsFullDottedName() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "id=key.id, trace=header.trace.id");
    TopicConfigs configs = new TopicConfigs("ks", "table", settings, connectorConfig);
    Schema valueSchema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    SinkRecord record = record(valueSchema, new Struct(valueSchema).put("name", "alice"));
    record.headers().addString("trace", "parent").addString("trace.id", "abc");

    configs.setTablePartitionAndColumnValues(record);

    assertEquals(1, configs.getTableColumnMap().size());
    assertTrue(configs.getTableColumnMap().containsKey("trace.id"));
  }

  private TopicConfigs expirationTopicConfigs(String mapping) {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", mapping);
    settings.put("expirationUnit", "MILLISECONDS");
    return new TopicConfigs("ks", "table", settings, connectorConfig, namesOf(settings));
  }

  // The full setting names the connector config passes, for the settings in the map.
  private static Map<String, String> namesOf(Map<String, String> settings) {
    Map<String, String> names = new HashMap<>();
    settings.keySet().forEach(suffix -> names.put(suffix, "topic.a.ks.table." + suffix));
    return names;
  }

  @Test
  public void shouldRejectExpirySettingsWithoutExpirationField() {
    for (String setting : new String[] {"expirationUnit", "expirationOffsetSeconds", "behaviorOnExpired"}) {
      Map<String, String> settings = new HashMap<>();
      settings.put("mapping", "id=key.id, name=value.name");
      settings.put(setting, "SECONDS");
      ConfigException e = assertThrows(ConfigException.class,
              () -> new TopicConfigs("ks", "table", settings, connectorConfig, namesOf(settings)));
      assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table." + setting));
      assertTrue(e.getMessage(), e.getMessage().contains("no __expiration"));
    }
  }

  @Test
  public void shouldRejectTtlSecondsWithExpirationField() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__expiration=value.validUntil");
    settings.put("expirationUnit", "SECONDS");
    settings.put("ttlSeconds", "3600");
    ConfigException e = assertThrows(ConfigException.class,
            () -> new TopicConfigs("ks", "table", settings, connectorConfig, namesOf(settings)));
    assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.ttlSeconds"));
    assertTrue(e.getMessage(), e.getMessage().contains("comes from __expiration"));
  }

  @Test
  public void shouldRejectInvalidBehaviorOnExpired() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__expiration=value.validUntil");
    settings.put("expirationUnit", "SECONDS");
    settings.put("behaviorOnExpired", "yes");
    ConfigException e = assertThrows(ConfigException.class,
            () -> new TopicConfigs("ks", "table", settings, connectorConfig, namesOf(settings)));
    assertTrue(e.getMessage(), e.getMessage().contains("topic.a.ks.table.behaviorOnExpired"));
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectNegativeExpirationOffset() {
    topicConfigs("__expiration=value.validUntil", "-1");
  }

  private TopicConfigs topicConfigs(String mapping, String offsetSeconds) {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", mapping);
    settings.put("expirationUnit", "MILLISECONDS");
    settings.put("expirationOffsetSeconds", offsetSeconds);
    return new TopicConfigs("ks", "table", settings, connectorConfig);
  }

  private SinkRecord timestampRecord(Date validUntil) {
    Schema valueSchema = SchemaBuilder.struct()
            .field("validUntil", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .build();
    return record(valueSchema, new Struct(valueSchema).put("validUntil", validUntil));
  }

  private SinkRecord longRecord(long validUntil) {
    Schema valueSchema = SchemaBuilder.struct()
            .field("validUntil", Schema.INT64_SCHEMA)
            .build();
    return record(valueSchema, new Struct(valueSchema).put("validUntil", validUntil));
  }

  private SinkRecord record(Schema valueSchema, Struct value) {
    Schema keySchema = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .build();
    Struct key = new Struct(keySchema).put("id", 1L);
    return new SinkRecord("topic", 0, keySchema, key, valueSchema, value, 0L, 1_000L, null);
  }
}
