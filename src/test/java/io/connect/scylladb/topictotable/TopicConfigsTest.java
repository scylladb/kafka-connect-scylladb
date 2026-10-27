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
    TopicConfigs configs = topicConfigs("__ttl=value.ttl", "0");
    Schema valueSchema = SchemaBuilder.struct()
            .field("ttl", Schema.INT32_SCHEMA)
            .build();
    SinkRecord record = record(valueSchema, new Struct(valueSchema).put("ttl", 42));

    configs.setTablePartitionAndColumnValues(record);
    configs.setTtlIfAvailable(record, 1_000_000L);

    assertEquals(Integer.valueOf(42), configs.getTtl());
  }

  @Test
  public void shouldSkipExpiredTimestamp() {
    TopicConfigs configs = topicConfigs("__expiration=value.validUntil", "0");
    SinkRecord record = longRecord(999_999L);

    configs.setTablePartitionAndColumnValues(record);
    assertFalse(configs.setTtlIfAvailable(record, 1_000_000L));
  }

  @Test
  public void shouldRejectExpiredTimestampWhenNotSkipping() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__expiration=value.validUntil");
    settings.put("expirationUnit", "MILLISECONDS");
    settings.put("skipExpired", "false");
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

  @Test(expected = ConfigException.class)
  public void shouldRejectInvalidExpirationUnit() {
    Map<String, String> settings = new HashMap<>();
    settings.put("expirationUnit", "HOURS");
    new TopicConfigs("ks", "table", settings, connectorConfig);
  }

  @Test
  public void shouldRequireExpirationUnitWithExpirationField() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "__expiration=value.validUntil");
    ConfigException e = assertThrows(ConfigException.class,
            () -> new TopicConfigs("ks", "table", settings, connectorConfig));
    assertTrue(e.getMessage(), e.getMessage().contains("expirationUnit"));
  }

  @Test
  public void shouldNotRequireExpirationUnitWithoutExpirationField() {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", "id=key.id, name=value.name");
    new TopicConfigs("ks", "table", settings, connectorConfig);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectTtlAndExpirationInSameMapping() {
    topicConfigs("__ttl=value.ttl, __expiration=value.validUntil", "0");
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectExpirationFromKey() {
    topicConfigs("id=key.id, __expiration=key.validUntil", "0");
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectNegativeExpirationOffsetWithoutExpirationField() {
    topicConfigs("id=key.id", "-1");
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectInvalidSkipExpired() {
    Map<String, String> settings = new HashMap<>();
    settings.put("skipExpired", "yes");
    new TopicConfigs("ks", "table", settings, connectorConfig);
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
