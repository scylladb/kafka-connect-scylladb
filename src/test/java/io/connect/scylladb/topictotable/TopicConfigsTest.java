package io.connect.scylladb.topictotable;

import io.connect.scylladb.ScyllaDbSinkConnectorConfig;
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
    configs.setTtlAndTimeStampIfAvailable(record, 1_000_000L);

    assertEquals(Integer.valueOf(86_410), configs.getTtl());
  }

  @Test
  public void shouldCalculateTtlFromEpochMillisecondsAndRoundUp() {
    TopicConfigs configs = topicConfigs("__expiration=value.validUntil", "0");
    SinkRecord record = longRecord(1_001_001L);

    configs.setTablePartitionAndColumnValues(record);
    configs.setTtlAndTimeStampIfAvailable(record, 1_000_000L);

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
    configs.setTtlAndTimeStampIfAvailable(record, 1_000_000L);

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
    configs.setTtlAndTimeStampIfAvailable(record, 1_000_000L);

    assertEquals(Integer.valueOf(42), configs.getTtl());
  }

  @Test(expected = DataException.class)
  public void shouldRejectExpiredTimestamp() {
    TopicConfigs configs = topicConfigs("__expiration=value.validUntil", "0");
    SinkRecord record = longRecord(999_999L);

    configs.setTablePartitionAndColumnValues(record);
    configs.setTtlAndTimeStampIfAvailable(record, 1_000_000L);
  }

  @Test(expected = DataException.class)
  public void shouldRejectTtlAndExpirationInSameMapping() {
    TopicConfigs configs = topicConfigs(
            "__ttl=value.ttl, __expiration=value.validUntil",
            "0");
    Schema valueSchema = SchemaBuilder.struct()
            .field("ttl", Schema.INT32_SCHEMA)
            .field("validUntil", Schema.INT64_SCHEMA)
            .build();
    Struct value = new Struct(valueSchema)
            .put("ttl", 10)
            .put("validUntil", 1_010_000L);

    configs.setTablePartitionAndColumnValues(record(valueSchema, value));
  }

  @Test(expected = DataException.class)
  public void shouldRejectNegativeExpirationOffset() {
    topicConfigs("__expiration=value.validUntil", "-1");
  }

  private TopicConfigs topicConfigs(String mapping, String offsetSeconds) {
    Map<String, String> settings = new HashMap<>();
    settings.put("mapping", mapping);
    settings.put("expirationOffsetSeconds", offsetSeconds);
    return new TopicConfigs(settings, connectorConfig);
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
