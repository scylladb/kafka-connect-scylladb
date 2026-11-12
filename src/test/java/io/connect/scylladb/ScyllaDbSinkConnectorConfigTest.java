package io.connect.scylladb;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.connect.scylladb.topictotable.TopicConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ScyllaDbSinkConnectorConfigTest {

  Map<String, String> settings;
  ScyllaDbSinkConnectorConfig config;

  @Before
  public void before() {
    settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
    config = null;
  }

  @Test
  public void shouldAcceptValidConfig() {
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, "9042");
    config = new ScyllaDbSinkConnectorConfig(settings);
    assertNotNull(config);
  }

  @Test
  public void shouldUseDefaults() {
    config = new ScyllaDbSinkConnectorConfig(settings);
    assertEquals(true, config.keyspaceCreateEnabled);
  }

  @Test
  public void shouldKeyTopicSettingsByRawTopicName() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id");
    settings.put("topic.a.b-c.ks1.t1.ttlSeconds", "3600");
    config = new ScyllaDbSinkConnectorConfig(settings);
    TopicConfigs topicConfigs = config.topicWiseConfigs.get("a.b-c");
    assertNotNull(topicConfigs);
    assertEquals("ks1", topicConfigs.getKeyspace());
    assertEquals("t1", topicConfigs.getTable());
    assertEquals("id=key.id", topicConfigs.getMappingStringForTopic());
    assertEquals(Integer.valueOf(3600), topicConfigs.getTtl());
    assertNull(config.topicWiseConfigs.get("a_b_c"));
  }

  @Test
  public void shouldRejectColumnMappedFromKeyAndValue() {
    String setting = "topic.a.b-c.ks1.T1.mapping";
    settings.put(setting, "userid=key.id, userid=value.id");

    ConfigException ex = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(ex.getMessage(), ex.getMessage().contains(setting));
    assertTrue(ex.getMessage(), ex.getMessage().contains("Column 'userid'"));
  }

  @Test
  public void shouldRejectEquivalentColumnNamesMappedFromDifferentFields() {
    settings.put("topic.a.ks1.t1.mapping", "USERID=value.id, \"userid\"=header.id");

    ConfigException ex = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(ex.getMessage(), ex.getMessage().contains("Column 'userid'"));
  }

  @Test
  public void shouldAcceptDistinctQuotedColumnNames() {
    settings.put("topic.a.ks1.t1.mapping", "userid=key.id, \"USERID\"=value.id");

    assertNotNull(new ScyllaDbSinkConnectorConfig(settings).topicWiseConfigs.get("a"));
  }

  @Test
  public void shouldKeyTopicSettingsForHyphenatedTopic() {
    settings.put("topic.topic-name.ks.tablename.ttlSeconds", "3600");
    config = new ScyllaDbSinkConnectorConfig(settings);
    assertEquals(Integer.valueOf(3600), config.topicWiseConfigs.get("topic-name").getTtl());
    assertEquals("tablename", config.topicWiseConfigs.get("topic-name").getTable());
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectTwoTargetsForOneTopic() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id");
    settings.put("topic.a.b-c.ks1.t2.ttlSeconds", "3600");
    new ScyllaDbSinkConnectorConfig(settings);
  }

  @Test
  public void shouldTreatUnquotedTargetNamesCaseInsensitively() {
    settings.put("topic.a.b-c.ks1.T1.mapping", "id=key.id");
    settings.put("topic.a.b-c.KS1.t1.ttlSeconds", "3600");
    assertNotNull(new ScyllaDbSinkConnectorConfig(settings).topicWiseConfigs.get("a.b-c"));
  }

  @Test
  public void shouldRejectTargetNamesThatAreNotCql() {
    settings.put("topic.a.1ks.t1.ttlSeconds", "3600");
    ConfigException keyspace = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(keyspace.getMessage(), keyspace.getMessage().contains("topic.a.1ks.t1.ttlSeconds"));

    settings.clear();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
    settings.put("topic.a.ks1.a-b.ttlSeconds", "3600");
    ConfigException table = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(table.getMessage(), table.getMessage().contains("topic.a.ks1.a-b.ttlSeconds"));
  }

  @Test
  public void shouldRejectEmptyQuotedTargetName() {
    settings.put("topic.a.ks1.\"\".mapping", "id=key.id");
    ConfigException ex = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(ex.getMessage(), ex.getMessage().contains("topic.a.ks1.\"\".mapping"));
  }

  @Test
  public void shouldRejectUndoubledQuoteInQuotedTargetName() {
    settings.put("topic.a.\"a\"b\".t1.ttlSeconds", "3600");
    ConfigException keyspace = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(keyspace.getMessage(), keyspace.getMessage().contains("must be valid CQL names"));

    settings.clear();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
    settings.put("topic.a.ks1.\"a\"b\".ttlSeconds", "3600");
    ConfigException table = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(table.getMessage(), table.getMessage().contains("must be valid CQL names"));
  }

  @Test
  public void shouldAcceptDoubledQuoteInQuotedTargetName() {
    settings.put("topic.a.ks1.\"a\"\"b\".ttlSeconds", "3600");
    String table = new ScyllaDbSinkConnectorConfig(settings).topicWiseConfigs.get("a").getTable();
    assertEquals("a\"b", CqlIdentifier.fromCql(table).asInternal());
  }

  @Test
  public void shouldRejectUnbalancedQuoteInTargetName() {
    settings.put("topic.a.ks1.\"abc.ttlSeconds", "3600");
    ConfigException ex = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(ex.getMessage(), ex.getMessage().contains("must be valid CQL names"));
  }

  @Test
  public void shouldUseKafkaTimestampByDefault() {
    config = new ScyllaDbSinkConnectorConfig(settings);
    assertNull(config.writeTimestamp);
  }

  @Test
  public void shouldLetPerTopicTimestampSettingsOverrideConnectorOnes() {
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "value.lut");
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_UNIT_CONFIG, "MILLISECONDS");
    settings.put("topic.a.ks.t.timestampUnit", "SECONDS");
    settings.put("topic.b.ks.t.timestampField", "header.lut");
    config = new ScyllaDbSinkConnectorConfig(settings);
    assertNotNull(config.writeTimestamp);
    SinkRecord record = new SinkRecord("a", 0, null, null, null,
        java.util.Collections.singletonMap("lut", 2L), 0L, 0L, null);
    record.headers().addLong("lut", 3L);
    assertEquals(2_000L, config.writeTimestamp.micros(record));
    assertEquals(2_000_000L, config.topicWiseConfigs.get("a").getWriteTimestamp().micros(record));
    assertEquals(3_000L, config.topicWiseConfigs.get("b").getWriteTimestamp().micros(record));
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectInvalidTimestampField() {
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "lut");
    new ScyllaDbSinkConnectorConfig(settings);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectInvalidTimestampUnit() {
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_UNIT_CONFIG, "HOURS");
    new ScyllaDbSinkConnectorConfig(settings);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectInvalidPerTopicTimestampUnitWithoutField() {
    settings.put("topic.a.ks.t.timestampUnit", "HOURS");
    new ScyllaDbSinkConnectorConfig(settings);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectInvalidPerTopicTimestampUnit() {
    settings.put("topic.a.ks.t.timestampField", "value.lut");
    settings.put("topic.a.ks.t.timestampUnit", "HOURS");
    new ScyllaDbSinkConnectorConfig(settings);
  }

  @Test
  public void shouldNameInvalidPerTopicTimestampSetting() {
    settings.put("topic.a.ks.t.timestampUnit", "HOURS");
    ConfigException unit = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(unit.getMessage(), unit.getMessage().contains("topic.a.ks.t.timestampUnit"));

    settings.remove("topic.a.ks.t.timestampUnit");
    settings.put("topic.a.ks.t.timestampField", "lut");
    ConfigException field = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(field.getMessage(), field.getMessage().contains("topic.a.ks.t.timestampField"));
  }

  @Test
  public void shouldRetryWithoutLimitByDefault() {
    config = new ScyllaDbSinkConnectorConfig(settings);
    assertEquals(-1, config.maxRetries);
    assertEquals(3000L, config.retryBackoffMs);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectMaxRetriesBelowMinusOne() {
    settings.put(ScyllaDbSinkConnectorConfig.MAX_RETRIES_CONFIG, "-2");
    new ScyllaDbSinkConnectorConfig(settings);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectZeroRetryBackoff() {
    settings.put(ScyllaDbSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, "0");
    new ScyllaDbSinkConnectorConfig(settings);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectSerialConsistencyLevel() {
    settings.put(ScyllaDbSinkConnectorConfig.CONSISTENCY_LEVEL_CONFIG, "LOCAL_SERIAL");
    new ScyllaDbSinkConnectorConfig(settings);
  }

  @Test
  public void shouldRejectSerialTopicConsistencyLevel() {
    settings.put("topic.a.b-c.ks1.t1.consistencyLevel", "SERIAL");
    ConfigException ex = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(ex.getMessage(), ex.getMessage().contains("topic.a.b-c.ks1.t1.consistencyLevel"));
  }

  @Test
  public void shouldListOnlyWritableTopicConsistencyLevels() {
    settings.put("topic.a.ks1.t1.consistencyLevel", "FOO");
    DataException ex = assertThrows(DataException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(ex.getMessage(), ex.getMessage().contains("LOCAL_QUORUM"));
    assertFalse(ex.getMessage(), ex.getMessage().contains("SERIAL"));
  }

  //TODO: Add more tests

  @Test
  public void shouldTrimPerTopicTimestampField() {
    settings.put("topic.a.ks.t.timestampField", "value.lut ");
    config = new ScyllaDbSinkConnectorConfig(settings);
    SinkRecord record = new SinkRecord("a", 0, null, null, null,
        java.util.Collections.singletonMap("lut", 2L), 0L, 0L, null);
    assertEquals(2L, config.topicWiseConfigs.get("a").getWriteTimestamp().micros(record));
  }

  @Test
  public void shouldTurnOffTimestampFieldWithBlankPerTopicValue() {
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "value.lut");
    settings.put("topic.a.ks.t.timestampField", "  ");
    config = new ScyllaDbSinkConnectorConfig(settings);
    assertNull(config.topicWiseConfigs.get("a").getWriteTimestamp());
  }

  @Test
  public void shouldNameInvalidConnectorTimestampField() {
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "lut");
    ConfigException ex = assertThrows(ConfigException.class, () -> new ScyllaDbSinkConnectorConfig(settings));
    assertTrue(ex.getMessage(), ex.getMessage().contains(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG));
  }
}
