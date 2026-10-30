package io.connect.scylladb;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import io.connect.scylladb.topictotable.TopicConfigs;
import io.connect.scylladb.utils.ScyllaDbConstants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScyllaDbSinkTaskHelperTest {

  static final String TOPIC = "a.b-c";
  static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("key")
      .field("id", Schema.INT64_SCHEMA)
      .build();
  static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("value")
      .field("id", Schema.INT64_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("time", Schema.INT32_SCHEMA)
      .build();
  // A delete flagged in the value carries no written fields, only change metadata.
  static final Schema FLAGGED_SCHEMA = SchemaBuilder.struct().name("flagged")
      .field("op", Schema.STRING_SCHEMA)
      .field("time", Schema.INT32_SCHEMA)
      .build();

  Map<String, String> settings;
  ScyllaDbSession session;
  RecordToBoundStatementConverter converter;
  BoundStatement statement;

  @Before
  public void before() {
    settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
    session = mock(ScyllaDbSession.class);
    converter = mock(RecordToBoundStatementConverter.class);
    statement = mock(BoundStatement.class, RETURNS_SELF);
    RecordToBoundStatementConverter.State state = new RecordToBoundStatementConverter.State(statement);
    state.parameters = 1;
    when(converter.convert(any(), any(), anyString())).thenReturn(state);
    when(session.insert(anyString(), anyString(), any())).thenReturn(converter);
    when(session.delete(anyString(), anyString())).thenReturn(converter);
    when(session.tableExists(anyString(), anyString())).thenReturn(true);
  }

  ScyllaDbSinkTaskHelper helper() {
    return new ScyllaDbSinkTaskHelper(new ScyllaDbSinkConnectorConfig(settings), session);
  }

  static Struct key(long id) {
    return new Struct(KEY_SCHEMA).put("id", id);
  }

  static SinkRecord write(String topic, long id) {
    Struct value = new Struct(VALUE_SCHEMA).put("id", id).put("name", "n").put("time", 100);
    return new SinkRecord(topic, 0, KEY_SCHEMA, key(id), VALUE_SCHEMA, value, 1L, 1000L, null);
  }

  static SinkRecord flagged(String topic, long id, String op) {
    Struct value = new Struct(FLAGGED_SCHEMA).put("op", op).put("time", 100);
    return new SinkRecord(topic, 0, KEY_SCHEMA, key(id), FLAGGED_SCHEMA, value, 3L, 1000L, null);
  }

  static SinkRecord write(String topic, long id, int time) {
    Struct value = new Struct(VALUE_SCHEMA).put("id", id).put("name", "n").put("time", time);
    return new SinkRecord(topic, 0, KEY_SCHEMA, key(id), VALUE_SCHEMA, value, 1L, 1000L, null);
  }

  static SinkRecord tombstone(String topic, long id) {
    return new SinkRecord(topic, 0, KEY_SCHEMA, key(id), null, null, 2L, 1000L, null);
  }

  @Test
  public void shouldApplyPerTopicSettingsToDottedTopic() {
    settings.put("topic.a.b-c.ks1.t1.consistencyLevel", "LOCAL_ONE");
    SinkRecord record = write(TOPIC, 1L);

    helper().getBoundStatementForRecord(record);

    verify(session).createOrAlterTable(eq("ks1"), eq("t1"), eq(record), any(TopicConfigs.class));
    verify(session).insert(eq("ks1"), eq("t1"), any(TopicConfigs.class));
    verify(statement).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE);
  }

  @Test
  public void shouldWriteToSanitizedTopicTableWithoutPerTopicSettings() {
    SinkRecord record = write(TOPIC, 1L);

    helper().getBoundStatementForRecord(record);

    verify(session).insert("scylladb", "a_b_c", null);
    verify(statement).setQueryTimestamp(1000L * 1000);
  }

  @Test
  public void shouldAcceptSettingsWrittenWithSanitizedTopic() {
    settings.put("topic.a_b_c.ks1.t1.consistencyLevel", "LOCAL_ONE");

    helper().getBoundStatementForRecord(write(TOPIC, 1L));

    verify(session).insert(eq("ks1"), eq("t1"), any(TopicConfigs.class));
    verify(statement).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE);
  }

  @Test
  public void shouldDeleteFromPerTopicTable() {
    settings.put("topic.a.b-c.ks1.t1.deletesEnabled", "true");

    helper().getBoundStatementForRecord(tombstone(TOPIC, 1L));

    verify(session).tableExists("ks1", "t1");
    verify(session).delete("ks1", "t1");
  }

  @Test
  public void shouldTakeDeleteTimestampFromKeyField() {
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_ENABLE_CONFIG, "true");
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "key.id");
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_UNIT_CONFIG, "MILLISECONDS");

    helper().getBoundStatementForRecord(tombstone(TOPIC, 42L));

    verify(session).delete("scylladb", "a_b_c");
    verify(statement).setQueryTimestamp(42_000L);
  }

  @Test
  public void shouldTakeWriteTimestampFromPerTopicField() {
    settings.put("topic.a.b-c.ks1.t1.timestampField", "value.time");
    settings.put("topic.a.b-c.ks1.t1.timestampUnit", "SECONDS");

    helper().getBoundStatementForRecord(write(TOPIC, 1L));

    verify(statement).setQueryTimestamp(100_000_000L);
  }

  @Test(expected = DataException.class)
  public void shouldFailTombstoneWhenTimestampFieldIsInValue() {
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_ENABLE_CONFIG, "true");
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "value.time");

    helper().getBoundStatementForRecord(tombstone(TOPIC, 1L));
  }

  @Test
  public void shouldDeleteWithValueMappedTtl() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id, name=value.name, __ttl=value.time");
    settings.put("topic.a.b-c.ks1.t1.deletesEnabled", "true");

    helper().getBoundStatementForRecord(tombstone(TOPIC, 1L));

    verify(session).delete("ks1", "t1");
  }

  @Test
  public void shouldCompleteValueMappingAfterLeadingTombstone() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "userid=key.id, username=value.name");
    settings.put("topic.a.b-c.ks1.t1.deletesEnabled", "true");
    ScyllaDbSinkTaskHelper helper = helper();

    helper.getBoundStatementForRecord(tombstone(TOPIC, 1L));
    SinkRecord record = write(TOPIC, 2L);
    helper.getBoundStatementForRecord(record);

    ArgumentCaptor<TopicConfigs> topicConfigs = ArgumentCaptor.forClass(TopicConfigs.class);
    verify(session).createOrAlterTable(eq("ks1"), eq("t1"), eq(record), topicConfigs.capture());
    assertEquals(Schema.STRING_SCHEMA,
        topicConfigs.getValue().getTableColumnMap().get("name").getKafkaRecordField().schema());
  }

  @Test
  public void shouldSkipDeleteFromMissingTable() {
    settings.put("topic.a.b-c.ks1.t1.deletesEnabled", "true");
    when(session.tableExists("ks1", "t1")).thenReturn(false);

    assertNull(helper().getBoundStatementForRecord(tombstone(TOPIC, 1L)));
    verify(session, never()).delete(anyString(), anyString());
  }

  @Test
  public void shouldNotBorrowSettingsOfListedSanitizedTopic() {
    settings.put("topics", "a.b-c,a_b_c");
    settings.put("topic.a_b_c.ks1.t1.consistencyLevel", "LOCAL_ONE");

    helper().getBoundStatementForRecord(write(TOPIC, 1L));

    verify(session).insert("scylladb", "a_b_c", null);
  }

  @Test
  public void shouldNotBorrowSanitizedSettingsUnderTopicsRegex() {
    settings.put("topics.regex", "a.*");
    settings.put("topic.a_b_c.ks1.t1.consistencyLevel", "LOCAL_ONE");
    ScyllaDbSinkTaskHelper helper = helper();

    helper.getBoundStatementForRecord(write(TOPIC, 1L));
    helper.getBoundStatementForRecord(write("a_b_c", 2L));

    verify(session).insert("scylladb", "a_b_c", null);
    verify(session).insert(eq("ks1"), eq("t1"), any(TopicConfigs.class));
  }

  @Test
  public void shouldTakeLegacyTimestampMappingInMicroseconds() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id, name=value.name, __timestamp=value.id");

    helper().getBoundStatementForRecord(write(TOPIC, 42L));

    verify(statement).setQueryTimestamp(42L);
  }

  @Test
  public void shouldUseKafkaTimestampForLegacyMappingOnTombstone() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id, name=value.name, __timestamp=value.id");
    settings.put("topic.a.b-c.ks1.t1.deletesEnabled", "true");

    helper().getBoundStatementForRecord(tombstone(TOPIC, 42L));

    verify(statement).setQueryTimestamp(1000L * 1000);
  }

  @Test
  public void shouldPreferTimestampFieldOverLegacyMapping() {
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "value.id");
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_UNIT_CONFIG, "MILLISECONDS");
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id, name=value.name, __timestamp=value.id");

    helper().getBoundStatementForRecord(write(TOPIC, 42L));

    verify(statement).setQueryTimestamp(42_000L);
  }

  void flagDeletes() {
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_FIELD_CONFIG, "value.op");
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_VALUES_CONFIG, "delete");
  }

  @Test
  public void shouldDeleteRecordFlaggedInValue() {
    flagDeletes();

    helper().getBoundStatementForRecord(flagged(TOPIC, 1L, "delete"));

    verify(session).delete("scylladb", "a_b_c");
    verify(converter).convert(any(), any(), eq(ScyllaDbConstants.DELETE_OPERATION));
    verify(session, never()).insert(anyString(), anyString(), any());
    verify(session, never()).createOrAlterTable(anyString(), anyString(), any(), any());
  }

  @Test
  public void shouldWriteRecordWithOtherFlagValue() {
    flagDeletes();

    helper().getBoundStatementForRecord(flagged(TOPIC, 1L, "write"));

    verify(session).insert("scylladb", "a_b_c", null);
    verify(session, never()).delete(anyString(), anyString());
  }

  @Test
  public void shouldWriteRecordWithoutFlagField() {
    flagDeletes();

    helper().getBoundStatementForRecord(write(TOPIC, 1L));

    verify(session).insert("scylladb", "a_b_c", null);
  }

  @Test(expected = DataException.class)
  public void shouldFailFlaggedDeleteWhenDeletesAreDisabled() {
    flagDeletes();
    settings.put(ScyllaDbSinkConnectorConfig.DELETES_ENABLE_CONFIG, "false");

    helper().getBoundStatementForRecord(flagged(TOPIC, 1L, "delete"));
  }

  @Test
  public void shouldSkipFlaggedDeleteFromMissingTable() {
    flagDeletes();
    when(session.tableExists("scylladb", "a_b_c")).thenReturn(false);

    assertNull(helper().getBoundStatementForRecord(flagged(TOPIC, 1L, "delete")));
    verify(session, never()).delete(anyString(), anyString());
  }

  @Test
  public void shouldTakeFlaggedDeleteTimestampFromValueField() {
    flagDeletes();
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_FIELD_CONFIG, "value.time");
    settings.put(ScyllaDbSinkConnectorConfig.TIMESTAMP_UNIT_CONFIG, "SECONDS");

    helper().getBoundStatementForRecord(flagged(TOPIC, 1L, "delete"));

    verify(session).delete("scylladb", "a_b_c");
    verify(statement).setQueryTimestamp(100_000_000L);
  }

  @Test
  public void shouldCompleteValueMappingAfterLeadingFlaggedDelete() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "userid=key.id, username=value.name, __ttl=value.time");
    settings.put("topic.a.b-c.ks1.t1.deletesField", "value.op");
    settings.put("topic.a.b-c.ks1.t1.deletesValues", "delete");
    ScyllaDbSinkTaskHelper helper = helper();

    helper.getBoundStatementForRecord(flagged(TOPIC, 1L, "delete"));
    SinkRecord record = write(TOPIC, 2L);
    helper.getBoundStatementForRecord(record);

    verify(session).delete("ks1", "t1");
    ArgumentCaptor<TopicConfigs> topicConfigs = ArgumentCaptor.forClass(TopicConfigs.class);
    verify(session).createOrAlterTable(eq("ks1"), eq("t1"), eq(record), topicConfigs.capture());
    assertEquals(Schema.STRING_SCHEMA,
        topicConfigs.getValue().getTableColumnMap().get("name").getKafkaRecordField().schema());
  }

  @Test
  public void shouldTakeLeadingFlaggedDeleteTimestampFromLegacyMapping() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "userid=key.id, username=value.name, __timestamp=value.time");
    settings.put("topic.a.b-c.ks1.t1.deletesEnabled", "true");
    settings.put("topic.a.b-c.ks1.t1.deletesField", "value.op");
    settings.put("topic.a.b-c.ks1.t1.deletesValues", "delete");

    helper().getBoundStatementForRecord(flagged(TOPIC, 1L, "delete"));

    verify(session).delete("ks1", "t1");
    // The mapped field, in microseconds, not the Kafka record timestamp.
    verify(statement).setQueryTimestamp(100L);
  }

  @Test
  public void shouldNotReadMappedTtlFromFlaggedDelete() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "userid=key.id, username=value.name, __ttl=value.time");
    settings.put("topic.a.b-c.ks1.t1.deletesField", "value.op");
    settings.put("topic.a.b-c.ks1.t1.deletesValues", "delete");
    ScyllaDbSinkTaskHelper helper = helper();
    Schema opOnly = SchemaBuilder.struct().name("op").field("op", Schema.STRING_SCHEMA).build();
    SinkRecord delete = new SinkRecord(TOPIC, 0, KEY_SCHEMA, key(1L), opOnly,
        new Struct(opOnly).put("op", "delete"), 3L, 1000L, null);

    helper.getBoundStatementForRecord(write(TOPIC, 1L));
    helper.getBoundStatementForRecord(delete);

    verify(session).delete("ks1", "t1");
  }

  @Test
  public void shouldSkipRecordThatHasAlreadyExpired() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id, name=value.name, __expiration=value.time");
    settings.put("topic.a.b-c.ks1.t1.expirationUnit", "SECONDS");

    assertNull(helper().getBoundStatementForRecord(write(TOPIC, 1L)));

    verify(session, never()).createOrAlterTable(anyString(), anyString(), any(), any());
    verify(session, never()).insert(anyString(), anyString(), any());
  }

  @Test
  public void shouldFailRecordThatHasAlreadyExpiredWhenNotSkipping() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id, name=value.name, __expiration=value.time");
    settings.put("topic.a.b-c.ks1.t1.expirationUnit", "SECONDS");
    settings.put("topic.a.b-c.ks1.t1.skipExpired", "false");
    ScyllaDbSinkTaskHelper helper = helper();

    DataException e = assertThrows(DataException.class, () -> helper.getBoundStatementForRecord(write(TOPIC, 1L)));
    assertTrue(e.getMessage(), e.getMessage().contains("expired"));
  }

  @Test
  public void shouldBindTtlOfEachRecord() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id, name=value.name, __expiration=value.time");
    settings.put("topic.a.b-c.ks1.t1.expirationUnit", "SECONDS");
    ScyllaDbSinkTaskHelper helper = helper();
    final int expiresAt = (int) (System.currentTimeMillis() / 1000) + 3600;

    helper.getBoundStatementForRecord(write(TOPIC, 1L, expiresAt));
    // A record that never expires must not keep the TTL of the one before.
    helper.getBoundStatementForRecord(write(TOPIC, 2L, 0));

    ArgumentCaptor<Integer> ttl = ArgumentCaptor.forClass(Integer.class);
    verify(statement, times(2)).setInt(eq(ScyllaDbConstants.TTL_BIND_MARKER), ttl.capture());
    assertEquals(3600, ttl.getAllValues().get(0), 1);
    assertEquals(Integer.valueOf(0), ttl.getAllValues().get(1));
  }
}
