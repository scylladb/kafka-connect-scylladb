package io.connect.scylladb;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import io.connect.scylladb.topictotable.TopicConfigs;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
  public void shouldDeleteDespiteValueMappedTtl() {
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
  public void shouldFailDeleteBindingNoKeyColumnAsDataException() {
    settings.put("topic.a.b-c.ks1.t1.deletesEnabled", "true");
    RecordToBoundStatementConverter.State empty = new RecordToBoundStatementConverter.State(statement);
    when(converter.convert(any(), any(), anyString())).thenReturn(empty);

    assertThrows(DataException.class, () -> helper().getBoundStatementForRecord(tombstone(TOPIC, 1L)));
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
}
