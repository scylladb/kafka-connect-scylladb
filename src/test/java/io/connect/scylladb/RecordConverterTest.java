package io.connect.scylladb;

import io.connect.scylladb.topictotable.TopicConfigs;
import io.connect.scylladb.utils.ScyllaDbConstants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RecordConverterTest {

  static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("key")
      .field("id", Schema.INT64_SCHEMA)
      .build();

  RecordConverter<Object> converter;
  ScyllaDbSinkConnectorConfig config;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    converter = mock(RecordConverter.class, CALLS_REAL_METHODS);
    config = new ScyllaDbSinkConnectorConfig(
        Collections.singletonMap(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks"));
  }

  void delete(String mapping) {
    Map<String, String> topicSettings = new HashMap<>();
    topicSettings.put("mapping", mapping);
    TopicConfigs topicConfigs = new TopicConfigs("ks", "t", topicSettings, config);
    Struct key = new Struct(KEY_SCHEMA).put("id", 1L);
    SinkRecord tombstone = new SinkRecord("t", 0, KEY_SCHEMA, key, null, null, 0L);
    topicConfigs.setTablePartitionAndColumnValues(tombstone);
    converter.convert(tombstone, topicConfigs, ScyllaDbConstants.DELETE_OPERATION);
  }

  @Test
  public void shouldDeleteByKeyFieldMappedThroughValue() {
    delete("userid=value.id, name=value.name");
    verify(converter).setInt64Field(any(), eq("userid"), eq(1L));
    verify(converter, times(1)).setInt64Field(any(), any(), anyLong());
  }

  @Test
  public void shouldDeleteByUnmappedKeyFieldName() {
    delete("name=value.name");
    verify(converter).setInt64Field(any(), eq("id"), eq(1L));
  }

  @Test
  public void shouldDeleteByKeyMapping() {
    delete("userid=key.id, id=value.other");
    verify(converter).setInt64Field(any(), eq("userid"), eq(1L));
    verify(converter, times(1)).setInt64Field(any(), any(), anyLong());
  }
}
