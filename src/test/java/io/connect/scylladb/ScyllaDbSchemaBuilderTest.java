package io.connect.scylladb;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScyllaDbSchemaBuilderTest {

  static final Schema KEY_SCHEMA = SchemaBuilder.struct().name("key")
      .field("id", Schema.INT64_SCHEMA)
      .build();

  ScyllaDbSession session;
  ScyllaDbSchemaBuilder builder;

  @Before
  public void before() {
    Map<String, String> settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    session = mock(ScyllaDbSession.class);
    when(session.tableMetadata("ks", "t")).thenReturn(mock(TableMetadata.Table.class, RETURNS_MOCKS));
    builder = new ScyllaDbSchemaBuilder(session, new ScyllaDbSinkConnectorConfig(settings));
  }

  static SinkRecord record(String topic) {
    Struct key = new Struct(KEY_SCHEMA).put("id", 1L);
    return new SinkRecord(topic, 0, KEY_SCHEMA, key, KEY_SCHEMA, key, 0L);
  }

  @Test
  public void shouldCheckTableOncePerTopic() {
    builder.build("ks", "t", record("a"), null);
    builder.build("ks", "t", record("a"), null);
    verify(session, times(1)).tableMetadata("ks", "t");
  }

  @Test
  public void shouldCheckSharedTableForEachTopic() {
    builder.build("ks", "t", record("a"), null);
    builder.build("ks", "t", record("b"), null);
    verify(session, times(2)).tableMetadata("ks", "t");
  }

  @Test
  public void shouldShareEntryBetweenSpellingsOfOneTable() {
    when(session.tableMetadata("KS", "TBL")).thenReturn(mock(TableMetadata.Table.class, RETURNS_MOCKS));
    com.datastax.oss.driver.api.core.metadata.schema.TableMetadata changed =
        mock(com.datastax.oss.driver.api.core.metadata.schema.TableMetadata.class);
    when(changed.getKeyspace()).thenReturn(CqlIdentifier.fromInternal("ks"));
    when(changed.getName()).thenReturn(CqlIdentifier.fromInternal("tbl"));

    builder.build("KS", "TBL", record("a"), null);
    builder.build("ks", "tbl", record("a"), null);
    verify(session, times(1)).tableMetadata("KS", "TBL");
    verify(session, times(0)).tableMetadata("ks", "tbl");

    builder.onTableUpdated(changed, changed);
    builder.build("KS", "TBL", record("a"), null);
    verify(session, times(2)).tableMetadata("KS", "TBL");
    verify(session).onTableChanged("ks", "tbl");
  }
}
