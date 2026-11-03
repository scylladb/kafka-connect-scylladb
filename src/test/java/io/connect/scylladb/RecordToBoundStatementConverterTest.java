package io.connect.scylladb;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecordToBoundStatementConverterTest {

  BoundStatement bound;

  @Before
  public void before() {
    bound = mock(BoundStatement.class, RETURNS_SELF);
  }

  /** A prepared delete whose only marker is the primary key column, under any of the given spellings. */
  PreparedStatement preparedDelete(String... primaryKeyColumn) {
    ColumnDefinitions variables = mock(ColumnDefinitions.class);
    for (String spelling : primaryKeyColumn) {
      when(variables.contains(spelling)).thenReturn(true);
    }
    PreparedStatement prepared = mock(PreparedStatement.class);
    when(prepared.getVariableDefinitions()).thenReturn(variables);
    when(prepared.bind()).thenReturn(bound);
    return prepared;
  }

  RecordToBoundStatementConverter.State delete(String mapping, String primaryKeyColumn, Struct key) {
    PreparedStatement prepared = preparedDelete(primaryKeyColumn);
    Map<String, String> topicSettings = new HashMap<>();
    topicSettings.put("mapping", mapping);
    TopicConfigs topicConfigs = new TopicConfigs("ks", "t", topicSettings, new ScyllaDbSinkConnectorConfig(
        Collections.singletonMap(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks")));
    SinkRecord tombstone = new SinkRecord("t", 0, key.schema(), key, null, null, 0L);
    topicConfigs.setTablePartitionAndColumnValues(tombstone);
    return new RecordToBoundStatementConverter(prepared)
        .convert(tombstone, topicConfigs, ScyllaDbConstants.DELETE_OPERATION);
  }

  @Test
  public void shouldNotBindNonKeyFieldOnUnmappedDelete() {
    Schema keySchema = SchemaBuilder.struct().name("key")
        .field("id", Schema.INT64_SCHEMA)
        .field("extra", Schema.STRING_SCHEMA)
        .build();
    Struct key = new Struct(keySchema).put("id", 1L).put("extra", "e");
    SinkRecord tombstone = new SinkRecord("t", 0, keySchema, key, null, null, 0L);

    RecordToBoundStatementConverter.State state = new RecordToBoundStatementConverter(preparedDelete("id"))
        .convert(tombstone, null, ScyllaDbConstants.DELETE_OPERATION);

    assertEquals(1, state.parameters);
    verify(bound).setLong("id", 1L);
    verify(bound, never()).setString(eq("extra"), anyString());
  }

  @Test
  public void shouldNotBindValueMappedNonKeyColumnOnDelete() {
    when(bound.setString(eq("username"), anyString()))
        .thenThrow(new IllegalArgumentException("username is not a variable in this bound statement"));
    Schema keySchema = SchemaBuilder.struct().name("key")
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();

    RecordToBoundStatementConverter.State state = delete("userid=key.id, username=value.name", "userid",
        new Struct(keySchema).put("id", 1L).put("name", "n"));

    assertEquals(1, state.parameters);
    verify(bound).setLong("userid", 1L);
    verify(bound, never()).setString(eq("username"), anyString());
  }

  @Test
  public void shouldFallBackToValueNamesakeWhenKeyMappingIsNotPrimaryKey() {
    // The table is keyed by 'id'; 'other' is a regular column that the key field also feeds.
    Schema keySchema = SchemaBuilder.struct().name("key").field("id", Schema.INT64_SCHEMA).build();

    RecordToBoundStatementConverter.State state = delete("other=key.id, id=value.id", "id",
        new Struct(keySchema).put("id", 1L));

    assertEquals(1, state.parameters);
    verify(bound).setLong("id", 1L);
    verify(bound, never()).setLong(eq("other"), anyLong());
  }

  @Test
  public void shouldBindEachDeleteColumnOnceWithKeyMappingWinning() {
    Schema keySchema = SchemaBuilder.struct().name("key")
        .field("alias", Schema.INT64_SCHEMA)
        .field("id", Schema.INT64_SCHEMA)
        .build();

    RecordToBoundStatementConverter.State state = delete("userid=key.id, userid=value.alias", "userid",
        new Struct(keySchema).put("alias", 2L).put("id", 1L));

    assertEquals(1, state.parameters);
    verify(bound).setLong("userid", 1L);
    verify(bound, never()).setLong("userid", 2L);
  }

  @Test
  public void shouldBindEachColumnOnceOnUnmappedDelete() {
    // 'ID' and 'id' name the same unquoted column; the earlier key field binds it.
    Schema keySchema = SchemaBuilder.struct().name("key")
        .field("ID", Schema.INT64_SCHEMA)
        .field("id", Schema.INT64_SCHEMA)
        .build();
    Struct key = new Struct(keySchema).put("ID", 1L).put("id", 2L);
    SinkRecord tombstone = new SinkRecord("t", 0, keySchema, key, null, null, 0L);

    RecordToBoundStatementConverter.State state = new RecordToBoundStatementConverter(preparedDelete("id", "ID"))
        .convert(tombstone, null, ScyllaDbConstants.DELETE_OPERATION);

    assertEquals(1, state.parameters);
    verify(bound).setLong("ID", 1L);
    verify(bound, never()).setLong("id", 2L);
  }
}
