package io.connect.scylladb;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import io.connect.scylladb.topictotable.TopicConfigs;
import io.connect.scylladb.utils.ScyllaDbConstants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

  @Test
  public void convertsSchemalessListOfMapsToListOfUdtValues() {
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    BoundStatement boundStatement = mock(BoundStatement.class);
    ColumnDefinitions definitions = mock(ColumnDefinitions.class);
    ColumnDefinition definition = mock(ColumnDefinition.class);
    ListType listType = mock(ListType.class);
    UserDefinedType udtType = mock(UserDefinedType.class);
    UdtValue firstUdtValue = mock(UdtValue.class);
    UdtValue secondUdtValue = mock(UdtValue.class);

    when(preparedStatement.getVariableDefinitions()).thenReturn(definitions);
    when(definitions.get("systems_sids")).thenReturn(definition);
    when(definition.getType()).thenReturn(listType);
    when(listType.getElementType()).thenReturn(udtType);
    when(udtType.newValue()).thenReturn(firstUdtValue, secondUdtValue);
    when(udtType.contains("sid")).thenReturn(true);
    when(udtType.contains("system")).thenReturn(true);
    when(udtType.firstIndexOf("sid")).thenReturn(0);
    when(udtType.firstIndexOf("system")).thenReturn(1);
    when(udtType.getFieldTypes()).thenReturn(
        Arrays.asList(DataTypes.listOf(DataTypes.TEXT), DataTypes.TEXT)
    );
    when(firstUdtValue.setList(
        CqlIdentifier.fromInternal("sid"), Collections.emptyList(), String.class
    )).thenReturn(firstUdtValue);
    when(firstUdtValue.set(
        CqlIdentifier.fromInternal("system"), "s1", String.class
    )).thenReturn(firstUdtValue);
    when(secondUdtValue.setList(
        CqlIdentifier.fromInternal("sid"), Collections.emptyList(), String.class
    )).thenReturn(secondUdtValue);
    when(secondUdtValue.set(
        CqlIdentifier.fromInternal("system"), "s2", String.class
    )).thenReturn(secondUdtValue);
    when(boundStatement.setList(
        CqlIdentifier.fromInternal("systems_sids"),
        Arrays.asList(firstUdtValue, secondUdtValue),
        UdtValue.class
    )).thenReturn(boundStatement);

    Map<String, Object> first = new LinkedHashMap<>();
    first.put("sid", Collections.emptyList());
    first.put("system", "s1");
    Map<String, Object> second = new LinkedHashMap<>();
    second.put("sid", Collections.emptyList());
    second.put("system", "s2");
    List<Map<String, Object>> value = Arrays.asList(first, second);

    RecordToBoundStatementConverter converter = new RecordToBoundStatementConverter(preparedStatement);
    RecordToBoundStatementConverter.State state =
        new RecordToBoundStatementConverter.State(boundStatement);

    converter.setArray(state, "systems_sids", null, value);

    verify(firstUdtValue).setList(
        CqlIdentifier.fromInternal("sid"), Collections.emptyList(), String.class
    );
    verify(firstUdtValue).set(
        CqlIdentifier.fromInternal("system"), "s1", String.class
    );
    verify(secondUdtValue).setList(
        CqlIdentifier.fromInternal("sid"), Collections.emptyList(), String.class
    );
    verify(secondUdtValue).set(
        CqlIdentifier.fromInternal("system"), "s2", String.class
    );
    verify(boundStatement).setList(
        CqlIdentifier.fromInternal("systems_sids"),
        Arrays.asList(firstUdtValue, secondUdtValue),
        UdtValue.class
    );
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
