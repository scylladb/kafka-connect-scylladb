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
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecordToBoundStatementConverterTest {

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
}
