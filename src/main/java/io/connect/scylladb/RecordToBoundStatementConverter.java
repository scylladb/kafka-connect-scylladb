package io.connect.scylladb;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.SettableById;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

class RecordToBoundStatementConverter extends RecordConverter<RecordToBoundStatementConverter.State> {
  private final PreparedStatement preparedStatement;

  static class State {

    public BoundStatement statement;
    public int parameters = 0;

    State(BoundStatement statement) {
      this.statement = statement;
    }
  }

  RecordToBoundStatementConverter(PreparedStatement preparedStatement) {
    this.preparedStatement = preparedStatement;
  }

  protected RecordToBoundStatementConverter.State newValue() {
    BoundStatement boundStatement = this.preparedStatement.bind();
    return new State(boundStatement);
  }

  protected void setStringField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      String value
  ) {
    state.statement = state.statement.setString(fieldName, value);
    state.parameters++;
  }

  protected void setFloat32Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Float value
  ) {
    state.statement = state.statement.setFloat(fieldName, value);
    state.parameters++;
  }

  protected void setFloat64Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Double value
  ) {
    state.statement = state.statement.setDouble(fieldName, value);
    state.parameters++;
  }

  protected void setTimestampField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Date value
  ) {
    state.statement = state.statement.setInstant(fieldName, value.toInstant());
    state.parameters++;
  }

  protected void setDateField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Date value
  ) {
    state.statement = state.statement.setLocalDate(fieldName, LocalDate.from(value.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()));
    state.parameters++;
  }

  protected void setTimeField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Date value
  ) {
    final long nanoseconds = TimeUnit.NANOSECONDS.convert(value.getTime(), TimeUnit.MILLISECONDS);
    state.statement = state.statement.setLocalTime(fieldName, LocalTime.ofNanoOfDay(nanoseconds));

    state.parameters++;
  }

  protected void setInt8Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Byte value
  ) {
    state.statement = state.statement.setByte(fieldName, value);
    state.parameters++;
  }

  protected void setInt16Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Short value
  ) {
    state.statement = state.statement.setShort(fieldName, value);
    state.parameters++;
  }

  protected void setInt32Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Integer value
  ) {
    state.statement = state.statement.setInt(fieldName, value);
    state.parameters++;
  }

  protected void setInt64Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Long value
  ) {
    state.statement = state.statement.setLong(fieldName, value);
    state.parameters++;
  }

  protected void setBytesField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      byte[] value
  ) {
    state.statement = state.statement.setByteBuffer(fieldName, ByteBuffer.wrap(value));
    state.parameters++;
  }

  protected void setDecimalField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      BigDecimal value
  ) {
    state.statement = state.statement.setBigDecimal(fieldName, value);
    state.parameters++;
  }

  protected void setBooleanField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Boolean value
  ) {
    state.statement = state.statement.setBool(fieldName, value);
    state.parameters++;
  }

  protected void setStructField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Struct value
  ) {
    throw new UnsupportedOperationException();
  }

  protected void setArray(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Schema schema,
      List value
  ) {
    state.statement = setCqlValue(state.statement, fieldName, value);
    state.parameters++;
  }

  protected void setMap(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Schema schema,
      Map value
  ) {
    state.statement = setCqlValue(state.statement, fieldName, value);
    state.parameters++;
  }

  private BoundStatement setCqlValue(BoundStatement statement, String fieldName, Object value) {
    ColumnDefinition definition = preparedStatement.getVariableDefinitions().get(fieldName);
    if (definition == null) {
      throw new DataException(String.format("No prepared statement variable found for field '%s'", fieldName));
    }
    return (BoundStatement)setCqlValue(statement, fieldName, value, definition.getType());
  }

  private SettableById setCqlValue(
      SettableById target,
      String fieldName,
      Object value,
      DataType dataType
  ) {
    CqlIdentifier identifier = CqlIdentifier.fromInternal(fieldName);
    if (dataType instanceof UserDefinedType) {
      if (!(value instanceof Map)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      return target.setUdtValue(identifier, toUdtValue(fieldName, (Map)value, (UserDefinedType)dataType));
    }
    if (dataType instanceof ListType) {
      if (!(value instanceof List)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      DataType elementType = ((ListType)dataType).getElementType();
      List<Object> converted = new ArrayList<>(((List)value).size());
      for (Object element : (List)value) {
        converted.add(toCqlValue(fieldName, element, elementType));
      }
      return target.setList(identifier, converted, javaType(elementType));
    }
    if (dataType instanceof MapType) {
      if (!(value instanceof Map)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      MapType mapType = (MapType)dataType;
      Map<Object, Object> converted = new LinkedHashMap<>();
      for (Object entryObject : ((Map)value).entrySet()) {
        Map.Entry entry = (Map.Entry)entryObject;
        converted.put(
            toCqlValue(fieldName, entry.getKey(), mapType.getKeyType()),
            toCqlValue(fieldName, entry.getValue(), mapType.getValueType())
        );
      }
      return target.setMap(
          identifier,
          converted,
          javaType(mapType.getKeyType()),
          javaType(mapType.getValueType())
      );
    }
    return target.set(identifier, value, value.getClass());
  }

  private Object toCqlValue(String fieldName, Object value, DataType dataType) {
    if (value == null) {
      return null;
    }
    if (dataType instanceof UserDefinedType) {
      if (!(value instanceof Map)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      return toUdtValue(fieldName, (Map)value, (UserDefinedType)dataType);
    }
    if (dataType instanceof ListType) {
      if (!(value instanceof List)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      DataType elementType = ((ListType)dataType).getElementType();
      List<Object> converted = new ArrayList<>(((List)value).size());
      for (Object element : (List)value) {
        converted.add(toCqlValue(fieldName, element, elementType));
      }
      return converted;
    }
    if (dataType instanceof MapType) {
      if (!(value instanceof Map)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      MapType mapType = (MapType)dataType;
      Map<Object, Object> converted = new LinkedHashMap<>();
      for (Object entryObject : ((Map)value).entrySet()) {
        Map.Entry entry = (Map.Entry)entryObject;
        converted.put(
            toCqlValue(fieldName, entry.getKey(), mapType.getKeyType()),
            toCqlValue(fieldName, entry.getValue(), mapType.getValueType())
        );
      }
      return converted;
    }
    return value;
  }

  private UdtValue toUdtValue(String fieldName, Map value, UserDefinedType userDefinedType) {
    UdtValue udtValue = userDefinedType.newValue();
    for (Object key : value.keySet()) {
      if (!(key instanceof String) || !userDefinedType.contains((String)key)) {
        throw new DataException(
            String.format("Field '%s' contains unknown UDT field '%s' for type %s", fieldName, key, userDefinedType)
        );
      }
      Object fieldValue = value.get(key);
      if (fieldValue == null) {
        udtValue = udtValue.setToNull((String)key);
      } else {
        DataType fieldType = userDefinedType.getFieldTypes().get(userDefinedType.firstIndexOf((String)key));
        udtValue = (UdtValue)setCqlValue(udtValue, (String)key, fieldValue, fieldType);
      }
    }
    return udtValue;
  }

  private Class javaType(DataType dataType) {
    if (dataType instanceof UserDefinedType) {
      return UdtValue.class;
    }
    if (dataType instanceof ListType) {
      return List.class;
    }
    if (dataType instanceof MapType) {
      return Map.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.ASCII)
        || dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.TEXT)) {
      return String.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.BOOLEAN)) {
      return Boolean.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.TINYINT)) {
      return Byte.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.SMALLINT)) {
      return Short.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.INT)) {
      return Integer.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.BIGINT)
        || dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.COUNTER)) {
      return Long.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.FLOAT)) {
      return Float.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.DOUBLE)) {
      return Double.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.DECIMAL)) {
      return BigDecimal.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.VARINT)) {
      return BigInteger.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.BLOB)) {
      return ByteBuffer.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.TIMESTAMP)) {
      return Instant.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.DATE)) {
      return LocalDate.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.TIME)) {
      return LocalTime.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.UUID)
        || dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.TIMEUUID)) {
      return UUID.class;
    }
    if (dataType.equals(com.datastax.oss.driver.api.core.type.DataTypes.INET)) {
      return InetAddress.class;
    }
    return Object.class;
  }

  private DataException incompatibleType(String fieldName, DataType dataType, Object value) {
    return new DataException(
        String.format(
            "Field '%s' expects CQL type %s but received %s",
            fieldName,
            dataType,
            value == null ? "null" : value.getClass().getName()
        )
    );
  }

  protected void setNullField(
      RecordToBoundStatementConverter.State state,
      String fieldName
  ) {
    state.statement = state.statement.setToNull(fieldName);
    state.parameters++;
  }
}
