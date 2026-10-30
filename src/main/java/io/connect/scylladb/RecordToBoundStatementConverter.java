package io.connect.scylladb;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class RecordToBoundStatementConverter extends RecordConverter<RecordToBoundStatementConverter.State> {
  private static final ObjectMapper JSON = new ObjectMapper();

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
    state.statement = state.statement.setLocalDate(fieldName, localDate(value));
    state.parameters++;
  }

  protected void setTimeField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Date value
  ) {
    state.statement = state.statement.setLocalTime(fieldName, localTime(value));

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

  /**
   * Binds a list or map by the column's CQL type, converting nested values to match it. Schemaless
   * values carry no element types, so the prepared statement's metadata decides them.
   */
  private BoundStatement setCqlValue(BoundStatement statement, String fieldName, Object value) {
    ColumnDefinitions definitions = preparedStatement.getVariableDefinitions();
    int index = definitions.firstIndexOf(fieldName);
    if (index < 0) {
      throw new DataException(String.format("No prepared statement variable found for field '%s'", fieldName));
    }
    DataType dataType = definitions.get(index).getType();
    CodecRegistry registry = statement.codecRegistry();
    return statement.set(index, toCqlValue(fieldName, value, dataType, registry), registry.codecFor(dataType));
  }

  private Object toCqlValue(String fieldName, Object value, DataType dataType, CodecRegistry registry) {
    if (value == null) {
      return null;
    }
    if (dataType instanceof UserDefinedType) {
      if (!(value instanceof Map)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      return toUdtValue(fieldName, (Map<?, ?>) value, (UserDefinedType) dataType, registry);
    }
    if (dataType instanceof ListType || dataType instanceof SetType) {
      if (!(value instanceof List)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      DataType elementType = dataType instanceof ListType
          ? ((ListType) dataType).getElementType()
          : ((SetType) dataType).getElementType();
      Collection<Object> converted = dataType instanceof ListType ? new ArrayList<>() : new LinkedHashSet<>();
      for (Object element : (List<?>) value) {
        converted.add(toCqlValue(fieldName, requireElement(fieldName, element), elementType, registry));
      }
      return converted;
    }
    if (dataType instanceof MapType) {
      if (!(value instanceof Map)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      MapType mapType = (MapType) dataType;
      Map<Object, Object> converted = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        Object key = toCqlValue(fieldName, requireElement(fieldName, entry.getKey()), mapType.getKeyType(), registry);
        if (converted.containsKey(key)) {
          // Distinct JSON keys, such as "1" and "01" for an int key, would otherwise overwrite each other.
          throw new DataException(String.format("Field '%s' has more than one key that converts to %s", fieldName, key));
        }
        converted.put(
            key, toCqlValue(fieldName, requireElement(fieldName, entry.getValue()), mapType.getValueType(), registry));
      }
      return converted;
    }
    if ((value instanceof Map || value instanceof List)
        && (dataType.equals(DataTypes.TEXT) || dataType.equals(DataTypes.ASCII))) {
      // Nested objects, such as geometry, are kept whole as JSON text.
      try {
        return JSON.writeValueAsString(value);
      } catch (JsonProcessingException e) {
        throw new DataException(String.format("Field '%s' cannot be written as JSON text", fieldName), e);
      }
    }
    return toScalar(fieldName, value, dataType, registry);
  }

  /**
   * Converts a scalar to the Java type of the column's default codec, through any registered codec
   * that accepts it (for example a JSON integer, which arrives as a Long, into an int column).
   */
  private Object toScalar(String fieldName, Object value, DataType dataType, CodecRegistry registry) {
    if (value instanceof byte[] && dataType.equals(DataTypes.BLOB)) {
      // Connect BYTES values in records with a schema.
      return ByteBuffer.wrap((byte[]) value);
    }
    if (value instanceof Date) {
      // Connect Timestamp, Date and Time values in records with a schema, as the top-level setters bind them.
      if (dataType.equals(DataTypes.TIMESTAMP)) {
        return ((Date) value).toInstant();
      }
      if (dataType.equals(DataTypes.DATE)) {
        return localDate((Date) value);
      }
      if (dataType.equals(DataTypes.TIME)) {
        return localTime((Date) value);
      }
    }
    TypeCodec<Object> target = registry.codecFor(dataType);
    if (target.accepts(value)) {
      return value;
    }
    TypeCodec<Object> source;
    try {
      source = registry.codecFor(dataType, value);
    } catch (CodecNotFoundException e) {
      throw incompatibleType(fieldName, dataType, value);
    }
    try {
      return target.decode(source.encode(value, ProtocolVersion.DEFAULT), ProtocolVersion.DEFAULT);
    } catch (IllegalArgumentException | DateTimeException e) {
      throw new DataException(
          String.format("Field '%s' value cannot be converted to CQL type %s", fieldName, dataType), e);
    }
  }

  private static LocalDate localDate(Date value) {
    return value.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
  }

  private static LocalTime localTime(Date value) {
    return LocalTime.ofNanoOfDay(TimeUnit.NANOSECONDS.convert(value.getTime(), TimeUnit.MILLISECONDS));
  }

  private static Object requireElement(String fieldName, Object element) {
    if (element == null) {
      throw new DataException(
          String.format("Field '%s' contains a null element; CQL collections cannot hold null", fieldName));
    }
    return element;
  }

  private UdtValue toUdtValue(String fieldName, Map<?, ?> value, UserDefinedType userDefinedType, CodecRegistry registry) {
    UdtValue udtValue = userDefinedType.newValue();
    Set<Integer> bound = new HashSet<>();
    for (Map.Entry<?, ?> entry : value.entrySet()) {
      int index = entry.getKey() instanceof String ? userDefinedType.firstIndexOf((String) entry.getKey()) : -1;
      if (index < 0) {
        throw new DataException(String.format(
            "Field '%s' contains unknown UDT field '%s' for type %s", fieldName, entry.getKey(), userDefinedType));
      }
      if (!bound.add(index)) {
        // UDT fields match case-insensitively, so "system" and "SYSTEM" would otherwise overwrite each other.
        throw new DataException(String.format("Field '%s' has more than one key for UDT field '%s'",
            fieldName, userDefinedType.getFieldNames().get(index).asInternal()));
      }
      DataType fieldType = userDefinedType.getFieldTypes().get(index);
      // Errors inside the UDT name its field, as in "systems_sids.sid".
      String udtFieldName = fieldName + "." + entry.getKey();
      udtValue = udtValue.set(
          index, toCqlValue(udtFieldName, entry.getValue(), fieldType, registry), registry.codecFor(fieldType));
    }
    return udtValue;
  }

  private DataException incompatibleType(String fieldName, DataType dataType, Object value) {
    return new DataException(
        String.format(
            "Field '%s' expects CQL type %s but received %s",
            fieldName,
            dataType,
            value.getClass().getName()
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
