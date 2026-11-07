package io.connect.scylladb;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
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
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class RecordToBoundStatementConverter extends RecordConverter<RecordToBoundStatementConverter.State> {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);
  // The CQL type each Connect logical type that arrives as a java.util.Date binds to.
  private static final Map<String, DataType> TEMPORAL_TYPES = new HashMap<>();

  static {
    TEMPORAL_TYPES.put(Timestamp.LOGICAL_NAME, DataTypes.TIMESTAMP);
    TEMPORAL_TYPES.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, DataTypes.DATE);
    TEMPORAL_TYPES.put(Time.LOGICAL_NAME, DataTypes.TIME);
  }

  private final PreparedStatement preparedStatement;

  static class State {

    public BoundStatement statement;
    public int parameters = 0;

    State(BoundStatement statement) {
      this.statement = statement;
    }

    /**
     * @return the variables of the prepared statement that are still unbound, in CQL form. A delete
     * has one for each primary key column, and must bind them all.
     */
    List<String> unboundVariables() {
      final ColumnDefinitions variables = statement.getPreparedStatement().getVariableDefinitions();
      final List<String> unbound = new ArrayList<>();
      for (int i = 0; i < variables.size(); i++) {
        if (!statement.isSet(i)) {
          unbound.add(variables.get(i).getName().asCql(true));
        }
      }
      return unbound;
    }
  }

  RecordToBoundStatementConverter(PreparedStatement preparedStatement) {
    this.preparedStatement = preparedStatement;
  }

  @Override
  protected boolean isPrimaryKeyColumn(String column) {
    // A delete is prepared with a marker for each primary key column and nothing else.
    return preparedStatement.getVariableDefinitions().contains(column);
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
    state.statement = state.statement.setLocalDate(fieldName, cqlDate(fieldName, localDate(value)));
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
    state.statement = setCqlValue(state.statement, fieldName, schema, value);
    state.parameters++;
  }

  protected void setMap(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Schema schema,
      Map value
  ) {
    state.statement = setCqlValue(state.statement, fieldName, schema, value);
    state.parameters++;
  }

  /**
   * Binds a list or map by the column's CQL type, converting nested values to match it. Schemaless
   * values carry no element types, so the prepared statement's metadata decides them; a schema, when
   * there is one, also names the Connect logical type of its dates.
   */
  private BoundStatement setCqlValue(BoundStatement statement, String fieldName, Schema schema, Object value) {
    ColumnDefinitions definitions = preparedStatement.getVariableDefinitions();
    int index = definitions.firstIndexOf(fieldName);
    if (index < 0) {
      throw new DataException(String.format("No prepared statement variable found for field '%s'", fieldName));
    }
    DataType dataType = definitions.get(index).getType();
    CodecRegistry registry = statement.codecRegistry();
    return statement.set(index, toCqlValue(fieldName, value, schema, dataType, registry), registry.codecFor(dataType));
  }

  private Object toCqlValue(String fieldName, Object value, Schema schema, DataType dataType, CodecRegistry registry) {
    if (value == null) {
      return null;
    }
    if (dataType instanceof UserDefinedType) {
      if (!(value instanceof Map)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      return toUdtValue(fieldName, (Map<?, ?>) value, valueSchema(schema), (UserDefinedType) dataType, registry);
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
        converted.add(toCqlElement(fieldName, element, valueSchema(schema), elementType, registry));
      }
      return converted;
    }
    if (dataType instanceof MapType) {
      if (!(value instanceof Map)) {
        throw incompatibleType(fieldName, dataType, value);
      }
      MapType mapType = (MapType) dataType;
      Map<Object, Object> converted = new LinkedHashMap<>();
      Set<Object> distinctKeys = new HashSet<>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
        Object key = toCqlElement(fieldName, entry.getKey(), keySchema(schema), mapType.getKeyType(), registry);
        if (!distinctKeys.add(distinctKey(key))) {
          // Distinct JSON keys, such as "1" and "01" for an int key, would otherwise overwrite each other.
          throw new DataException(String.format("Field '%s' has more than one key that converts to %s", fieldName, key));
        }
        converted.put(
            key, toCqlElement(fieldName, entry.getValue(), valueSchema(schema), mapType.getValueType(), registry));
      }
      return converted;
    }
    if ((value instanceof Map || value instanceof List) && dataType.equals(DataTypes.TEXT)) {
      // Nested objects, such as geometry, are kept whole as JSON text.
      try {
        return JSON.writeValueAsString(value);
      } catch (JsonProcessingException e) {
        throw new DataException(String.format("Field '%s' cannot be written as JSON text", fieldName), e);
      }
    }
    if (value instanceof Map || value instanceof List) {
      throw incompatibleType(fieldName, dataType, value);
    }
    return toScalar(fieldName, value, schema, dataType, registry);
  }

  /** Converts a collection element or map key, which CQL collections cannot hold as null. */
  private Object toCqlElement(String fieldName, Object element, Schema schema, DataType dataType, CodecRegistry registry) {
    // Some strings also convert to null, such as "" or "NULL" for a uuid.
    return requireElement(fieldName, toCqlValue(fieldName, requireElement(fieldName, element), schema, dataType, registry));
  }

  private static Schema keySchema(Schema schema) {
    return schema != null && schema.type() == Schema.Type.MAP ? schema.keySchema() : null;
  }

  private static Schema valueSchema(Schema schema) {
    return schema != null && (schema.type() == Schema.Type.ARRAY || schema.type() == Schema.Type.MAP)
        ? schema.valueSchema()
        : null;
  }

  /**
   * Converts a scalar to the Java type of the column's default codec, through any registered codec
   * that accepts it (for example a JSON integer, which arrives as a Long, into an int column).
   */
  private Object toScalar(String fieldName, Object value, Schema schema, DataType dataType, CodecRegistry registry) {
    if (value instanceof byte[] && dataType.equals(DataTypes.BLOB)) {
      // Connect BYTES values in records with a schema.
      return ByteBuffer.wrap((byte[]) value);
    }
    if (value instanceof Date) {
      // Connect Timestamp, Date and Time values in records with a schema, which names their type.
      DataType temporalType = schema == null ? null : TEMPORAL_TYPES.get(schema.name());
      if (temporalType != null && !temporalType.equals(dataType)) {
        throw new DataException(String.format("Field '%s' value of logical type %s cannot be converted to CQL type %s",
            fieldName, schema.name(), dataType));
      }
      try {
        if (dataType.equals(DataTypes.TIMESTAMP)) {
          return ((Date) value).toInstant();
        }
        if (dataType.equals(DataTypes.DATE)) {
          return cqlDate(fieldName, localDate((Date) value));
        }
        if (dataType.equals(DataTypes.TIME)) {
          return localTime((Date) value);
        }
      } catch (DateTimeException e) {
        throw cannotConvert(fieldName, dataType, e);
      }
    }
    TypeCodec<Object> target = registry.codecFor(dataType);
    if (target.accepts(value)) {
      return value instanceof LocalDate ? cqlDate(fieldName, (LocalDate) value) : value;
    }
    TypeCodec<Object> source;
    try {
      source = registry.codecFor(dataType, value);
    } catch (CodecNotFoundException e) {
      throw incompatibleType(fieldName, dataType, value);
    }
    try {
      return target.decode(source.encode(value, ProtocolVersion.DEFAULT), ProtocolVersion.DEFAULT);
    } catch (IllegalArgumentException | DateTimeException | ArithmeticException e) {
      // A timestamp past the range of epoch milliseconds overflows when encoded.
      throw cannotConvert(fieldName, dataType, e);
    }
  }

  // ScyllaDB compares decimals by value, so 1.0 and 1.00 are the same key, and the driver writes
  // timestamps in whole milliseconds, so keys that differ below that are too. This also holds inside
  // a frozen collection, tuple or UDT key.
  private static Object distinctKey(Object key) {
    if (key instanceof BigDecimal) {
      return ((BigDecimal) key).stripTrailingZeros();
    }
    if (key instanceof Instant) {
      return ((Instant) key).truncatedTo(ChronoUnit.MILLIS);
    }
    if (key instanceof Collection) {
      Collection<Object> normalized = key instanceof Set ? new HashSet<>() : new ArrayList<>();
      for (Object element : (Collection<?>) key) {
        normalized.add(distinctKey(element));
      }
      return normalized;
    }
    if (key instanceof Map) {
      Map<Object, Object> normalized = new HashMap<>();
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) key).entrySet()) {
        normalized.put(distinctKey(entry.getKey()), distinctKey(entry.getValue()));
      }
      return normalized;
    }
    if (key instanceof GettableByIndex) {
      GettableByIndex fields = (GettableByIndex) key;
      List<Object> normalized = new ArrayList<>();
      for (int i = 0; i < fields.size(); i++) {
        normalized.add(distinctKey(fields.getObject(i)));
      }
      return normalized;
    }
    return key;
  }

  // The driver writes a date as a 32-bit day count, which wraps instead of failing outside that range.
  private static LocalDate cqlDate(String fieldName, LocalDate value) {
    long epochDay = value.toEpochDay();
    if (epochDay < Integer.MIN_VALUE || epochDay > Integer.MAX_VALUE) {
      throw new DataException(
          String.format("Field '%s' value %s is outside the range of CQL type %s", fieldName, value, DataTypes.DATE));
    }
    return value;
  }

  private static DataException cannotConvert(String fieldName, DataType dataType, Exception cause) {
    return new DataException(
        String.format("Field '%s' value cannot be converted to CQL type %s", fieldName, dataType), cause);
  }

  // A Connect Date is midnight UTC, so its epoch day is its date in any JVM time zone.
  private static LocalDate localDate(Date value) {
    return LocalDate.ofEpochDay(Math.floorDiv(value.getTime(), MILLIS_PER_DAY));
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

  private UdtValue toUdtValue(
      String fieldName, Map<?, ?> value, Schema schema, UserDefinedType userDefinedType, CodecRegistry registry) {
    UdtValue udtValue = userDefinedType.newValue();
    Set<Integer> bound = new HashSet<>();
    for (Map.Entry<?, ?> entry : value.entrySet()) {
      int index = -1;
      if (entry.getKey() instanceof String) {
        // The exact name first, so quoted fields that differ only in case, such as "Sys" and "sys", both bind.
        index = userDefinedType.firstIndexOf(CqlIdentifier.fromInternal((String) entry.getKey()));
        if (index < 0) {
          index = userDefinedType.firstIndexOf((String) entry.getKey());
        }
      }
      if (index < 0) {
        throw new DataException(String.format(
            "Field '%s' contains unknown UDT field '%s' for type %s", fieldName, entry.getKey(), userDefinedType));
      }
      if (!bound.add(index)) {
        // Other keys match case-insensitively, so "system" and "SYSTEM" would otherwise overwrite each other.
        throw new DataException(String.format("Field '%s' has more than one key for UDT field '%s'",
            fieldName, userDefinedType.getFieldNames().get(index).asInternal()));
      }
      DataType fieldType = userDefinedType.getFieldTypes().get(index);
      // Errors inside the UDT name its field, as in "systems_sids.sid".
      String udtFieldName = fieldName + "." + entry.getKey();
      Object fieldValue = toCqlValue(udtFieldName, entry.getValue(), schema, fieldType, registry);
      try {
        udtValue = udtValue.set(index, fieldValue, registry.codecFor(fieldType));
      } catch (IllegalArgumentException | ArithmeticException e) {
        // The codec encodes on set, and can still reject the value, such as non-ASCII text in an ascii field
        // or a timestamp past the range of epoch milliseconds.
        throw cannotConvert(udtFieldName, fieldType, e);
      }
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
