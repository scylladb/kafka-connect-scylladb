package io.connect.scylladb;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.schemabuilder.UDTType;
import io.connect.scylladb.codec.ListTupleCodec;
import io.connect.scylladb.codec.MapUDTCodec;
import io.connect.scylladb.codec.StructTupleCodec;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class RecordToBoundStatementConverter extends RecordConverter<RecordToBoundStatementConverter.State> {
  private final PreparedStatement preparedStatement;

  static class State {

    public final BoundStatement statement;
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
    state.statement.setString(fieldName, value);
    state.parameters++;
  }

  protected void setFloat32Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Float value
  ) {
    state.statement.setFloat(fieldName, value);
    state.parameters++;
  }

  protected void setFloat64Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Double value
  ) {
    state.statement.setDouble(fieldName, value);
    state.parameters++;
  }

  protected void setTimestampField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Date value
  ) {
    state.statement.setTimestamp(fieldName, value);
    state.parameters++;
  }

  protected void setDateField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Date value
  ) {
    state.statement.setDate(fieldName, LocalDate.fromMillisSinceEpoch(value.getTime()));
    state.parameters++;
  }

  protected void setTimeField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Date value
  ) {
    final long nanoseconds = TimeUnit.MILLISECONDS.convert(value.getTime(), TimeUnit.NANOSECONDS);
    state.statement.setTime(fieldName, nanoseconds);
    state.parameters++;
  }

  protected void setInt8Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Byte value
  ) {
    state.statement.setByte(fieldName, value);
    state.parameters++;
  }

  protected void setInt16Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Short value
  ) {
    state.statement.setShort(fieldName, value);
    state.parameters++;
  }

  protected void setInt32Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Integer value
  ) {
    state.statement.setInt(fieldName, value);
    state.parameters++;
  }

  protected void setInt64Field(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Long value
  ) {
    state.statement.setLong(fieldName, value);
    state.parameters++;
  }

  protected void setBytesField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      byte[] value
  ) {
    state.statement.setBytes(fieldName, ByteBuffer.wrap(value));
    state.parameters++;
  }

  protected void setDecimalField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      BigDecimal value
  ) {
    state.statement.setDecimal(fieldName, value);
    state.parameters++;
  }

  protected void setBooleanField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Boolean value
  ) {
    state.statement.setBool(fieldName, value);
    state.parameters++;
  }

  protected void setStructField(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Struct value
  ) {
    DataType colType = preparedStatement.getVariables().getType(fieldName);
    switch (colType.getName()){
      case TUPLE:
        TypeCodec<Struct> codec;
        try{
          codec = preparedStatement.getCodecRegistry().codecFor(colType, Struct.class);
        } catch (CodecNotFoundException e) {
          preparedStatement.getCodecRegistry().register(new StructTupleCodec(preparedStatement.getCodecRegistry(), (TupleType) colType));
          codec = preparedStatement.getCodecRegistry().codecFor(colType, Struct.class);
        }
        state.statement.set(fieldName, value, codec);
        break;
      case UDT:
        state.statement.set(fieldName, value, preparedStatement.getCodecRegistry().codecFor(preparedStatement.getVariables().getType(fieldName), value));
        break;
      default:
        throw new UnsupportedOperationException("No behavior implemented for inserting Kafka Struct into " + colType.getName());
    }
    state.parameters++;
  }

  protected void setArray(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Schema schema,
      List value
  ) {
    DataType colType = preparedStatement.getVariables().getType(fieldName);
    switch (colType.getName()){
      case TUPLE:
        TypeCodec<List> codec;
        try{
          codec = preparedStatement.getCodecRegistry().codecFor(colType, List.class);
        } catch (CodecNotFoundException e) {
          preparedStatement.getCodecRegistry().register(new ListTupleCodec(preparedStatement.getCodecRegistry(), (TupleType) colType));
          codec = preparedStatement.getCodecRegistry().codecFor(colType, List.class);
        }
        state.statement.set(fieldName, value, codec);
        break;
      default:
        state.statement.setList(fieldName, value);
    }
    state.parameters++;
  }

  protected void setMap(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Schema schema,
      Map value
  ) {
    DataType colType = preparedStatement.getVariables().getType(fieldName);
    switch (colType.getName()){
      case UDT:
        TypeCodec<Map> codec;
        try{
          codec = preparedStatement.getCodecRegistry().codecFor(colType, Map.class);
        } catch (CodecNotFoundException e) {
          preparedStatement.getCodecRegistry().register(new MapUDTCodec(preparedStatement.getCodecRegistry(), (UserType) colType));
          codec = preparedStatement.getCodecRegistry().codecFor(colType, Map.class);
        }
        state.statement.set(fieldName, value, codec);
        break;
      case MAP:
        state.statement.setMap(fieldName, value);
        break;
      default:
        throw new UnsupportedOperationException("No behavior implemented for inserting Java Map into " + colType.getName());
    }
    state.parameters++;
  }

  protected void setNullField(
      RecordToBoundStatementConverter.State state,
      String fieldName
  ) {
    state.statement.setToNull(fieldName);
    state.parameters++;
  }
}
