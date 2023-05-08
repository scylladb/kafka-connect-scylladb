package io.connect.scylladb;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
    state.statement = state.statement.setList(fieldName, value, Object.class);
    state.parameters++;
  }

  protected void setMap(
      RecordToBoundStatementConverter.State state,
      String fieldName,
      Schema schema,
      Map value
  ) {
    state.statement = state.statement.setMap(fieldName, value, Object.class, Object.class);
    state.parameters++;
  }

  protected void setNullField(
      RecordToBoundStatementConverter.State state,
      String fieldName
  ) {
    state.statement = state.statement.setToNull(fieldName);
    state.parameters++;
  }
}
