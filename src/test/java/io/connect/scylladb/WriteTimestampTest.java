package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class WriteTimestampTest {

  static final Schema METADATA_SCHEMA = SchemaBuilder.struct().name("metadata")
      .field("lut", Schema.OPTIONAL_INT64_SCHEMA)
      .build();
  static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("value")
      .field("id", Schema.INT64_SCHEMA)
      .field("lut", Schema.OPTIONAL_INT64_SCHEMA)
      .field("updated", Timestamp.builder().optional().build())
      .field("metadata", METADATA_SCHEMA)
      .build();

  static SinkRecord record(Object key, Object value) {
    return new SinkRecord("t", 0, null, key, null, value, 7L, 1_000L, null, new ConnectHeaders());
  }

  static Struct value(Long lut) {
    return new Struct(VALUE_SCHEMA)
        .put("id", 1L)
        .put("lut", lut)
        .put("updated", new Date(1_500L))
        .put("metadata", new Struct(METADATA_SCHEMA).put("lut", 2_000L));
  }

  @Test
  public void shouldConvertEachUnit() {
    SinkRecord record = record(null, value(3L));
    assertEquals(3_000_000L, WriteTimestamp.of("value.lut", "SECONDS").micros(record));
    assertEquals(3_000L, WriteTimestamp.of("value.lut", "milliseconds").micros(record));
    assertEquals(3L, WriteTimestamp.of("value.lut", "MICROSECONDS").micros(record));
    assertEquals(0L, WriteTimestamp.of("value.lut", "NANOSECONDS").micros(record));
  }

  @Test
  public void shouldReadNestedStructField() {
    assertEquals(2_000_000L, WriteTimestamp.of("value.metadata.lut", "MILLISECONDS").micros(record(null, value(1L))));
  }

  @Test
  public void shouldReadTimestampFieldIgnoringUnit() {
    assertEquals(1_500_000L, WriteTimestamp.of("value.updated", "SECONDS").micros(record(null, value(1L))));
  }

  @Test
  public void shouldReadSchemalessMapsAndIntegers() {
    Map<String, Object> metadata = new HashMap<>();
    metadata.put("lut", 5);
    Map<String, Object> value = Collections.singletonMap("metadata", metadata);
    assertEquals(5_000L, WriteTimestamp.of("value.metadata.lut", "MILLISECONDS").micros(record(null, value)));
  }

  @Test
  public void shouldReadKeyOfTombstone() {
    Map<String, Object> key = Collections.singletonMap("lut", 9L);
    assertEquals(9_000L, WriteTimestamp.of("key.lut", "MILLISECONDS").micros(record(key, null)));
  }

  @Test
  public void shouldReadNumericStringHeader() {
    SinkRecord record = record(null, null);
    record.headers().addString("lut", "11");
    assertEquals(11_000L, WriteTimestamp.of("header.lut", "MILLISECONDS").micros(record));
  }

  @Test(expected = DataException.class)
  public void shouldFailOnNullField() {
    WriteTimestamp.of("value.lut", "MILLISECONDS").micros(record(null, value(null)));
  }

  @Test(expected = DataException.class)
  public void shouldFailOnMissingField() {
    WriteTimestamp.of("value.nope", "MILLISECONDS").micros(record(null, value(1L)));
  }

  @Test(expected = DataException.class)
  public void shouldFailOnMissingHeader() {
    WriteTimestamp.of("header.lut", "MILLISECONDS").micros(record(null, value(1L)));
  }

  @Test(expected = DataException.class)
  public void shouldFailOnValueFieldOfTombstone() {
    WriteTimestamp.of("value.lut", "MILLISECONDS").micros(record(Collections.singletonMap("id", 1L), null));
  }

  @Test(expected = DataException.class)
  public void shouldFailOnNonIntegerString() {
    SinkRecord record = record(null, null);
    record.headers().addString("lut", "soon");
    WriteTimestamp.of("header.lut", "MILLISECONDS").micros(record);
  }

  @Test
  public void shouldFallBackToKafkaTimestampForLegacyMappingOnTombstone() {
    assertEquals(1_000_000L, WriteTimestamp.legacyMapping("lut").micros(record(Collections.singletonMap("id", 1L), null)));
    assertEquals(4L, WriteTimestamp.legacyMapping("lut").micros(record(null, value(4L))));
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectUnknownSource() {
    WriteTimestamp.of("body.lut", "MILLISECONDS");
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectMissingPath() {
    WriteTimestamp.of("value", "MILLISECONDS");
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectEmptyPathSegment() {
    WriteTimestamp.of("value.metadata..lut", "MILLISECONDS");
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectUnknownUnit() {
    WriteTimestamp.of("value.lut", "MINUTES");
  }

  @Test(expected = DataException.class)
  public void shouldRejectTimeLogicalType() {
    Schema schema = SchemaBuilder.struct().field("at", Time.SCHEMA).build();
    WriteTimestamp.of("value.at", "MILLISECONDS")
        .micros(record(null, new Struct(schema).put("at", new Date(1_000L))));
  }

  @Test(expected = DataException.class)
  public void shouldRejectDateLogicalType() {
    Schema schema = SchemaBuilder.struct().field("on", org.apache.kafka.connect.data.Date.SCHEMA).build();
    WriteTimestamp.of("value.on", "MILLISECONDS")
        .micros(record(null, new Struct(schema).put("on", new Date(0L))));
  }

  @Test
  public void shouldReadHeaderNameContainingDots() {
    SinkRecord record = record(null, null);
    record.headers().addLong("source.lut", 12L);
    assertEquals(12_000L, WriteTimestamp.of("header.source.lut", "MILLISECONDS").micros(record));
  }

  @Test(expected = DataException.class)
  public void shouldRejectTimestampOutOfRange() {
    WriteTimestamp.of("value.lut", "SECONDS").micros(record(null, value(Long.MAX_VALUE / 1_000L)));
  }

  @Test(expected = DataException.class)
  public void shouldRejectReservedMinimumTimestamp() {
    WriteTimestamp.of("value.lut", "MICROSECONDS").micros(record(null, value(Long.MIN_VALUE)));
  }
}
