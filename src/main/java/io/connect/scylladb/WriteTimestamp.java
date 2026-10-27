package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Reads the write timestamp (CQL <code>USING TIMESTAMP</code>) of a record from one of its fields.
 * The field is addressed as <code>key.&lt;path&gt;</code>, <code>value.&lt;path&gt;</code> or
 * <code>header.&lt;name&gt;[.&lt;path&gt;]</code>, where a path may step into nested structs or maps
 * (<code>value.metadata.lut</code>). A header name may contain dots; the longest matching name wins.
 */
public final class WriteTimestamp {

  public static final List<String> UNITS = Collections.unmodifiableList(Arrays.asList(
      TimeUnit.SECONDS.name(), TimeUnit.MILLISECONDS.name(), TimeUnit.MICROSECONDS.name(), TimeUnit.NANOSECONDS.name()
  ));

  private static final String LABEL = "Timestamp field";

  private final RecordField recordField;
  private final String field;
  private final TimeUnit unit;
  private final boolean tombstoneFallback;

  private WriteTimestamp(RecordField recordField, TimeUnit unit, boolean tombstoneFallback) {
    this.recordField = recordField;
    this.field = recordField.field;
    this.unit = unit;
    this.tombstoneFallback = tombstoneFallback;
  }

  /**
   * @param field field reference such as <code>value.metadata.lut</code>; surrounding spaces are
   * ignored, as per-topic settings keep them
   * @param unit one of {@link #UNITS}, case-insensitive
   */
  public static WriteTimestamp of(String field, String unit) {
    return new WriteTimestamp(RecordField.parse(field.trim(), LABEL), parseUnit(unit), false);
  }

  /**
   * The deprecated <code>__timestamp=value.&lt;field&gt;</code> mapping: microseconds, and tombstones
   * fall back to the Kafka record timestamp.
   */
  public static WriteTimestamp legacyMapping(String valueField) {
    return new WriteTimestamp(RecordField.ofValueField(valueField, LABEL), TimeUnit.MICROSECONDS, true);
  }

  public static TimeUnit parseUnit(String unit) {
    final String name = unit.trim().toUpperCase(Locale.ROOT);
    if (!UNITS.contains(name)) {
      throw new ConfigException(String.format(
          "Timestamp unit '%s' must be one of %s.", unit, UNITS));
    }
    return TimeUnit.valueOf(name);
  }

  /**
   * Default write timestamp: the Kafka record timestamp, in microseconds.
   */
  static long kafkaTimestampMicros(SinkRecord record) {
    // Timestamps in Kafka (record.timestamp()) are in millisecond precision,
    // while Scylla expects a microsecond precision: 1 ms = 1000 us.
    return record.timestamp() * 1000;
  }

  /**
   * @return the write timestamp of the record in microseconds.
   * @throws DataException when the field is missing, null or not a timestamp.
   */
  public long micros(SinkRecord record) {
    if (record.value() == null && recordField.source == RecordField.Source.VALUE) {
      if (tombstoneFallback) {
        return kafkaTimestampMicros(record);
      }
      throw new DataException(String.format(
          "Timestamp field '%s' cannot be read from a tombstone in %s: "
              + "a tombstone has no value, so deletes need a 'key.' or 'header.' timestamp field.",
          field, RecordField.where(record)));
    }
    final RecordField.Resolved resolved = recordField.resolve(record);
    if (resolved.value == null) {
      throw new DataException(String.format(
          "Timestamp field '%s' is missing or null in a record of %s.", field, RecordField.where(record)));
    }
    return toMicros(record, resolved.value, resolved.schema);
  }

  private long toMicros(SinkRecord record, Object value, Schema schema) {
    if (value instanceof Date) {
      // Connect's Date and Time logical types are java.util.Date too, but hold no point in time.
      if (schema != null && !Timestamp.LOGICAL_NAME.equals(schema.name())) {
        throw new DataException(String.format(
            "Timestamp field '%s' in %s must be an integer or a %s, but is a %s.",
            field, RecordField.where(record), Timestamp.LOGICAL_NAME, schema.name()));
      }
      return checked(record, ((Date) value).getTime(), TimeUnit.MILLISECONDS);
    }
    final long amount;
    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
      amount = ((Number) value).longValue();
    } else if (value instanceof BigInteger && ((BigInteger) value).bitLength() < Long.SIZE) {
      amount = ((BigInteger) value).longValue();
    } else if (value instanceof String) {
      try {
        amount = Long.parseLong(((String) value).trim());
      } catch (NumberFormatException e) {
        throw new DataException(String.format(
            "Timestamp field '%s' in %s is not an integer: '%s'.", field, RecordField.where(record), value));
      }
    } else {
      throw new DataException(String.format(
          "Timestamp field '%s' in %s must be an integer or a timestamp, but is a %s.",
          field, RecordField.where(record), value.getClass().getName()));
    }
    return checked(record, amount, unit);
  }

  private long checked(SinkRecord record, long amount, TimeUnit unit) {
    final long micros = TimeUnit.MICROSECONDS.convert(amount, unit);
    // TimeUnit saturates instead of overflowing, and a clamped timestamp would outlive every later
    // write. From a coarser unit, Long.MAX_VALUE microseconds can only be a clamp, as it is no multiple
    // of 1000. Long.MIN_VALUE also means "no timestamp" to the driver, which would substitute its own.
    if (micros == Long.MIN_VALUE
        || (micros == Long.MAX_VALUE && unit.compareTo(TimeUnit.MICROSECONDS) > 0)) {
      throw new DataException(String.format(
          "Timestamp field '%s' in %s is out of range in %s: %d.",
          field, RecordField.where(record), unit, amount));
    }
    return micros;
  }
}
