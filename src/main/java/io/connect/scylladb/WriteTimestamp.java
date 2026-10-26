package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

  private enum Source { KEY, VALUE, HEADER }

  private final String field;
  private final Source source;
  private final String[] path;
  private final TimeUnit unit;
  private final boolean tombstoneFallback;

  private WriteTimestamp(String field, Source source, String[] path, TimeUnit unit, boolean tombstoneFallback) {
    this.field = field;
    this.source = source;
    this.path = path;
    this.unit = unit;
    this.tombstoneFallback = tombstoneFallback;
  }

  /**
   * @param field field reference such as <code>value.metadata.lut</code>
   * @param unit one of {@link #UNITS}, case-insensitive
   */
  public static WriteTimestamp of(String field, String unit) {
    final int dot = field.indexOf('.');
    final String[] path = dot < 0 ? new String[0] : field.substring(dot + 1).split("\\.", -1);
    if (dot < 0 || Arrays.stream(path).anyMatch(String::isEmpty)) {
      throw new ConfigException(String.format(
          "Timestamp field '%s' must be 'key.<field>', 'value.<field>' or 'header.<name>'.", field));
    }
    final Source source;
    try {
      source = Source.valueOf(field.substring(0, dot).toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new ConfigException(String.format(
          "Timestamp field '%s' must start with 'key.', 'value.' or 'header.'.", field));
    }
    return new WriteTimestamp(field, source, path, parseUnit(unit), false);
  }

  /**
   * The deprecated <code>__timestamp=value.&lt;field&gt;</code> mapping: microseconds, and tombstones
   * fall back to the Kafka record timestamp.
   */
  public static WriteTimestamp legacyMapping(String valueField) {
    return new WriteTimestamp("value." + valueField, Source.VALUE, new String[] {valueField},
        TimeUnit.MICROSECONDS, true);
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
    Object current;
    Schema schema;
    int start = 0;
    switch (source) {
      case KEY:
        current = record.key();
        schema = record.keySchema();
        break;
      case VALUE:
        if (record.value() == null && tombstoneFallback) {
          return kafkaTimestampMicros(record);
        }
        current = record.value();
        schema = record.valueSchema();
        break;
      default:
        // Header names may contain dots, so the longest name that matches a header wins.
        Header header = null;
        for (start = path.length; start > 0 && header == null; start--) {
          header = record.headers().lastWithName(String.join(".", Arrays.copyOfRange(path, 0, start)));
        }
        start++;
        current = header == null ? null : header.value();
        schema = header == null ? null : header.schema();
    }
    for (int i = start; i < path.length && current != null; i++) {
      schema = childSchema(current instanceof Struct ? ((Struct) current).schema() : schema, path[i]);
      current = child(record, current, path[i]);
    }
    if (current == null) {
      throw new DataException(String.format(
          "Timestamp field '%s' is missing or null in a record of topic '%s' (partition %d, offset %d).",
          field, record.topic(), record.kafkaPartition(), record.kafkaOffset()));
    }
    return toMicros(record, current, schema);
  }

  private static Schema childSchema(Schema parent, String name) {
    if (parent == null) {
      return null;
    }
    if (parent.type() == Schema.Type.STRUCT) {
      final Field child = parent.field(name);
      return child == null ? null : child.schema();
    }
    return parent.type() == Schema.Type.MAP ? parent.valueSchema() : null;
  }

  private Object child(SinkRecord record, Object parent, String name) {
    if (parent instanceof Struct) {
      final Struct struct = (Struct) parent;
      final Field child = struct.schema().field(name);
      return child == null ? null : struct.get(child);
    }
    if (parent instanceof Map) {
      return ((Map<?, ?>) parent).get(name);
    }
    throw new DataException(String.format(
        "Timestamp field '%s' cannot be read in topic '%s': '%s' is inside a %s, not a struct or map.",
        field, record.topic(), name, parent.getClass().getName()));
  }

  private long toMicros(SinkRecord record, Object value, Schema schema) {
    if (value instanceof Date) {
      // Connect's Date and Time logical types are java.util.Date too, but hold no point in time.
      if (schema != null && !Timestamp.LOGICAL_NAME.equals(schema.name())) {
        throw new DataException(String.format(
            "Timestamp field '%s' in topic '%s' must be an integer or a %s, but is a %s.",
            field, record.topic(), Timestamp.LOGICAL_NAME, schema.name()));
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
            "Timestamp field '%s' in topic '%s' is not an integer: '%s'.", field, record.topic(), value));
      }
    } else {
      throw new DataException(String.format(
          "Timestamp field '%s' in topic '%s' must be an integer or a timestamp, but is a %s.",
          field, record.topic(), value.getClass().getName()));
    }
    return checked(record, amount, unit);
  }

  private long checked(SinkRecord record, long amount, TimeUnit unit) {
    final long micros = TimeUnit.MICROSECONDS.convert(amount, unit);
    // TimeUnit saturates instead of overflowing, and a clamped timestamp would outlive every later
    // write. Long.MIN_VALUE also means "no timestamp" to the driver, which would substitute its own.
    if (micros == Long.MIN_VALUE || (micros == Long.MAX_VALUE && amount != Long.MAX_VALUE)) {
      throw new DataException(String.format(
          "Timestamp field '%s' in topic '%s' is out of range in %s: %d.",
          field, record.topic(), unit, amount));
    }
    return micros;
  }
}
