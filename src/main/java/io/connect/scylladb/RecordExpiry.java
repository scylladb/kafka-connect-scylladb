package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

/**
 * Derives the TTL (CQL <code>USING TTL</code>) of a record from the absolute expiry time in one of
 * its value fields, mapped as <code>__expiration=value.&lt;path&gt;</code>, where a path may step
 * into nested structs or maps (<code>value.metadata.exp</code>). An expiry of 0 means the record
 * never expires.
 */
public final class RecordExpiry {

  public static final List<String> UNITS = Collections.unmodifiableList(Arrays.asList(
      TimeUnit.SECONDS.name(), TimeUnit.MILLISECONDS.name()
  ));

  /** The longest TTL ScyllaDB accepts: 20 years. */
  static final long MAX_TTL_SECONDS = 630_720_000L;

  private static final String LABEL = "Expiration field";

  private final RecordField recordField;
  private final String field;
  private final TimeUnit unit;
  private final long offsetMillis;
  private final boolean skipExpired;

  private RecordExpiry(RecordField recordField, TimeUnit unit, long offsetMillis, boolean skipExpired) {
    this.recordField = recordField;
    this.field = recordField.field;
    this.unit = unit;
    this.offsetMillis = offsetMillis;
    this.skipExpired = skipExpired;
  }

  /**
   * @param field field reference such as <code>value.metadata.exp</code>
   * @param unit one of {@link #UNITS}, case-insensitive; a Kafka Connect Timestamp is read as such
   * @param offsetSeconds added to the expiry time, not negative
   * @param skipExpired whether a record that has already expired is skipped rather than failed
   */
  public static RecordExpiry of(String field, String unit, int offsetSeconds, boolean skipExpired) {
    checkOffset(offsetSeconds);
    return new RecordExpiry(RecordField.parse(field, LABEL), parseUnit(unit), offsetSeconds * 1000L, skipExpired);
  }

  public static int parseOffsetSeconds(String offsetSeconds) {
    final int parsed;
    try {
      parsed = Integer.parseInt(offsetSeconds.trim());
    } catch (NumberFormatException e) {
      throw new ConfigException("The setting expirationOffsetSeconds must be an integer.");
    }
    checkOffset(parsed);
    return parsed;
  }

  private static void checkOffset(int offsetSeconds) {
    if (offsetSeconds < 0) {
      throw new ConfigException("The setting expirationOffsetSeconds must not be negative.");
    }
  }

  public static TimeUnit parseUnit(String unit) {
    final String name = unit.trim().toUpperCase(Locale.ROOT);
    if (!UNITS.contains(name)) {
      throw new ConfigException(String.format(
          "Expiration unit '%s' must be one of %s.", unit, UNITS));
    }
    return TimeUnit.valueOf(name);
  }

  /**
   * @param nowMillis current time in Unix epoch milliseconds
   * @return the TTL in seconds to write the record with, 0 when it never expires; empty when the
   * record has already expired and is to be skipped.
   * @throws DataException when the field is missing, null or not a time, when the TTL is out of
   * range, or when the record has already expired and is not to be skipped.
   */
  public OptionalInt ttlSeconds(SinkRecord record, long nowMillis) {
    final RecordField.Resolved resolved = recordField.resolve(record);
    final Object value = resolved.value;
    final long amount;
    final TimeUnit amountUnit;
    if (value instanceof Date) {
      // Connect's Date and Time logical types are java.util.Date too, but hold no point in time.
      if (resolved.schema != null && !Timestamp.LOGICAL_NAME.equals(resolved.schema.name())) {
        throw new DataException(String.format(
            "Expiration field '%s' in topic '%s' must be an integer or a %s, but is a %s.",
            field, record.topic(), Timestamp.LOGICAL_NAME, resolved.schema.name()));
      }
      amount = ((Date) value).getTime();
      amountUnit = TimeUnit.MILLISECONDS;
    } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
      amount = ((Number) value).longValue();
      amountUnit = unit;
    } else {
      throw new DataException(String.format(
          "Expiration field '%s' in a record of topic '%s' (partition %d, offset %d) must be an integer"
              + " or a timestamp, but is %s.",
          field, record.topic(), record.kafkaPartition(), record.kafkaOffset(),
          value == null ? "missing or null" : "a " + value.getClass().getName()));
    }
    if (amount == 0) {
      // TTL 0 writes without one, also overriding the table's default_time_to_live.
      return OptionalInt.of(0);
    }
    final long remainingMillis;
    try {
      final long expiresAtMillis = Math.addExact(
          Math.multiplyExact(amount, TimeUnit.MILLISECONDS.convert(1, amountUnit)), offsetMillis);
      remainingMillis = Math.subtractExact(expiresAtMillis, nowMillis);
    } catch (ArithmeticException e) {
      throw new DataException(String.format(
          "Expiration field '%s' in topic '%s' is out of range in %s: %d.",
          field, record.topic(), amountUnit, amount), e);
    }
    if (remainingMillis <= 0) {
      if (skipExpired) {
        return OptionalInt.empty();
      }
      throw new DataException(String.format(
          "Record of topic '%s' (partition %d, offset %d) expired %d ms ago, at %d %s plus %d s.",
          record.topic(), record.kafkaPartition(), record.kafkaOffset(), -remainingMillis,
          amount, amountUnit, offsetMillis / 1000));
    }
    // Round up, so a record never expires before its time.
    final long ttlSeconds = (remainingMillis - 1) / 1000 + 1;
    if (ttlSeconds > MAX_TTL_SECONDS) {
      throw new DataException(String.format(
          "Expiration field '%s' in topic '%s' gives a TTL of %d s, more than the maximum of %d s.",
          field, record.topic(), ttlSeconds, MAX_TTL_SECONDS));
    }
    return OptionalInt.of((int) ttlSeconds);
  }
}
