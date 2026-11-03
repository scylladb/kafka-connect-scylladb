package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.OptionalInt;

import static io.connect.scylladb.RecordExpiry.OnExpired.DELETE;
import static io.connect.scylladb.RecordExpiry.OnExpired.FAIL;
import static io.connect.scylladb.RecordExpiry.OnExpired.SKIP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RecordExpiryTest {

  static final long NOW_MILLIS = 1_700_000_000_500L;

  static SinkRecord record(Object expiresAt) {
    return new SinkRecord("t", 0, null, null, null, Collections.singletonMap("expiresAt", expiresAt), 7L);
  }

  static OptionalInt ttl(String unit, int offsetSeconds, RecordExpiry.OnExpired behaviorOnExpired, Object expiresAt) {
    return RecordExpiry.of("value.expiresAt", unit, offsetSeconds, behaviorOnExpired).ttlSeconds(record(expiresAt), NOW_MILLIS);
  }

  @Test
  public void shouldReadSecondsAndRoundUp() {
    // 1_700_000_010 s is 9.5 s after NOW_MILLIS.
    assertEquals(OptionalInt.of(10), ttl("SECONDS", 0, DELETE, 1_700_000_010L));
    assertEquals(OptionalInt.of(10), ttl("seconds", 0, DELETE, 1_700_000_010));
  }

  @Test
  public void shouldReadMilliseconds() {
    assertEquals(OptionalInt.of(2), ttl("MILLISECONDS", 0, DELETE, NOW_MILLIS + 1_001L));
  }

  @Test
  public void shouldCountTtlFromTheCurrentWholeSecond() {
    // The expiry is 900 ms away, at 1_700_000_001.4 s. ScyllaDB adds the TTL to 1_700_000_000 s, so
    // TTL 1 would expire 400 ms early.
    assertEquals(OptionalInt.of(2), ttl("MILLISECONDS", 0, DELETE, NOW_MILLIS + 900L));
    assertEquals(OptionalInt.of(1), ttl("MILLISECONDS", 0, DELETE, NOW_MILLIS + 500L));
  }

  @Test
  public void shouldReadTimestampAsMillisecondsInAnyUnit() {
    Schema schema = SchemaBuilder.struct().field("expiresAt", Timestamp.SCHEMA).build();
    Struct value = new Struct(schema).put("expiresAt", new Date(NOW_MILLIS + 60_000L));
    SinkRecord record = new SinkRecord("t", 0, null, null, schema, value, 7L);

    // 60.5 s from the current whole second.
    assertEquals(OptionalInt.of(61),
        RecordExpiry.of("value.expiresAt", "SECONDS", 0, DELETE).ttlSeconds(record, NOW_MILLIS));
  }

  @Test(expected = DataException.class)
  public void shouldRejectConnectDate() {
    Schema schema = SchemaBuilder.struct().field("expiresAt", org.apache.kafka.connect.data.Date.SCHEMA).build();
    Struct value = new Struct(schema).put("expiresAt", new Date(86_400_000L));
    SinkRecord record = new SinkRecord("t", 0, null, null, schema, value, 7L);

    RecordExpiry.of("value.expiresAt", "SECONDS", 0, DELETE).ttlSeconds(record, NOW_MILLIS);
  }

  @Test
  public void shouldReadNestedField() {
    SinkRecord record = new SinkRecord("t", 0, null, null, null,
        Collections.singletonMap("metadata", Collections.singletonMap("expiresAt", 1_700_000_010L)), 7L);

    assertEquals(OptionalInt.of(10),
        RecordExpiry.of("value.metadata.expiresAt", "SECONDS", 0, DELETE).ttlSeconds(record, NOW_MILLIS));
  }

  @Test
  public void shouldAddOffset() {
    assertEquals(OptionalInt.of(3_610), ttl("SECONDS", 3_600, DELETE, 1_700_000_010L));
  }

  @Test
  public void shouldNeverExpireAtZero() {
    assertEquals(OptionalInt.of(0), ttl("SECONDS", 0, DELETE, 0L));
    assertEquals(OptionalInt.of(0), ttl("MILLISECONDS", 3_600, FAIL, 0));
  }

  @Test
  public void shouldNotWriteExpiredRecordToBeDeletedOrSkipped() {
    for (RecordExpiry.OnExpired behavior : new RecordExpiry.OnExpired[] {DELETE, SKIP}) {
      assertFalse(ttl("SECONDS", 0, behavior, 1_700_000_000L).isPresent());
      assertFalse(ttl("SECONDS", 0, behavior, 1L).isPresent());
      assertFalse(ttl("MILLISECONDS", 0, behavior, NOW_MILLIS).isPresent());
    }
  }

  @Test
  public void shouldFailExpiredRecordWhenSetToFail() {
    DataException e = assertThrows(DataException.class, () -> ttl("SECONDS", 0, FAIL, 1_700_000_000L));
    assertTrue(e.getMessage(), e.getMessage().contains("expired"));
  }

  @Test
  public void shouldAcceptTtlAtMaximum() {
    assertEquals(OptionalInt.of((int) RecordExpiry.MAX_TTL_SECONDS),
        ttl("SECONDS", 0, DELETE, NOW_MILLIS / 1000 + RecordExpiry.MAX_TTL_SECONDS));
  }

  @Test(expected = DataException.class)
  public void shouldRejectTtlAboveMaximum() {
    ttl("SECONDS", 0, DELETE, NOW_MILLIS / 1000 + RecordExpiry.MAX_TTL_SECONDS + 1);
  }

  @Test(expected = DataException.class)
  public void shouldRejectOverflow() {
    ttl("SECONDS", 0, DELETE, Long.MAX_VALUE);
  }

  @Test(expected = DataException.class)
  public void shouldRejectMissingField() {
    ttl("SECONDS", 0, DELETE, null);
  }

  @Test
  public void shouldRejectUnsetFieldRatherThanUseItsDefault() {
    Schema schema = SchemaBuilder.struct()
        .field("expiresAt", SchemaBuilder.int64().optional().defaultValue(1_700_000_010L).build())
        .build();
    SinkRecord record = new SinkRecord("t", 0, null, null, schema, new Struct(schema), 7L);

    DataException e = assertThrows(DataException.class,
        () -> RecordExpiry.of("value.expiresAt", "SECONDS", 0, DELETE).ttlSeconds(record, NOW_MILLIS));
    assertTrue(e.getMessage(), e.getMessage().contains("missing or null"));
  }

  @Test
  public void shouldReadIntegerStringsAndBigIntegers() {
    // The same types as a timestamp field.
    assertEquals(OptionalInt.of(10), ttl("SECONDS", 0, DELETE, " 1700000010 "));
    assertEquals(OptionalInt.of(10), ttl("SECONDS", 0, DELETE, BigInteger.valueOf(1_700_000_010L)));
  }

  @Test
  public void shouldRejectNonIntegerField() {
    DataException text = assertThrows(DataException.class, () -> ttl("SECONDS", 0, DELETE, "soon"));
    assertTrue(text.getMessage(), text.getMessage().contains("is not an integer"));
    DataException decimal = assertThrows(DataException.class, () -> ttl("SECONDS", 0, DELETE, 1.7e9));
    assertTrue(decimal.getMessage(), decimal.getMessage().contains("must be an integer or a timestamp"));
  }

  @Test
  public void shouldRejectNegativeExpiryRatherThanSkipIt() {
    DataException integer = assertThrows(DataException.class, () -> ttl("SECONDS", 0, DELETE, -1L));
    assertTrue(integer.getMessage(), integer.getMessage().contains("negative"));
    Schema schema = SchemaBuilder.struct().field("expiresAt", Timestamp.SCHEMA).build();
    SinkRecord record = new SinkRecord("t", 0, null, null, schema,
        new Struct(schema).put("expiresAt", new Date(-1_000L)), 7L);
    assertThrows(DataException.class,
        () -> RecordExpiry.of("value.expiresAt", "SECONDS", 3_600, DELETE).ttlSeconds(record, NOW_MILLIS));
  }

  @Test
  public void shouldNameTheRecordInEveryError() {
    for (Object expiresAt : new Object[] {null, -1L, Long.MAX_VALUE,
        NOW_MILLIS / 1000 + RecordExpiry.MAX_TTL_SECONDS + 1, 1_700_000_000L}) {
      DataException e = assertThrows(DataException.class, () -> ttl("SECONDS", 0, FAIL, expiresAt));
      assertTrue(e.getMessage(), e.getMessage().contains("topic 't' (partition 0, offset 7)"));
    }
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectUnsupportedUnit() {
    RecordExpiry.of("value.expiresAt", "MICROSECONDS", 0, DELETE);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectNegativeOffset() {
    RecordExpiry.of("value.expiresAt", "SECONDS", -1, DELETE);
  }

  @Test
  public void shouldParseBehaviorOnExpiredIgnoringCaseAndSpaces() {
    assertEquals(DELETE, RecordExpiry.parseBehaviorOnExpired("delete"));
    assertEquals(SKIP, RecordExpiry.parseBehaviorOnExpired(" Skip "));
    assertEquals(FAIL, RecordExpiry.parseBehaviorOnExpired("FAIL"));
  }

  @Test
  public void shouldRejectUnknownBehaviorOnExpired() {
    ConfigException e = assertThrows(ConfigException.class, () -> RecordExpiry.parseBehaviorOnExpired("yes"));
    assertTrue(e.getMessage(), e.getMessage().contains("[DELETE, SKIP, FAIL]"));
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectNonIntegerOffset() {
    RecordExpiry.parseOffsetSeconds("1d");
  }
}
