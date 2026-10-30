package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.OptionalInt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RecordExpiryTest {

  static final long NOW_MILLIS = 1_700_000_000_500L;

  static SinkRecord record(Object expiresAt) {
    return new SinkRecord("t", 0, null, null, null, Collections.singletonMap("expiresAt", expiresAt), 7L);
  }

  static OptionalInt ttl(String unit, int offsetSeconds, boolean skipExpired, Object expiresAt) {
    return RecordExpiry.of("value.expiresAt", unit, offsetSeconds, skipExpired).ttlSeconds(record(expiresAt), NOW_MILLIS);
  }

  @Test
  public void shouldReadSecondsAndRoundUp() {
    // 1_700_000_010 s is 9.5 s after NOW_MILLIS.
    assertEquals(OptionalInt.of(10), ttl("SECONDS", 0, true, 1_700_000_010L));
    assertEquals(OptionalInt.of(10), ttl("seconds", 0, true, 1_700_000_010));
  }

  @Test
  public void shouldReadMilliseconds() {
    assertEquals(OptionalInt.of(2), ttl("MILLISECONDS", 0, true, NOW_MILLIS + 1_001L));
  }

  @Test
  public void shouldReadTimestampAsMillisecondsInAnyUnit() {
    Schema schema = SchemaBuilder.struct().field("expiresAt", Timestamp.SCHEMA).build();
    Struct value = new Struct(schema).put("expiresAt", new Date(NOW_MILLIS + 60_000L));
    SinkRecord record = new SinkRecord("t", 0, null, null, schema, value, 7L);

    assertEquals(OptionalInt.of(60),
        RecordExpiry.of("value.expiresAt", "SECONDS", 0, true).ttlSeconds(record, NOW_MILLIS));
  }

  @Test(expected = DataException.class)
  public void shouldRejectConnectDate() {
    Schema schema = SchemaBuilder.struct().field("expiresAt", org.apache.kafka.connect.data.Date.SCHEMA).build();
    Struct value = new Struct(schema).put("expiresAt", new Date(86_400_000L));
    SinkRecord record = new SinkRecord("t", 0, null, null, schema, value, 7L);

    RecordExpiry.of("value.expiresAt", "SECONDS", 0, true).ttlSeconds(record, NOW_MILLIS);
  }

  @Test
  public void shouldReadNestedField() {
    SinkRecord record = new SinkRecord("t", 0, null, null, null,
        Collections.singletonMap("metadata", Collections.singletonMap("expiresAt", 1_700_000_010L)), 7L);

    assertEquals(OptionalInt.of(10),
        RecordExpiry.of("value.metadata.expiresAt", "SECONDS", 0, true).ttlSeconds(record, NOW_MILLIS));
  }

  @Test
  public void shouldAddOffset() {
    assertEquals(OptionalInt.of(3_610), ttl("SECONDS", 3_600, true, 1_700_000_010L));
  }

  @Test
  public void shouldNeverExpireAtZero() {
    assertEquals(OptionalInt.of(0), ttl("SECONDS", 0, true, 0L));
    assertEquals(OptionalInt.of(0), ttl("MILLISECONDS", 3_600, false, 0));
  }

  @Test
  public void shouldSkipExpiredRecord() {
    assertFalse(ttl("SECONDS", 0, true, 1_700_000_000L).isPresent());
    assertFalse(ttl("SECONDS", 0, true, 1L).isPresent());
    assertFalse(ttl("MILLISECONDS", 0, true, NOW_MILLIS).isPresent());
  }

  @Test
  public void shouldFailExpiredRecordWhenNotSkipping() {
    DataException e = assertThrows(DataException.class, () -> ttl("SECONDS", 0, false, 1_700_000_000L));
    assertTrue(e.getMessage(), e.getMessage().contains("expired"));
  }

  @Test
  public void shouldAcceptTtlAtMaximum() {
    assertEquals(OptionalInt.of((int) RecordExpiry.MAX_TTL_SECONDS),
        ttl("SECONDS", 0, true, NOW_MILLIS / 1000 + RecordExpiry.MAX_TTL_SECONDS));
  }

  @Test(expected = DataException.class)
  public void shouldRejectTtlAboveMaximum() {
    ttl("SECONDS", 0, true, NOW_MILLIS / 1000 + RecordExpiry.MAX_TTL_SECONDS + 1);
  }

  @Test(expected = DataException.class)
  public void shouldRejectOverflow() {
    ttl("SECONDS", 0, true, Long.MAX_VALUE);
  }

  @Test(expected = DataException.class)
  public void shouldRejectMissingField() {
    ttl("SECONDS", 0, true, null);
  }

  @Test(expected = DataException.class)
  public void shouldRejectNonIntegerField() {
    ttl("SECONDS", 0, true, "1700000010");
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectUnsupportedUnit() {
    RecordExpiry.of("value.expiresAt", "MICROSECONDS", 0, true);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectNegativeOffset() {
    RecordExpiry.of("value.expiresAt", "SECONDS", -1, true);
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectNonIntegerOffset() {
    RecordExpiry.parseOffsetSeconds("1d");
  }
}
