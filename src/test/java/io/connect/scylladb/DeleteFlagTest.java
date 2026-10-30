package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeleteFlagTest {

  static final Schema METADATA_SCHEMA = SchemaBuilder.struct().name("metadata")
      .field("op", Schema.OPTIONAL_STRING_SCHEMA)
      .build();
  static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("value")
      .field("id", Schema.INT64_SCHEMA)
      .field("metadata", METADATA_SCHEMA)
      .build();

  static SinkRecord record(Object key, Object value) {
    return new SinkRecord("t", 0, null, key, null, value, 7L, 1_000L, null, new ConnectHeaders());
  }

  static Struct struct(String op) {
    return new Struct(VALUE_SCHEMA).put("id", 1L).put("metadata", new Struct(METADATA_SCHEMA).put("op", op));
  }

  static Map<String, Object> map(Object op) {
    Map<String, Object> value = new HashMap<>();
    value.put("id", 1L);
    value.put("metadata", Collections.singletonMap("op", op));
    return value;
  }

  static DeleteFlag flag(String field, String... values) {
    return DeleteFlag.of(field, Arrays.asList(values));
  }

  @Test
  public void shouldMatchNestedStructField() {
    assertTrue(flag("value.metadata.op", "delete").matches(record(null, struct("delete"))));
    assertFalse(flag("value.metadata.op", "delete").matches(record(null, struct("write"))));
  }

  @Test
  public void shouldMatchNestedMapField() {
    assertTrue(flag("value.metadata.op", "delete").matches(record(null, map("delete"))));
    assertFalse(flag("value.metadata.op", "delete").matches(record(null, map("write"))));
  }

  @Test
  public void shouldMatchAnyOfTheValues() {
    DeleteFlag flag = flag("value.metadata.op", "d", " delete ");
    assertTrue(flag.matches(record(null, map("d"))));
    assertTrue(flag.matches(record(null, map("delete"))));
    assertFalse(flag.matches(record(null, map("u"))));
  }

  @Test
  public void shouldCompareNumbersAndBooleansAsStrings() {
    assertTrue(flag("value.metadata.op", "true").matches(record(null, map(true))));
    assertFalse(flag("value.metadata.op", "true").matches(record(null, map(false))));
    assertTrue(flag("value.metadata.op", "3").matches(record(null, map(3L))));
  }

  @Test
  public void shouldMatchKeyField() {
    assertTrue(flag("key.deleted", "true").matches(record(Collections.singletonMap("deleted", true), map("write"))));
  }

  @Test
  public void shouldMatchHeader() {
    SinkRecord record = record(null, map("write"));
    record.headers().addString("op.type", "delete");
    assertTrue(flag("header.op.type", "delete").matches(record));
  }

  @Test
  public void shouldNotMatchMissingOrNullField() {
    DeleteFlag flag = flag("value.metadata.op", "delete");
    assertFalse(flag.matches(record(null, Collections.singletonMap("id", 1L))));
    assertFalse(flag.matches(record(null, map(null))));
    assertFalse(flag.matches(record(null, struct(null))));
    assertFalse(flag.matches(record(null, null)));
  }

  @Test
  public void shouldIgnoreSpacesAroundField() {
    assertTrue(flag(" value.metadata.op ", "delete").matches(record(null, map("delete"))));
  }

  @Test(expected = DataException.class)
  public void shouldRejectStructuredFieldValue() {
    flag("value.metadata", "delete").matches(record(null, map("delete")));
  }

  @Test(expected = ConfigException.class)
  public void shouldRequireAValue() {
    flag("value.metadata.op", " ", "");
  }

  @Test(expected = ConfigException.class)
  public void shouldRejectFieldWithoutSource() {
    flag("op", "delete");
  }
}
