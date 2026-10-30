package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

/**
 * A field of a record, addressed as <code>key.&lt;path&gt;</code>, <code>value.&lt;path&gt;</code> or
 * <code>header.&lt;name&gt;[.&lt;path&gt;]</code>, where a path may step into nested structs or maps
 * (<code>value.metadata.lut</code>). A header name may contain dots; the longest matching name wins.
 */
final class RecordField {

  enum Source { KEY, VALUE, HEADER }

  /** A resolved field: its value, and its schema when the record has one. */
  static final class Resolved {
    final Object value;
    final Schema schema;

    Resolved(Object value, Schema schema) {
      this.value = value;
      this.schema = schema;
    }
  }

  final String field;
  final Source source;
  private final String[] path;
  private final String label;

  private RecordField(String field, Source source, String[] path, String label) {
    this.field = field;
    this.source = source;
    this.path = path;
    this.label = label;
  }

  /**
   * @param field field reference such as <code>value.metadata.lut</code>
   * @param label what the field is for, used in error messages, e.g. "Timestamp field"
   */
  static RecordField parse(String field, String label) {
    final int dot = field.indexOf('.');
    final String[] path = dot < 0 ? new String[0] : field.substring(dot + 1).split("\\.", -1);
    if (dot < 0 || Arrays.stream(path).anyMatch(String::isEmpty)) {
      throw new ConfigException(String.format(
          "%s '%s' must be 'key.<field>', 'value.<field>' or 'header.<name>'.", label, field));
    }
    final Source source;
    try {
      source = Source.valueOf(field.substring(0, dot).toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new ConfigException(String.format(
          "%s '%s' must start with 'key.', 'value.' or 'header.'.", label, field));
    }
    return new RecordField(field, source, path, label);
  }

  static RecordField ofValueField(String valueField, String label) {
    return new RecordField("value." + valueField, Source.VALUE, new String[] {valueField}, label);
  }

  /**
   * @return the field's value and schema; the value is null when the field is missing or null.
   * @throws DataException when the path steps into something that is not a struct or map.
   */
  Resolved resolve(SinkRecord record) {
    Object current;
    Schema schema;
    int start = 0;
    switch (source) {
      case KEY:
        current = record.key();
        schema = record.keySchema();
        break;
      case VALUE:
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
    return new Resolved(current, schema);
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
        "%s '%s' cannot be read in topic '%s': '%s' is inside a %s, not a struct or map.",
        label, field, record.topic(), name, parent.getClass().getName()));
  }
}
