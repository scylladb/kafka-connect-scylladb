package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Marks records as deletes by the value of one of their fields, for sources that send a delete as
 * a record with a payload instead of a tombstone. The field is addressed like the write timestamp
 * field, e.g. <code>value.metadata.op</code>.
 */
public final class DeleteFlag {

  private static final Logger log = LoggerFactory.getLogger(DeleteFlag.class);

  static final String LABEL = "Delete field";

  private final RecordField recordField;
  private final Set<String> values;
  // Topics already warned about a missing or null field, so each is logged once per task.
  private final Set<String> warnedTopics = ConcurrentHashMap.newKeySet();

  private DeleteFlag(RecordField recordField, Set<String> values) {
    this.recordField = recordField;
    this.values = values;
  }

  /**
   * @param field field reference such as <code>value.metadata.op</code>; surrounding spaces are
   * ignored, as per-topic settings keep them
   * @param values field values that mark a delete, compared as strings
   */
  public static DeleteFlag of(String field, Collection<String> values) {
    final RecordField recordField = RecordField.parse(field.trim(), LABEL);
    final Set<String> trimmed = new LinkedHashSet<>();
    for (String value : values) {
      if (!value.trim().isEmpty()) {
        trimmed.add(value.trim());
      }
    }
    if (trimmed.isEmpty()) {
      throw new ConfigException(String.format(
          "%s '%s' needs at least one value that marks a delete.", LABEL, recordField.field));
    }
    return new DeleteFlag(recordField, Collections.unmodifiableSet(trimmed));
  }

  /**
   * @return true when the field holds one of the delete values; false when it holds another value,
   * or is missing or null.
   * @throws DataException when the field holds anything but a string, number or boolean (a struct,
   * map, list, bytes or date), or when its path steps into something that is not a struct or map.
   */
  public boolean matches(SinkRecord record) {
    final Object value = recordField.resolve(record).value;
    if (value == null) {
      if (warnedTopics.add(record.topic())) {
        log.warn("{} '{}' is missing or null in topic '{}' at offset {}, so the record is written, not deleted. "
            + "Logged once per topic.", LABEL, recordField.field, record.topic(), record.kafkaOffset());
      } else {
        log.trace("matches() - {} '{}' is missing or null in topic '{}' at offset {}; not a delete.",
            LABEL, recordField.field, record.topic(), record.kafkaOffset());
      }
      return false;
    }
    if (!(value instanceof CharSequence || value instanceof Number || value instanceof Boolean)) {
      throw new DataException(String.format(
          "%s '%s' in topic '%s' must be a string, number or boolean, but is a %s.",
          LABEL, recordField.field, record.topic(), value.getClass().getName()));
    }
    return values.contains(value.toString());
  }
}
