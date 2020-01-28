package io.connect.scylladb.integration;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Preconditions;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

import static io.connect.scylladb.integration.TestDataUtil.asMap;

class RowValidator {
  final String table;
  final Map<String, Object> key;
  final Map<String, Object> value;
  final boolean rowExists;


  RowValidator(String table, Map<String, Object> key, Map<String, Object> value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkState(!key.isEmpty());
    this.table = table;
    this.key = key;
    this.value = value;
    this.rowExists = null != this.value;
  }

  public static RowValidator of(String table, Map<String, Object> key, Map<String, Object> value) {
    return new RowValidator(table, key, value);
  }

  public static RowValidator of(String table, Struct keyStruct, Struct valueStruct) {
    Map<String, Object> key = asMap(keyStruct);
    Map<String, Object> value = asMap(valueStruct);
    return new RowValidator(table, key, value);
  }

  public static Map<String, Object> toMapChecked(Object o) {
    Map<String, Object> result;
    if (o instanceof Map) {
      result = (Map<String, Object>) o;
    } else if (o instanceof Struct) {
      result = asMap((Struct) o);
    } else if (null == o) {
      result = null;
    } else {
      throw new UnsupportedOperationException("Must be a struct or map");
    }
    return result;
  }

  public static RowValidator of(SinkRecord record) {
    Map<String, Object> key = toMapChecked(record.key());
    Map<String, Object> value = toMapChecked(record.value());
    return new RowValidator(record.topic(), key, value);
  }


  @Override
  public String toString() {
    Select select = QueryBuilder.select()
        .from(table);
    Select.Where where = null;

    for (Map.Entry<String, Object> e : key.entrySet()) {
      if (null == where) {
        where = select.where(QueryBuilder.eq(e.getKey(), e.getValue()));
      } else {
        where = where.and(QueryBuilder.eq(e.getKey(), e.getValue()));
      }
    }

    return where.toString();
  }
}
