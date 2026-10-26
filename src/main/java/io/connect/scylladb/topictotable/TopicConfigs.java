package io.connect.scylladb.topictotable;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import io.connect.scylladb.ScyllaDbSinkConnectorConfig;
import io.connect.scylladb.WriteTimestamp;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TopicConfigs {

  private static final Logger log = LoggerFactory.getLogger(TopicConfigs.class);
  private final String keyspace;
  private final String table;
  private String mappingStringForTopic;
  private Map<String, KafkaScyllaColumnMapper> tablePartitionKeyMap;
  private Map<String, KafkaScyllaColumnMapper> tableColumnMap;
  private final Map<String, String> valueFieldColumns = new HashMap<>();
  private ConsistencyLevel consistencyLevel = null;
  private String ttlMappedField;
  private Integer ttl;
  private String timeStampMappedField;
  private final WriteTimestamp writeTimestamp;
  private boolean deletesEnabled;
  private boolean isScyllaColumnsMapped;

  public TopicConfigs(String keyspace, String table, Map<String, String> configsMapForTheTopic,
                      ScyllaDbSinkConnectorConfig scyllaDbSinkConnectorConfig) {
    this.keyspace = keyspace;
    this.table = table;
    this.tablePartitionKeyMap = new HashMap<>();
    this.tableColumnMap = new HashMap<>();
    this.consistencyLevel = scyllaDbSinkConnectorConfig.consistencyLevel;
    this.ttl = scyllaDbSinkConnectorConfig.ttl;
    this.deletesEnabled = scyllaDbSinkConnectorConfig.deletesEnabled;
    // A per-topic field or unit overrides only its own connector-level counterpart.
    final String timestampField = configsMapForTheTopic.getOrDefault(
            "timestampField", scyllaDbSinkConnectorConfig.timestampField);
    final String timestampUnit = configsMapForTheTopic.getOrDefault(
            "timestampUnit", scyllaDbSinkConnectorConfig.timestampUnit);
    if (configsMapForTheTopic.containsKey("timestampUnit")) {
      // Checked even when no field uses it, so a typo is not silently ignored.
      WriteTimestamp.parseUnit(timestampUnit);
    }
    this.writeTimestamp = Strings.isNullOrEmpty(timestampField)
            ? null : WriteTimestamp.of(timestampField, timestampUnit);
    if (configsMapForTheTopic.containsKey("mapping")) {
      this.mappingStringForTopic = configsMapForTheTopic.get("mapping");
      for (String mappedEntry : this.mappingStringForTopic.split(",")) {
        String[] columnNameMap = mappedEntry.split("=");
        String column = columnNameMap[0].trim();
        if (columnNameMap.length == 2 && columnNameMap[1].startsWith("value.")
            && !column.equals("__ttl") && !column.equals("__timestamp")) {
          this.valueFieldColumns.put(columnNameMap[1].substring("value.".length()), column);
        }
      }
    }
    if (configsMapForTheTopic.containsKey("deletesEnabled")) {
      String deleteEnabledValue = configsMapForTheTopic.get("deletesEnabled");
      if ("true".equalsIgnoreCase(deleteEnabledValue) || "false".equalsIgnoreCase(deleteEnabledValue)) {
        this.deletesEnabled = Boolean.parseBoolean(deleteEnabledValue);
      } else {
        throw new DataException(
                String.format("%s is not a valid value for deletesEnabled. Valid values are : true, false",
                        deleteEnabledValue
                )
        );
      }
    }
    try {
      if (configsMapForTheTopic.containsKey("ttlSeconds")) {
        this.ttl = Integer.parseInt(configsMapForTheTopic.get("ttlSeconds"));
      }
      if (configsMapForTheTopic.containsKey("consistencyLevel")) {
        this.consistencyLevel = DefaultConsistencyLevel.valueOf(configsMapForTheTopic.get("consistencyLevel"));
      }
    } catch (NumberFormatException e) {
      throw new DataException(
              String.format("The setting ttlSeconds must be of type Integer. %s is not a supported type",
                      configsMapForTheTopic.get("ttlSeconds").getClass().getName()));
    } catch (IllegalArgumentException e) {
      throw  new DataException(
              String.format("%s is not a valid value for consistencyLevel. Valid values are %s",
                      configsMapForTheTopic.get("consistencyLevel"), Arrays.toString(DefaultConsistencyLevel.values()))
      );
    }
  }

  public void setTablePartitionAndColumnValues(SinkRecord record) {
    // A tombstone has no value schema to resolve value fields against, so it maps only the key,
    // and the first record with a value completes the mapping.
    final boolean tombstone = record.value() == null;
    for (String mappedEntry : this.mappingStringForTopic.split(",")) {
      String[] columnNameMap = mappedEntry.split("=");
      if (tombstone && !columnNameMap[1].startsWith("key.")) {
        continue;
      }
      String recordField = columnNameMap[1].split("\\.").length > 0
              ? columnNameMap[1].split("\\.")[1] : "";
      String scyllaColumnName = columnNameMap[0].trim();
      KafkaScyllaColumnMapper kafkaScyllaColumnMapper = new KafkaScyllaColumnMapper(scyllaColumnName);
      if (columnNameMap[1].startsWith("key.")) {
        if (record.keySchema() != null) {
          kafkaScyllaColumnMapper.kafkaRecordField = getFiledForNameFromSchema(record.keySchema(), recordField, "record.keySchema()");
        }
        this.tablePartitionKeyMap.put(recordField, kafkaScyllaColumnMapper);
      } else if (columnNameMap[1].startsWith("value.")) {
        Field valueField = null;
        if (record.valueSchema() != null) {
          valueField = getFiledForNameFromSchema(record.valueSchema(), recordField, "record.valueSchema()");
        }
        if (scyllaColumnName.equals("__ttl")) {
          ttlMappedField = recordField;
        } else if (scyllaColumnName.equals("__timestamp")) {
          timeStampMappedField = recordField;
        } else {
          kafkaScyllaColumnMapper.kafkaRecordField = valueField;
          this.tableColumnMap.put(recordField, kafkaScyllaColumnMapper);
        }
      } else if (columnNameMap[1].startsWith("header.")) {
        int index = 0;
        for (Header header : record.headers()) {
          if (header.key().equals(recordField)) {
            if (header.schema().type().isPrimitive()) {
              kafkaScyllaColumnMapper.kafkaRecordField = new Field(header.key(), index, header.schema());
              tableColumnMap.put(recordField, kafkaScyllaColumnMapper);
              index++;
            } else {
              throw new IllegalArgumentException(String.format("Header schema type should be of primitive type. "
                      + "%s schema type is not allowed in header.", header.schema().type().getName()));
            }
          }
        }
      } else {
        throw new IllegalArgumentException("field name must start with 'key.', 'value.' or 'header.'.");
      }
    }
    this.isScyllaColumnsMapped = !tombstone;
  }

  private Field getFiledForNameFromSchema(Schema schema, String name, String schemaType) {
    Field schemaField = schema.field(name);
    if (null == schemaField) {
      throw new DataException(
              String.format(
                      schemaType + " must contain all of key fields mentioned in the "
                                + "'topic.my_topic.my_ks.my_table.mapping' config. " + schemaType
                                + "is missing field '%s'. " + schemaType + " is used by the connector "
                                + "to persist data to the table in ScyllaDb. Here are "
                                + "the available fields for " + schemaType + "(%s).",
                      name,
                      Joiner.on(", ").join(
                              schema.fields().stream().map(Field::name).collect(Collectors.toList())
                      )
              )
      );
    }
    return schemaField;
  }

  public void setTtlIfAvailable(SinkRecord record) {
    // Tombstones carry no value, so value-mapped fields cannot apply to them.
    if (record.value() == null) {
      return;
    }
    if (ttlMappedField != null) {
      Object ttlValue = getValueOfField(record.value(), ttlMappedField);
      if (ttlValue instanceof  Integer) {
        this.ttl = (Integer) ttlValue;
      } else {
        throw new DataException(
                String.format("TTL should be of type Integer. But record provided for %s is of type %s",
                        ttlMappedField, ttlValue.getClass().getName()
                ));
      }
    }
  }

  public Object getValueOfField(Object value, String field) {
    Preconditions.checkNotNull(value, "value cannot be null.");
    if (value instanceof Struct) {
      return ((Struct)value).get(field);
    } else {
      if (!(value instanceof Map)) {
        throw new DataException(String.format("Only Schema (%s) or Schema less (%s) are supported. %s is not a supported type.", Struct.class.getName(), Map.class.getName(), value.getClass().getName()));
      }
      return ((Map)value).get(field);
    }
  }

  public String getKeyspace() {
    return keyspace;
  }

  public String getTable() {
    return table;
  }

  public Map<String, KafkaScyllaColumnMapper> getTablePartitionKeyMap() {
    return tablePartitionKeyMap;
  }

  /**
   * Columns a delete binds the given key fields to. Without {@code key.*} entries, a key field
   * goes to the column its {@code value.*} namesake maps to, or else to a column of its own name.
   */
  public Map<String, KafkaScyllaColumnMapper> getDeleteKeyMap(Collection<String> keyFields) {
    if (!tablePartitionKeyMap.isEmpty()) {
      return tablePartitionKeyMap;
    }
    Map<String, KafkaScyllaColumnMapper> deleteKeyMap = new HashMap<>();
    for (String keyField : keyFields) {
      deleteKeyMap.put(keyField,
          new KafkaScyllaColumnMapper(valueFieldColumns.getOrDefault(keyField, keyField)));
    }
    return deleteKeyMap;
  }

  public Map<String, KafkaScyllaColumnMapper> getTableColumnMap() {
    return tableColumnMap;
  }

  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  public String getTtlMappedField() {
    return ttlMappedField;
  }

  public Integer getTtl() {
    return ttl;
  }

  /**
   * @return the write timestamp source set by the timestampField settings, or by the deprecated
   * <code>__timestamp</code> mapping; null when the Kafka record timestamp applies.
   */
  public WriteTimestamp getWriteTimestamp() {
    if (writeTimestamp == null && timeStampMappedField != null) {
      return WriteTimestamp.legacyMapping(timeStampMappedField);
    }
    return writeTimestamp;
  }

  public boolean isScyllaColumnsMapped() {
    return isScyllaColumnsMapped;
  }

  public void setScyllaColumnsMappedFalse() {
    this.isScyllaColumnsMapped = false;
  }

  public String getMappingStringForTopic() {
    return mappingStringForTopic;
  }

  public boolean isDeletesEnabled() {
    return deletesEnabled;
  }

  public class KafkaScyllaColumnMapper {
    private String scyllaColumnName;
    private Field kafkaRecordField;

    KafkaScyllaColumnMapper(String scyllaColumnName) {
      this.scyllaColumnName = scyllaColumnName;
    }

    public String getScyllaColumnName() {
      return scyllaColumnName;
    }

    public Field getKafkaRecordField() {
      return kafkaRecordField;
    }
  }
}
