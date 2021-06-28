package io.connect.scylladb.topictotable;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import io.connect.scylladb.ScyllaDbSinkConnectorConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TopicConfigs {

  private static final Logger log = LoggerFactory.getLogger(TopicConfigs.class);
  private String mappingStringForTopic;
  private Map<String, KafkaScyllaColumnMapper> tablePartitionKeyMap;
  private Map<String, KafkaScyllaColumnMapper> tableColumnMap;
  private ConsistencyLevel consistencyLevel = null;
  private String ttlMappedField;
  private Integer ttl;
  private String timeStampMappedField;
  private Long timeStamp;
  private boolean deletesEnabled;
  private boolean isScyllaColumnsMapped;
  private String keyspace;

  public TopicConfigs(Map<String, String> configsMapForTheTopic,
                      ScyllaDbSinkConnectorConfig scyllaDbSinkConnectorConfig) {
    this.tablePartitionKeyMap = new HashMap<>();
    this.tableColumnMap = new HashMap<>();
    this.consistencyLevel = scyllaDbSinkConnectorConfig.consistencyLevel;
    this.ttl = scyllaDbSinkConnectorConfig.ttl;
    this.deletesEnabled = scyllaDbSinkConnectorConfig.deletesEnabled;
    if (configsMapForTheTopic.containsKey("keyspace")) {
      this.keyspace = configsMapForTheTopic.get("keyspace");
    } else {
      this.keyspace = scyllaDbSinkConnectorConfig.keyspace;
    }
    if (configsMapForTheTopic.containsKey("mapping")) {
      this.mappingStringForTopic = configsMapForTheTopic.get("mapping");
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
        this.consistencyLevel = ConsistencyLevel.valueOf(configsMapForTheTopic.get("consistencyLevel"));
      }
    } catch (NumberFormatException e) {
      throw new DataException(
              String.format("The setting ttlSeconds must be of type Integer. %s is not a suppoerted type",
                      configsMapForTheTopic.get("ttlSeconds").getClass().getName()));
    } catch (IllegalArgumentException e) {
      throw  new DataException(
              String.format("%s is not a valid value for consistencyLevel. Valid values are %s",
                      configsMapForTheTopic.get("consistencyLevel"), Arrays.toString(ConsistencyLevel.values()))
      );
    }
  }

  public void setTablePartitionAndColumnValues(SinkRecord record) {
    for (String mappedEntry : this.mappingStringForTopic.split(",")) {
      String[] columnNameMap = mappedEntry.split("=");
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
    this.isScyllaColumnsMapped = true;
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

  public void setTtlAndTimeStampIfAvailable(SinkRecord record) {
    // Timestamps in Kafka (record.timestamp()) are in millisecond precision,
    // while Scylla expects a microsecond precision: 1 ms = 1000 us.
    this.timeStamp = record.timestamp() * 1000;
    if (timeStampMappedField != null) {
      Object timeStampValue = getValueOfField(record.value(), timeStampMappedField);
      if (timeStampValue instanceof Long) {
        this.timeStamp = (Long) timeStampValue;
      } else {
        throw new DataException(
                String.format("TimeStamp should be of type Long. But record provided for %s is of type %s",
                        timeStampMappedField, timeStampValue.getClass().getName()
                ));
      }
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

  public Map<String, KafkaScyllaColumnMapper> getTablePartitionKeyMap() {
    return tablePartitionKeyMap;
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

  public Long getTimeStamp() {
    return timeStamp;
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

  public String getKeyspace() {
    return keyspace;
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
