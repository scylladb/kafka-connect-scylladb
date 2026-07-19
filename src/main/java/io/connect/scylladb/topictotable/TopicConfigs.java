package io.connect.scylladb.topictotable;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
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
import java.util.Date;
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
  private String expirationMappedField;
  private int expirationOffsetSeconds;
  private String timeStampMappedField;
  private Long timeStamp;
  private boolean deletesEnabled;
  private boolean isScyllaColumnsMapped;

  public TopicConfigs(Map<String, String> configsMapForTheTopic,
                      ScyllaDbSinkConnectorConfig scyllaDbSinkConnectorConfig) {
    this.tablePartitionKeyMap = new HashMap<>();
    this.tableColumnMap = new HashMap<>();
    this.consistencyLevel = scyllaDbSinkConnectorConfig.consistencyLevel;
    this.ttl = scyllaDbSinkConnectorConfig.ttl;
    this.deletesEnabled = scyllaDbSinkConnectorConfig.deletesEnabled;
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
      if (configsMapForTheTopic.containsKey("expirationOffsetSeconds")) {
        this.expirationOffsetSeconds = Integer.parseInt(configsMapForTheTopic.get("expirationOffsetSeconds"));
        if (this.expirationOffsetSeconds < 0) {
          throw new DataException("The setting expirationOffsetSeconds must not be negative.");
        }
      }
      if (configsMapForTheTopic.containsKey("consistencyLevel")) {
        this.consistencyLevel = DefaultConsistencyLevel.valueOf(configsMapForTheTopic.get("consistencyLevel"));
      }
    } catch (NumberFormatException e) {
      throw new DataException(
              "The settings ttlSeconds and expirationOffsetSeconds must be integers.", e);
    } catch (IllegalArgumentException e) {
      throw  new DataException(
              String.format("%s is not a valid value for consistencyLevel. Valid values are %s",
                      configsMapForTheTopic.get("consistencyLevel"), Arrays.toString(DefaultConsistencyLevel.values()))
      );
    }
  }

  public void setTablePartitionAndColumnValues(SinkRecord record) {
    for (String mappedEntry : this.mappingStringForTopic.split(",")) {
      String[] columnNameMap = mappedEntry.trim().split("=", 2);
      if (columnNameMap.length != 2) {
        throw new DataException("Each mapping entry must have the form column=key.field, column=value.field or column=header.field.");
      }
      String recordFieldMapping = columnNameMap[1].trim();
      String[] recordFieldParts = recordFieldMapping.split("\\.", 2);
      String recordField = recordFieldParts.length == 2 ? recordFieldParts[1] : "";
      String scyllaColumnName = columnNameMap[0].trim();
      KafkaScyllaColumnMapper kafkaScyllaColumnMapper = new KafkaScyllaColumnMapper(scyllaColumnName);
      if (recordFieldMapping.startsWith("key.")) {
        if (record.keySchema() != null) {
          kafkaScyllaColumnMapper.kafkaRecordField = getFiledForNameFromSchema(record.keySchema(), recordField, "record.keySchema()");
        }
        this.tablePartitionKeyMap.put(recordField, kafkaScyllaColumnMapper);
      } else if (recordFieldMapping.startsWith("value.")) {
        Field valueField = null;
        if (record.valueSchema() != null) {
          valueField = getFiledForNameFromSchema(record.valueSchema(), recordField, "record.valueSchema()");
        }
        if (scyllaColumnName.equals("__ttl")) {
          ttlMappedField = recordField;
        } else if (scyllaColumnName.equals("__expiration")) {
          expirationMappedField = recordField;
        } else if (scyllaColumnName.equals("__timestamp")) {
          timeStampMappedField = recordField;
        } else {
          kafkaScyllaColumnMapper.kafkaRecordField = valueField;
          this.tableColumnMap.put(recordField, kafkaScyllaColumnMapper);
        }
      } else if (recordFieldMapping.startsWith("header.")) {
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
    if (ttlMappedField != null && expirationMappedField != null) {
      throw new DataException("A mapping cannot contain both __ttl and __expiration.");
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
    setTtlAndTimeStampIfAvailable(record, System.currentTimeMillis());
  }

  void setTtlAndTimeStampIfAvailable(SinkRecord record, long currentTimeMillis) {
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
    } else if (expirationMappedField != null) {
      Object expirationValue = getValueOfField(record.value(), expirationMappedField);
      final long expirationTimeMillis;
      if (expirationValue instanceof Date) {
        expirationTimeMillis = ((Date) expirationValue).getTime();
      } else if (expirationValue instanceof Long) {
        expirationTimeMillis = (Long) expirationValue;
      } else {
        throw new DataException(
                String.format("Expiration should be a Kafka Timestamp or epoch-millisecond Long. "
                                + "But record provided for %s is of type %s",
                        expirationMappedField,
                        expirationValue == null ? "null" : expirationValue.getClass().getName()));
      }

      final long expiresAtMillis;
      try {
        expiresAtMillis = Math.addExact(
                expirationTimeMillis,
                Math.multiplyExact((long) expirationOffsetSeconds, 1000L));
      } catch (ArithmeticException e) {
        throw new DataException("Expiration timestamp plus expirationOffsetSeconds is outside the supported range.", e);
      }
      final long remainingMillis;
      try {
        remainingMillis = Math.subtractExact(expiresAtMillis, currentTimeMillis);
      } catch (ArithmeticException e) {
        throw new DataException("Calculated expiration interval is outside the supported range.", e);
      }
      if (remainingMillis <= 0) {
        throw new DataException("Expiration timestamp plus expirationOffsetSeconds must be in the future.");
      }
      long ttlSeconds = Math.floorDiv(remainingMillis - 1, 1000) + 1;
      if (ttlSeconds > Integer.MAX_VALUE) {
        throw new DataException("Calculated TTL exceeds the maximum supported integer value.");
      }
      this.ttl = (int) ttlSeconds;
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
