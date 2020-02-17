package io.connect.scylladb;

import com.google.common.base.Preconditions;
import io.connect.scylladb.topictotable.TopicConfigs;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class RecordConverter<T> {

    private static final Logger log = LoggerFactory.getLogger(RecordConverter.class);

    protected abstract T newValue();

    protected abstract void setStringField(T result, String name, String value);

    protected abstract void setFloat32Field(T result, String name, Float value);

    protected abstract void setFloat64Field(T result, String name, Double value);

    protected abstract void setTimestampField(T result, String name, Date value);

    protected abstract void setDateField(T result, String name, Date value);

    protected abstract void setTimeField(T result, String name, Date value);

    protected abstract void setInt8Field(T result, String name, Byte value);

    protected abstract void setInt16Field(T result, String name, Short value);

    protected abstract void setInt32Field(T result, String name, Integer value);

    protected abstract void setInt64Field(T result, String name, Long value);

    protected abstract void setBytesField(T result, String name, byte[] value);

    protected abstract void setDecimalField(T result, String name, BigDecimal value);

    protected abstract void setBooleanField(T result, String name, Boolean value);

    protected abstract void setStructField(T result, String name, Struct value);

    protected abstract void setArray(T result, String name, Schema schema, List value);

    protected abstract void setMap(T result, String name, Schema schema, Map value);

    protected abstract void setNullField(T result, String name);

    public T convert(SinkRecord record, TopicConfigs topicConfigs, String operationType) {
        Object recordObject = "delete".equals(operationType) ? record.key() : record.value();
        T result = this.newValue();
        Map<String, TopicConfigs.KafkaScyllaColumnMapper> columnDetailsMap =
                (null == topicConfigs) ? null : topicConfigs.getTableColumnMap();
        Preconditions.checkNotNull(recordObject, ("delete".equals(operationType) ? "key " : "value ") + "cannot be null.");
        if (topicConfigs != null) {
            Preconditions.checkNotNull(record.key(), "key cannot be null.");
            findRecordTypeAndConvert(result, record.key(), topicConfigs.getTablePartitionKeyMap());
            for (Header header : record.headers()) {
                if (topicConfigs.getTableColumnMap().containsKey(header.key())) {
                    TopicConfigs.KafkaScyllaColumnMapper headerKafkaScyllaColumnMapper = topicConfigs.getTableColumnMap().get(header.key());
                    parseStructAndSetInStatement(result, header.schema(),
                            headerKafkaScyllaColumnMapper.getKafkaRecordField(), header.value(),
                            headerKafkaScyllaColumnMapper.getScyllaColumnName());
                }
            }
        }
        findRecordTypeAndConvert(result, recordObject, columnDetailsMap);
        return result;
    }

    void findRecordTypeAndConvert(T result, Object recordObject,
                                  Map<String, TopicConfigs.KafkaScyllaColumnMapper> columnDetailsMap) {
        if (recordObject instanceof Struct) {
            this.convertStruct(result, (Struct)recordObject, columnDetailsMap);
        } else {
            if (!(recordObject instanceof Map)) {
                throw new DataException(String.format("Only Schema (%s) or Schema less (%s) are supported. %s is not a supported type.", Struct.class.getName(), Map.class.getName(), recordObject.getClass().getName()));
            }

            this.convertMap(result, (Map)recordObject, columnDetailsMap);
        }
    }

    void convertMap(T result, Map value, Map<String, TopicConfigs.KafkaScyllaColumnMapper> columnDetailsMap) {
        Iterator valueIterator = value.keySet().iterator();

        while(valueIterator.hasNext()) {
            Object key = valueIterator.next();
            Preconditions.checkState(key instanceof String, "Map key must be a String.");
            String fieldName = (String)key;
            Object fieldValue = value.get(key);
            if (columnDetailsMap != null) {
                if (columnDetailsMap.containsKey(fieldName)) {
                    fieldName = columnDetailsMap.get(fieldName).getScyllaColumnName();
                } else {
                    continue;
                }
            }

            try {
                if (null == fieldValue) {
                    log.trace("convertStruct() - Setting '{}' to null.", fieldName);
                    this.setNullField(result, fieldName);
                } else if (fieldValue instanceof String) {
                    log.trace("convertStruct() - Processing '{}' as string.", fieldName);
                    this.setStringField(result, fieldName, (String)fieldValue);
                } else if (fieldValue instanceof Byte) {
                    log.trace("convertStruct() - Processing '{}' as int8.", fieldName);
                    this.setInt8Field(result, fieldName, (Byte)fieldValue);
                } else if (fieldValue instanceof Short) {
                    log.trace("convertStruct() - Processing '{}' as int16.", fieldName);
                    this.setInt16Field(result, fieldName, (Short)fieldValue);
                } else if (fieldValue instanceof Integer) {
                    log.trace("convertStruct() - Processing '{}' as int32.", fieldName);
                    this.setInt32Field(result, fieldName, (Integer)fieldValue);
                } else if (fieldValue instanceof Long) {
                    log.trace("convertStruct() - Processing '{}' as long.", fieldName);
                    this.setInt64Field(result, fieldName, (Long)fieldValue);
                } else if (fieldValue instanceof BigInteger) {
                    log.trace("convertStruct() - Processing '{}' as long.", fieldName);
                    this.setInt64Field(result, fieldName, ((BigInteger)fieldValue).longValue());
                } else if (fieldValue instanceof Double) {
                    log.trace("convertStruct() - Processing '{}' as float64.", fieldName);
                    this.setFloat64Field(result, fieldName, (Double)fieldValue);
                } else if (fieldValue instanceof Float) {
                    log.trace("convertStruct() - Processing '{}' as float32.", fieldName);
                    this.setFloat32Field(result, fieldName, (Float)fieldValue);
                } else if (fieldValue instanceof BigDecimal) {
                    log.trace("convertStruct() - Processing '{}' as decimal.", fieldName);
                    this.setDecimalField(result, fieldName, (BigDecimal)fieldValue);
                } else if (fieldValue instanceof Boolean) {
                    log.trace("convertStruct() - Processing '{}' as boolean.", fieldName);
                    this.setBooleanField(result, fieldName, (Boolean)fieldValue);
                } else if (fieldValue instanceof Date) {
                    log.trace("convertStruct() - Processing '{}' as timestamp.", fieldName);
                    this.setTimestampField(result, fieldName, (Date)fieldValue);
                } else if (fieldValue instanceof byte[]) {
                    log.trace("convertStruct() - Processing '{}' as bytes.", fieldName);
                    this.setBytesField(result, fieldName, (byte[])((byte[])fieldValue));
                } else if (fieldValue instanceof List) {
                    log.trace("convertStruct() - Processing '{}' as array.", fieldName);
                    this.setArray(result, fieldName, (Schema)null, (List)fieldValue);
                } else {
                    if (!(fieldValue instanceof Map)) {
                        throw new DataException(String.format("%s is not a supported data type.", fieldValue.getClass().getName()));
                    }

                    log.trace("convertStruct() - Processing '{}' as map.", fieldName);
                    this.setMap(result, fieldName, (Schema)null, (Map)fieldValue);
                }
            } catch (Exception ex) {
                throw new DataException(String.format("Exception thrown while processing field '%s'", fieldName), ex);
            }
        }

    }

    void convertStruct(T result, Struct struct, Map<String, TopicConfigs.KafkaScyllaColumnMapper> columnDetailsMap) {
        Schema schema = struct.schema();
        Iterator fieldsIterator = schema.fields().iterator();

        while(fieldsIterator.hasNext()) {
            Field field = (Field)fieldsIterator.next();
            String fieldName = field.name();
            log.trace("convertStruct() - Processing '{}'", field.name());
            Object fieldValue = struct.get(field);
            if (columnDetailsMap != null) {
                if (columnDetailsMap.containsKey(fieldName)) {
                    fieldName = columnDetailsMap.get(fieldName).getScyllaColumnName();
                } else {
                    continue;
                }
            }
            parseStructAndSetInStatement(result, schema, field, fieldValue, fieldName);
        }
    }

    void parseStructAndSetInStatement(T result, Schema schema, Field field, Object fieldValue, String fieldName) {
        try {
            if (null == fieldValue) {
                log.trace("convertStruct() - Setting '{}' to null.", fieldName);
                this.setNullField(result, fieldName);
            } else {
                log.trace("convertStruct() - Field '{}'.field().schema().type() = '{}'", fieldName, field.schema().type());
                switch(field.schema().type()) {
                    case STRING:
                        log.trace("convertStruct() - Processing '{}' as string.", fieldName);
                        this.setStringField(result, fieldName, (String)fieldValue);
                        break;
                    case INT8:
                        log.trace("convertStruct() - Processing '{}' as int8.", fieldName);
                        this.setInt8Field(result, fieldName, (Byte)fieldValue);
                        break;
                    case INT16:
                        log.trace("convertStruct() - Processing '{}' as int16.", fieldName);
                        this.setInt16Field(result, fieldName, (Short)fieldValue);
                        break;
                    case INT32:
                        if ("org.apache.kafka.connect.data.Date".equals(field.schema().name())) {
                            log.trace("convertStruct() - Processing '{}' as date.", fieldName);
                            this.setDateField(result, fieldName, (Date)fieldValue);
                        } else if ("org.apache.kafka.connect.data.Time".equals(field.schema().name())) {
                            log.trace("convertStruct() - Processing '{}' as time.", fieldName);
                            this.setTimeField(result, fieldName, (Date)fieldValue);
                        } else {
                            Integer int32Value = (Integer)fieldValue;
                            log.trace("convertStruct() - Processing '{}' as int32.", fieldName);
                            this.setInt32Field(result, fieldName, int32Value);
                        }
                        break;
                    case INT64:
                        if ("org.apache.kafka.connect.data.Timestamp".equals(field.schema().name())) {
                            log.trace("convertStruct() - Processing '{}' as timestamp.", fieldName);
                            this.setTimestampField(result, fieldName, (Date)fieldValue);
                        } else {
                            Long int64Value = (Long)fieldValue;
                            log.trace("convertStruct() - Processing '{}' as int64.", fieldName);
                            this.setInt64Field(result, fieldName, int64Value);
                        }
                        break;
                    case BYTES:
                        if ("org.apache.kafka.connect.data.Decimal".equals(field.schema().name())) {
                            log.trace("convertStruct() - Processing '{}' as decimal.", fieldName);
                            this.setDecimalField(result, fieldName, (BigDecimal)fieldValue);
                        } else {
                            byte[] bytes = (byte[])((byte[])fieldValue);
                            log.trace("convertStruct() - Processing '{}' as bytes.", fieldName);
                            this.setBytesField(result, fieldName, bytes);
                        }
                        break;
                    case FLOAT32:
                        log.trace("convertStruct() - Processing '{}' as float32.", fieldName);
                        this.setFloat32Field(result, fieldName, (Float)fieldValue);
                        break;
                    case FLOAT64:
                        log.trace("convertStruct() - Processing '{}' as float64.", fieldName);
                        this.setFloat64Field(result, fieldName, (Double)fieldValue);
                        break;
                    case BOOLEAN:
                        log.trace("convertStruct() - Processing '{}' as boolean.", fieldName);
                        this.setBooleanField(result, fieldName, (Boolean)fieldValue);
                        break;
                    case STRUCT:
                        log.trace("convertStruct() - Processing '{}' as struct.", fieldName);
                        this.setStructField(result, fieldName, (Struct)fieldValue);
                        break;
                    case ARRAY:
                        log.trace("convertStruct() - Processing '{}' as array.", fieldName);
                        this.setArray(result, fieldName, schema, (List)fieldValue);
                        break;
                    case MAP:
                        log.trace("convertStruct() - Processing '{}' as map.", fieldName);
                        this.setMap(result, fieldName, schema, (Map)fieldValue);
                        break;
                    default:
                        throw new DataException("Unsupported schema.type(): " + schema.type());
                }
            }
        } catch (Exception ex) {
            throw new DataException(String.format("Exception thrown while processing field '%s'", fieldName), ex);
        }
    }
}

