package io.connect.scylladb.integration;

import com.google.common.base.Preconditions;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordUtil {

    public static final int PARTITION = 1;
    public static final long OFFSET = 91283741L;
    public static final long TIMESTAMP = 1530286549123L;

    public static SinkRecord delete(String topic, Struct key) {
        Preconditions.checkNotNull(key, "key cannot be null.");
        return delete(topic, new SchemaAndValue(key.schema(), key));
    }

    public static SinkRecord delete(String topic, Schema keySchema, Object key) {
        return delete(topic, new SchemaAndValue(keySchema, key));
    }

    public static SinkRecord delete(String topic, SchemaAndValue key) {
        Preconditions.checkNotNull(topic, "topic cannot be null");
        if (null == key) {
            throw new DataException("key cannot be null.");
        }
        if (null == key.value()) {
            throw new DataException("key values cannot be null.");
        }

        return new SinkRecord(
                topic,
                PARTITION,
                key.schema(),
                key.value(),
                null,
                null,
                OFFSET,
                TIMESTAMP,
                TimestampType.CREATE_TIME
        );
    }

    public static SinkRecord write(String topic, Struct key, Struct value) {
        return write(
                topic,
                new SchemaAndValue(key.schema(), key),
                new SchemaAndValue(value.schema(), value)
        );
    }

    public static SinkRecord write(String topic, Schema keySchema, Object key, Schema valueSchema, Object value) {
        return write(
                topic,
                new SchemaAndValue(keySchema, key),
                new SchemaAndValue(valueSchema, value)
        );
    }

    public static SinkRecord write(String topic, SchemaAndValue key, SchemaAndValue value) {
        Preconditions.checkNotNull(topic, "topic cannot be null");
        Preconditions.checkNotNull(key, "key cannot be null.");
        Preconditions.checkNotNull(key.value(), "key values cannot be null.");
        Preconditions.checkNotNull(value, "value cannot be null.");
        Preconditions.checkNotNull(value.value(), "value values cannot be null.");

        return new SinkRecord(
                topic,
                PARTITION,
                key.schema(),
                key.value(),
                value.schema(),
                value.value(),
                OFFSET,
                TIMESTAMP,
                TimestampType.CREATE_TIME
        );
    }

}
