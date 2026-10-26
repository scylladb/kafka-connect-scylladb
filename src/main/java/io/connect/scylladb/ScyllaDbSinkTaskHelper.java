package io.connect.scylladb;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.connect.scylladb.topictotable.TopicConfigs;
import io.connect.scylladb.utils.ScyllaDbConstants;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ScyllaDbSinkTaskHelper {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSinkTaskHelper.class);

  private ScyllaDbSinkConnectorConfig scyllaDbSinkConnectorConfig;
  private ScyllaDbSession session;
  private Map<TopicPartition, Integer> topicPartitionRecordSizeMap;


  public ScyllaDbSinkTaskHelper(ScyllaDbSinkConnectorConfig scyllaDbSinkConnectorConfig,
                                ScyllaDbSession session) {
    this.scyllaDbSinkConnectorConfig = scyllaDbSinkConnectorConfig;
    this.session = session;
  }

  public void validateRecord(SinkRecord record) {
    if (null == record.key()) {
      throw new DataException(
              "Record with a null key was encountered. This connector requires that records "
                      + "from Kafka contain the keys for the ScyllaDb table. Please use a "
                      + "transformation like org.apache.kafka.connect.transforms.ValueToKey "
                      + "to create a key with the proper fields."
      );
    }

    if (!(record.key() instanceof Struct) && !(record.key() instanceof Map)) {
      throw new DataException(
              "Key must be a struct or map. This connector requires that records from Kafka "
                      + "contain the keys for the ScyllaDb table. Please use a transformation like "
                      + "org.apache.kafka.connect.transforms.ValueToKey to create a key with the "
                      + "proper fields."
      );
    }
  }

  private static String sanitizedTableName(String topic) {
    return topic.replaceAll("\\.", "_").replaceAll("-", "_");
  }

  private TopicConfigs topicConfigsFor(String topic) {
    final Map<String, TopicConfigs> topicWiseConfigs = scyllaDbSinkConnectorConfig.topicWiseConfigs;
    final String sanitized = sanitizedTableName(topic);
    TopicConfigs topicConfigs = topicWiseConfigs.get(topic);
    if (topicConfigs == null) {
      // Before per-topic settings were matched by the raw topic name, they only took effect
      // when written with the sanitized name, so keep accepting that form, unless the sanitized
      // name is, or under topics.regex may be, a topic of this connector in its own right.
      if (!scyllaDbSinkConnectorConfig.topicsByRegex
              && !scyllaDbSinkConnectorConfig.listedTopics.contains(sanitized)) {
        topicConfigs = topicWiseConfigs.get(sanitized);
      }
      if (topicConfigs != null && scyllaDbSinkConnectorConfig.topicSettingWarnings.add(topic)) {
        log.warn("Per-topic settings for topic '{}' are configured as 'topic.{}.*'. This form is deprecated; "
                + "use the topic name of the record: 'topic.{}.*'.", topic, sanitized, topic);
      }
    } else if (!sanitized.equals(topic) && topicWiseConfigs.containsKey(sanitized)
            && scyllaDbSinkConnectorConfig.topicSettingWarnings.add(topic)) {
      log.warn("Topic '{}' has settings under both 'topic.{}.*' and 'topic.{}.*'. Only 'topic.{}.*' apply to it; "
              + "the others apply only to a topic named '{}'.", topic, topic, sanitized, topic, sanitized);
    }
    return topicConfigs;
  }

  /**
   * @return the statement to execute for the record, or <code>null</code> when there is nothing to
   * execute: a delete from a table that does not exist.
   */
  public BoundStatement getBoundStatementForRecord(SinkRecord record) {
    BoundStatement boundStatement = null;
    final TopicConfigs topicConfigs = topicConfigsFor(record.topic());
    final String keyspace;
    final String tableName;
    if (topicConfigs != null) {
      keyspace = topicConfigs.getKeyspace();
      tableName = topicConfigs.getTable();
      if (topicConfigs.getMappingStringForTopic() != null && !topicConfigs.isScyllaColumnsMapped()) {
        topicConfigs.setTablePartitionAndColumnValues(record);
      }
      topicConfigs.setTtlIfAvailable(record);
    } else {
      keyspace = scyllaDbSinkConnectorConfig.keyspace;
      tableName = sanitizedTableName(record.topic());
    }
    // Read before any DDL, so a record that fails here leaves the table untouched.
    final WriteTimestamp writeTimestamp = topicConfigs != null
            ? topicConfigs.getWriteTimestamp() : scyllaDbSinkConnectorConfig.writeTimestamp;
    final long timestamp = writeTimestamp != null
            ? writeTimestamp.micros(record) : WriteTimestamp.kafkaTimestampMicros(record);
    if (null == record.value()) {
      boolean deletionEnabled = topicConfigs != null
              ? topicConfigs.isDeletesEnabled() : scyllaDbSinkConnectorConfig.deletesEnabled;
      if (deletionEnabled) {
        if (this.session.tableExists(keyspace, tableName)) {
          final RecordToBoundStatementConverter boundStatementConverter = this.session.delete(keyspace, tableName);
          final RecordToBoundStatementConverter.State state = boundStatementConverter.convert(record, topicConfigs, ScyllaDbConstants.DELETE_OPERATION);
          Preconditions.checkState(
                  state.parameters > 0,
                  "key must contain the columns in the primary key."
          );
          boundStatement = state.statement;
        } else {
          log.warn("put() - table '{}.{}' does not exist. Skipping delete.", keyspace, tableName);
          return null;
        }
      } else {
        throw new DataException(
                String.format("Record with null value found for the key '%s'. If you are trying to delete the record set "
                                + "scylladb.deletes.enabled = true or topic.my_topic.my_ks.my_table.deletesEnabled = true in "
                                + "your connector configuration.",
                        record.key()));
      }
    } else {
      this.session.createOrAlterTable(keyspace, tableName, record, topicConfigs);
      final RecordToBoundStatementConverter boundStatementConverter = this.session.insert(keyspace, tableName, topicConfigs);
      final RecordToBoundStatementConverter.State state = boundStatementConverter.convert(record, topicConfigs, ScyllaDbConstants.INSERT_OPERATION);
      boundStatement = state.statement;
    }

    if (topicConfigs != null) {
      log.trace("Topic mapped Consistency level : " + topicConfigs.getConsistencyLevel()
              + ", Record/Topic mapped timestamp : " + timestamp);
      boundStatement = boundStatement.setConsistencyLevel(topicConfigs.getConsistencyLevel());
    } else {
      boundStatement = boundStatement.setConsistencyLevel(this.scyllaDbSinkConnectorConfig.consistencyLevel);
    }
    // The write timestamp is the only ordering between writes to one row: put() runs them concurrently.
    return boundStatement.setQueryTimestamp(timestamp);
  }
}
