package io.connect.scylladb;

import com.datastax.driver.core.BoundStatement;
import com.google.common.base.Preconditions;
import io.connect.scylladb.topictotable.TopicConfigs;
import io.connect.scylladb.utils.ScyllaDbConstants;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ScyllaDbSinkTaskHelper {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSinkTaskHelper.class);

  private ScyllaDbSinkConnectorConfig scyllaDbSinkConnectorConfig;
  private ScyllaDbSession session;


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

  private String extractKeyspace(SinkRecord record, TopicConfigs topicConfigs) {
    Header keyspaceHeader = record.headers().lastWithName("keyspace");
    if (keyspaceHeader != null) {
      return keyspaceHeader.value().toString();
    } else if (topicConfigs != null && topicConfigs.getKeyspace() != null) {
      return topicConfigs.getKeyspace();
    } else {
      return scyllaDbSinkConnectorConfig.keyspace;
    }
  }

  public BoundStatement getBoundStatementForRecord(SinkRecord record) {
    final String tableName = record.topic();
    BoundStatement boundStatement = null;
    TopicConfigs topicConfigs = null;
    if (scyllaDbSinkConnectorConfig.topicWiseConfigs.containsKey(tableName)) {
      topicConfigs = scyllaDbSinkConnectorConfig.topicWiseConfigs.get(tableName);
      if (topicConfigs.getMappingStringForTopic() != null && !topicConfigs.isScyllaColumnsMapped()) {
        topicConfigs.setTablePartitionAndColumnValues(record);
      }
      topicConfigs.setTtlAndTimeStampIfAvailable(record);
    }
    final String keyspace = extractKeyspace(record, topicConfigs);
    if (null == record.value()) {
      boolean deletionEnabled = topicConfigs != null
              ? topicConfigs.isDeletesEnabled() : scyllaDbSinkConnectorConfig.deletesEnabled;
      if (deletionEnabled) {
        if (this.session.tableExists(keyspace, tableName)) {
          final RecordToBoundStatementConverter boundStatementConverter = this.session.delete(keyspace, tableName);
          final RecordToBoundStatementConverter.State state = boundStatementConverter.convert(record, null, ScyllaDbConstants.DELETE_OPERATION);
          Preconditions.checkState(
                  state.parameters > 0,
                  "key must contain the columns in the primary key."
          );
          boundStatement = state.statement;
        } else {
          log.warn("put() - table '{}' does not exist. Skipping delete.", tableName);
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
              + ", Record/Topic mapped timestamp : " + topicConfigs.getTimeStamp());
      boundStatement.setConsistencyLevel(topicConfigs.getConsistencyLevel());
      boundStatement.setDefaultTimestamp(topicConfigs.getTimeStamp());
    } else {
      boundStatement.setConsistencyLevel(this.scyllaDbSinkConnectorConfig.consistencyLevel);
      // Timestamps in Kafka (record.timestamp()) are in millisecond precision,
      // while Scylla expects a microsecond precision: 1 ms = 1000 us.
      boundStatement.setDefaultTimestamp(record.timestamp() * 1000);
    }
    return boundStatement;
  }
}
