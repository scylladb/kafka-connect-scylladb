package io.connect.scylladb;

import com.datastax.driver.core.BoundStatement;
import com.google.common.base.Preconditions;
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

  public BoundStatement getBoundStatementForRecord(SinkRecord record) {
    final String tableName = record.topic().replaceAll("\\.", "_").replaceAll("-", "_");
    BoundStatement boundStatement = null;
    TopicConfigs topicConfigs = null;
    if (scyllaDbSinkConnectorConfig.topicWiseConfigs.containsKey(tableName)) {
      topicConfigs = scyllaDbSinkConnectorConfig.topicWiseConfigs.get(tableName);
      if (topicConfigs.getMappingStringForTopic() != null && !topicConfigs.isScyllaColumnsMapped()) {
        topicConfigs.setTablePartitionAndColumnValues(record);
      }
      topicConfigs.setTtlAndTimeStampIfAvailable(record);
    }
    if (null == record.value()) {
      boolean deletionEnabled = topicConfigs != null
              ? topicConfigs.isDeletesEnabled() : scyllaDbSinkConnectorConfig.deletesEnabled;
      if (deletionEnabled) {
        if (this.session.tableExists(tableName)) {
          final RecordToBoundStatementConverter boundStatementConverter = this.session.delete(tableName);
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
      this.session.createOrAlterTable(tableName, record, topicConfigs);
      final RecordToBoundStatementConverter boundStatementConverter = this.session.insert(tableName, topicConfigs);
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
      boundStatement.setDefaultTimestamp(record.timestamp());
    }
    return boundStatement;
  }
}
