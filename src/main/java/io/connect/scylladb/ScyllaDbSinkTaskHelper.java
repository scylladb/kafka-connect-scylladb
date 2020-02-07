package io.connect.scylladb;

import com.datastax.driver.core.BoundStatement;
import com.google.common.base.Preconditions;
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
    final String tableName = record.topic();
    BoundStatement boundStatement = null;
    if (null == record.value()) {
      if (scyllaDbSinkConnectorConfig.deletesEnabled) {
        if (this.session.tableExists(tableName)) {
          final RecordToBoundStatementConverter boundStatementConverter = this.session.delete(tableName);
          final RecordToBoundStatementConverter.State state = boundStatementConverter.convert(record.key());
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
                String.format("Record with null value found for the key '%s'. If you are trying to delete the record set " +
                                "scylladb.deletes.enabled = true in your connector configuration.",
                        record.key()));
      }
    } else {
      this.session.createOrAlterTable(tableName, record.keySchema(), record.valueSchema());
      final RecordToBoundStatementConverter boundStatementConverter = this.session.insert(tableName);
      final RecordToBoundStatementConverter.State state = boundStatementConverter.convert(record.value());
      boundStatement = state.statement;
    }

    boundStatement.setConsistencyLevel(this.scyllaDbSinkConnectorConfig.consistencyLevel);
    if (null != record.timestamp()) {
      boundStatement.setDefaultTimestamp(record.timestamp());
    }

    return boundStatement;
  }

}
