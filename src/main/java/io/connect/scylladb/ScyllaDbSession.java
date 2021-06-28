package io.connect.scylladb;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import io.connect.scylladb.topictotable.TopicConfigs;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

public interface ScyllaDbSession extends Closeable {

    /**
     * Execute a statement
     */
    ResultSet executeStatement(Statement statement);

    /**
     * Execute a statement asynchronously
     */
    ResultSetFuture executeStatementAsync(Statement statement);

    /**
     * Execute a query
     */
    ResultSet executeQuery(String query);

    /**
     * Lookup metadata for a table.
     * @param tableName name of the table
     */
    TableMetadata.Table tableMetadata(String keyspace, String tableName);

    /**
     * Check if a table exists.
     * @param tableName name of the table
     */
    boolean tableExists(String keyspace, String tableName);

    /**
     * Ensure that a table has a specified schema.
     * @param tableName name of the table
     * @param sinkRecord which will have keySchema that will be used for the primary key and
     * valueSchema that will be used for the rest of the table.
     * @param topicConfigs class containing mapping details for the record
     */
    void createOrAlterTable(String keyspace, String tableName, SinkRecord sinkRecord, TopicConfigs topicConfigs);

    /**
     * Flag to determine if the session is valid.
     */
    boolean isValid();

    /**
     * Method is used to mark a session as invalid.
     */
    void setInvalid();

    /**
     * Method will return a RecordToBoundStatementConverter for a delete the supplied table.
     * @param tableName table to return the RecordToBoundStatementConverter for
     * @return RecordToBoundStatementConverter that can be used for the record.
     */
    RecordToBoundStatementConverter delete(String keyspace, String tableName);

    /**
     * Method will return a RecordToBoundStatementConverter for an insert the supplied table.
     * @param tableName table to return the RecordToBoundStatementConverter for
     * @param topicConfigs class containing mapping details for the record
     * @return RecordToBoundStatementConverter that can be used for the record.
     */
    RecordToBoundStatementConverter insert(String keyspace, String tableName, TopicConfigs topicConfigs);

    /**
     * Method generates a BoundStatement, that inserts the offset metadata
     * for a given topic and partition.
     * @param topicPartition topic and partition for the offset
     * @param metadata offset metadata to be inserted
     * @return statement that inserts the provided offset to Scylla.
     */
    BoundStatement getInsertOffsetStatement(TopicPartition topicPartition, OffsetAndMetadata metadata);

    /**
     * Method is used to load offsets from storage in ScyllaDb
     * @param assignment The assignment of TopicPartitions that have been assigned to this task.
     * @return The offsets by TopicPartition based on the assignment.
     */
    Map<TopicPartition, Long> loadOffsets(String keyspace, Set<TopicPartition> assignment);

    /**
     * Callback that is fired when a table has changed.
     * @param keyspace Keyspace for the table change.
     * @param tableName Table name for the change.
     */
    void onTableChanged(String keyspace, String tableName);

}
