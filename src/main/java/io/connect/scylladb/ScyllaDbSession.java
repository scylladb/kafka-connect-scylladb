package io.connect.scylladb;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;

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
    TableMetadata.Table tableMetadata(String tableName);

    /**
     * Check if a table exists.
     * @param tableName name of the table
     */
    boolean tableExists(String tableName);

    /**
     * Ensure that a table has a specified schema.
     * @param tableName name of the table
     * @param keySchema schema that will be used for the primary key.
     * @param valueSchema schema that will be used for the rest of the table.
     */
    void createOrAlterTable(String tableName, Schema keySchema, Schema valueSchema);

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
    RecordToBoundStatementConverter delete(String tableName);

    /**
     * Method will return a RecordToBoundStatementConverter for an insert the supplied table.
     * @param tableName table to return the RecordToBoundStatementConverter for
     * @return RecordToBoundStatementConverter that can be used for the record.
     */
    RecordToBoundStatementConverter insert(String tableName);

    /**
     * Method is used to add prepared statements for the offsets that are in the current batch.
     * @param batch statement batch that will be written to ScyllaDb
     * @param offsetStates The list of SinkOffsetStates for the current batch of SinkRecords.
     */
    void addOffsetsToBatch(BatchStatement batch, Map<TopicPartition, OffsetAndMetadata> offsetStates);

    /**
     * Method is used to load offsets from storage in ScyllaDb
     * @param assignment The assignment of TopicPartitions that have been assigned to this task.
     * @return The offsets by TopicPartition based on the assignment.
     */
    Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> assignment);

    /**
     * Callback that is fired when a table has changed.
     * @param keyspace Keyspace for the table change.
     * @param tableName Table name for the change.
     */
    void onTableChanged(String keyspace, String tableName);

}
