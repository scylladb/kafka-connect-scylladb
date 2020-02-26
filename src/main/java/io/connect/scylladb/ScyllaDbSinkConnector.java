package io.connect.scylladb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.connect.scylladb.utils.VersionUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.ImmutableMap;

/**
 * Sink connector class for Scylla Database.
 */
public class ScyllaDbSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSinkConnector.class);

  ScyllaDbSinkConnectorConfig config;
  
  /**
   * Start the Connector. The method will establish a ScyllaDB session 
   * and perform the following:<ol>
   * <li>Create a keyspace with the name under <code>scylladb.keyspace</code> configuration, 
   * if <code>scylladb.keyspace.create.enabled</code> is set to true.
   * <li>Create a table for managing offsets in ScyllaDB with the name under 
   * <code>scylladb.offset.storage.table</code> configuration, 
   * if <code>scylladb.offset.storage.table.enable</code> is set to true.
   * </ol>
   */
  @Override
  public void start(Map<String, String> settings) {
    
    config = new ScyllaDbSinkConnectorConfig(settings);
    ScyllaDbSessionFactory sessionFactory = new ScyllaDbSessionFactory();

    try (ScyllaDbSession session = sessionFactory.newSession(config)) {

      if (config.keyspaceCreateEnabled) {
        KeyspaceOptions createKeyspace = SchemaBuilder.createKeyspace(config.keyspace)
                .ifNotExists()
                .with()
                .durableWrites(true)
                .replication(ImmutableMap.of(
                        "class", (Object) "SimpleStrategy",
                        "replication_factor", config.keyspaceReplicationFactor
                ));
        session.executeStatement(createKeyspace);
      }
      if (config.offsetEnabledInScyllaDB) {
        final Create createOffsetStorage = SchemaBuilder
                .createTable(config.keyspace, config.offsetStorageTable)
                .addPartitionKey("topic", DataType.varchar())
                .addPartitionKey("partition", DataType.cint())
                .addColumn("offset", DataType.bigint())
                .ifNotExists();
        session.executeStatement(createOffsetStorage);
      }

    } catch (IOException ex) {
      //TODO: improve error handling for both cases
      throw new ConnectException("Failed to start the Connector.", ex);
    }
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();
    taskProps.putAll(config.originalsStrings());
    for (int i = 0; i < maxTasks; i++) {
      configs.add(taskProps);
    }
    return configs;
  }

  @Override
  public void stop() {
    log.info("Stopping ScyllaDB Sink Connector.");
  }

  @Override
  public ConfigDef config() {
    return ScyllaDbSinkConnectorConfig.config();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return ScyllaDbSinkTask.class;
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }
}