package io.connect.scylladb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.connect.scylladb.utils.VersionUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        CreateKeyspace createKeyspace = SchemaBuilder.createKeyspace(config.keyspace)
                .ifNotExists()
                .withReplicationOptions(ImmutableMap.of(
                "class", (Object) "SimpleStrategy",
                "replication_factor", config.keyspaceReplicationFactor
                  ))
            .withDurableWrites(true);
        session.executeStatement(createKeyspace.build());
      }
      if (config.offsetEnabledInScyllaDB) {
        final CreateTable createOffsetStorage = SchemaBuilder
                .createTable(config.keyspace, config.offsetStorageTable)
                .ifNotExists()
                .withPartitionKey("topic", DataTypes.TEXT)
                .withPartitionKey("partition", DataTypes.INT)
                .withColumn("offset", DataTypes.BIGINT);
        session.executeStatement(createOffsetStorage.build());
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

  @Override
  public Config validate(Map<String, String> connectorConfigs){
    // Do the usual validation, field by field.
    Config result = super.validate(connectorConfigs);

    ConfigValue userConfig = getConfigValue(result, ScyllaDbSinkConnectorConfig.USERNAME_CONFIG);
    ConfigValue passwordConfig = getConfigValue(result, ScyllaDbSinkConnectorConfig.PASSWORD_CONFIG);
    if (userConfig.value() == null && passwordConfig.value() != null) {
      userConfig.addErrorMessage("Username not provided, even though password is.");
    }
    if (userConfig.value() != null && passwordConfig.value() == null) {
      userConfig.addErrorMessage("Password not provided, even though username is.");
    }

    ConfigValue securityEnable = getConfigValue(result, ScyllaDbSinkConnectorConfig.SECURITY_ENABLE_CONFIG);
    if (Objects.equals(securityEnable.value(), true)) {
      if (userConfig.value() == null) {
        userConfig.addErrorMessage("Username is required with security enabled.");
      }
      if (passwordConfig.value() == null) {
        passwordConfig.addErrorMessage("Password is required with security enabled.");
      }
    }

    boolean hasErrors = result.configValues().stream().anyMatch(c -> !(c.errorMessages().isEmpty()) );
    if (!hasErrors)
    {
      config = new ScyllaDbSinkConnectorConfig(connectorConfigs);
      // Make trial connection
      ScyllaDbSessionFactory sessionFactory = new ScyllaDbSessionFactory();
      try (ScyllaDbSession session = sessionFactory.newSession(config)) {
        if (!session.isValid()) {
          ConfigValue contactPoints = getConfigValue(result, ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG);
          contactPoints.addErrorMessage("Session is invalid.");
        } else {
          ConfigValue keyspaceCreateConfig = getConfigValue(result, ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG);
          ConfigValue keyspaceConfig = getConfigValue(result, ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG);
          if (config.keyspaceCreateEnabled) {
            if (keyspaceConfig.value() == null) {
              // This is possibly not needed, since keyspace should be always required non-null.
              keyspaceConfig.addErrorMessage("Scylla keyspace name is required when keyspace creation is enabled.");
            }
          } else {
            // Check if keyspace exists:
            if (!session.keyspaceExists(config.keyspace)) {
              keyspaceCreateConfig.addErrorMessage("Seems provided keyspace is not present. Did you mean to set "
                                                   + ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG
                                                   + " to true?");
              keyspaceConfig.addErrorMessage("Keyspace " + config.keyspace + " not found.");
            }
          }
        }
      } catch (Exception ex) {
        ConfigValue contactPoints = getConfigValue(result, ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG);
        contactPoints.addErrorMessage("Failed to establish trial session: " + ex.getMessage());
      }
    }
    return result;
  }

  private ConfigValue getConfigValue(Config config, String configName){
    return config.configValues().stream()
            .filter(value -> value.name().equals(configName) )
            .findFirst().get();
  }
}