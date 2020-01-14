package io.connect.scylladb.integration;

import static io.connect.scylladb.ScyllaDbSinkConnectorConfig.PORT_CONFIG;
import static io.connect.scylladb.ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG;
import static io.connect.scylladb.ScyllaDbSinkConnectorConfig.OFFSET_STORAGE_TABLE_CONF;
import static io.connect.scylladb.ScyllaDbSinkConnectorConfig.KEYSPACE_REPLICATION_FACTOR_CONFIG;
import static io.connect.scylladb.ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG;
import static io.connect.scylladb.ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

  protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);

  protected EmbeddedConnectCluster connect;

  protected void startConnect() throws IOException {
    connect = new EmbeddedConnectCluster.Builder()
        .name("my-connect-cluster")
        .build();

    // start the clusters
    connect.start();

    //TODO: Start proxy or external system
  }

  protected void stopConnect() {
    // stop all Connect, Kafka and Zk threads.
    connect.stop();
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
   * name to start the specified number of tasks.
   *
   * @param name the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @return the time this method discovered the connector has started, in milliseconds past epoch
   * @throws InterruptedException if this was interrupted
   */
  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time."
    );
    return System.currentTimeMillis();
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks the minimum number of tasks
   * @return true if the connector and tasks are in RUNNING state; false otherwise
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
                       && info.tasks().size() >= numTasks
                       && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                       && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.error("Could not check connector state info.", e);
      return Optional.empty();
    }
  }

  protected Map<String, String> getCommonConfiguration() {
    // setup up props for the sink connector
    Map<String, String> props = new HashMap<>();

    props.put(PORT_CONFIG, "9042");
    props.put(KEYSPACE_CONFIG, "testing");
    props.put(OFFSET_STORAGE_TABLE_CONF, "kafka_connect_offsets");
    props.put(KEYSPACE_REPLICATION_FACTOR_CONFIG, "1");
    props.put(KEYSPACE_CREATE_ENABLED_CONFIG, "true");
    props.put(CONTACT_POINTS_CONFIG, "172.17.0.2");

    return props;
  }
}
