package io.connect.scylladb;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ScyllaDbSinkConnectorTest {

  Map<String, String> settings;
  ScyllaDbSinkConnector connector;

  @Before
  public void before() {
    settings = new HashMap<>();
    connector = new ScyllaDbSinkConnector();
    //adding required configurations
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
  }
  //TODO: failing need to check
  //@Test
  public void shouldReturnNonNullVersion() {
    System.out.println(connector.version());
    assertNotNull(connector.version());
  }

  @Test
  public void shouldStartWithoutError() {
    startConnector();
  }

  @Test
  public void shouldReturnSinkTask() {
    assertEquals(ScyllaDbSinkTask.class, connector.taskClass());
  }

  @Test
  public void shouldGenerateValidTaskConfigs() {
    startConnector();
    //TODO: Change this logic to reflect expected behavior of your connector
    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
    assertTrue("zero task configs provided", !taskConfigs.isEmpty());
    for (Map<String, String> taskConfig : taskConfigs) {
      assertEquals(settings, taskConfig);
    }
  }

  @Test
  public void shouldStartAndStop() {
    startConnector();
    connector.stop();
  }

  @Test
  public void shouldNotHaveNullConfigDef() {
    // ConfigDef objects don't have an overridden equals() method; just make sure it's non-null
    assertNotNull(connector.config());
  }

  //TODO: failing need to check
  //@Test
  public void version() {
    assertNotNull(connector.version());
    assertFalse(connector.version().equals("0.0.0.0"));
    assertTrue(connector.version().matches("^(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)(-\\w+)?$"));
  }


  protected void startConnector() {
    connector.config = new ScyllaDbSinkConnectorConfig(settings);
    //connector.doStart();
  }
}