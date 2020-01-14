package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ScyllaDbSinkConnectorConfigTest {

  Map<String, String> settings;
  ScyllaDbSinkConnectorConfig config;

  @Before
  public void before() {
    settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "scylladb");
    config = null;
  }

  @Test
  public void shouldAcceptValidConfig() {
    settings.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, "9042");
    config = new ScyllaDbSinkConnectorConfig(settings);
    assertNotNull(config);
  }

  @Test
  public void shouldUseDefaults() {
    config = new ScyllaDbSinkConnectorConfig(settings);
    assertEquals(true, config.keyspaceCreateEnabled);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldNotAllowInvalidSSLProvide() {
    settings.put(ScyllaDbSinkConnectorConfig.SSL_PROVIDER_CONFIG, "DKJ");
    new ScyllaDbSinkConnectorConfig(settings);
  }

  //TODO: Add more tests
}