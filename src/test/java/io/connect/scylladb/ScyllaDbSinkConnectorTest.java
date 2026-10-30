package io.connect.scylladb;

import org.apache.kafka.common.config.ConfigValue;
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
import static org.mockito.Mockito.never;
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
  public void shouldReportConflictingTopicTargetsAsValidationError() {
    settings.put("topic.a.b-c.ks1.t1.mapping", "id=key.id");
    settings.put("topic.a.b-c.ks1.t2.ttlSeconds", "3600");
    ConfigValue keyspace = connector.validate(settings).configValues().stream()
        .filter(v -> v.name().equals(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG))
        .findFirst().get();
    assertTrue(keyspace.errorMessages().toString(),
        keyspace.errorMessages().stream().anyMatch(m -> m.contains("two different targets")));
  }

  @Test
  public void shouldCheckQuotedTopicKeyspaceDifferingOnlyInCase() {
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "\"ks\"");
    settings.put("topic.t.\"KS\".t1.ttlSeconds", "3600");
    settings.put("topic.u.KS.t1.ttlSeconds", "3600");
    ScyllaDbSession session = mock(ScyllaDbSession.class);

    List<String> errors = ScyllaDbSinkConnector.missingTopicKeyspaces(session, new ScyllaDbSinkConnectorConfig(settings));

    // "KS" is a different keyspace from "ks", while unquoted KS is the same one.
    assertEquals(errors.toString(), 1, errors.size());
    verify(session).keyspaceExists("\"KS\"");
    verify(session, never()).keyspaceExists("KS");
  }

  @Test
  public void shouldCheckDefaultKeyspaceTargetWhenNotCreated() {
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG, "false");
    settings.put("topic.t.scylladb.t1.ttlSeconds", "3600");
    ScyllaDbSession session = mock(ScyllaDbSession.class);

    List<String> errors = ScyllaDbSinkConnector.missingTopicKeyspaces(session, new ScyllaDbSinkConnectorConfig(settings));

    assertEquals(errors.toString(), 1, errors.size());
    verify(session).keyspaceExists("scylladb");
  }

  @Test
  public void shouldNotCheckDefaultKeyspaceTargetWhenCreated() {
    settings.put("topic.t.scylladb.t1.ttlSeconds", "3600");
    ScyllaDbSession session = mock(ScyllaDbSession.class);

    List<String> errors = ScyllaDbSinkConnector.missingTopicKeyspaces(session, new ScyllaDbSinkConnectorConfig(settings));

    assertTrue(errors.toString(), errors.isEmpty());
    verify(session, never()).keyspaceExists("scylladb");
  }

  @Test
  public void shouldReportInvalidTargetNameAsValidationError() {
    settings.put("topic.t.ks1.a-b.ttlSeconds", "3600");
    ConfigValue keyspace = connector.validate(settings).configValues().stream()
        .filter(v -> v.name().equals(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG))
        .findFirst().get();
    assertTrue(keyspace.errorMessages().toString(),
        keyspace.errorMessages().stream().anyMatch(m -> m.contains("topic.t.ks1.a-b.ttlSeconds")));
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