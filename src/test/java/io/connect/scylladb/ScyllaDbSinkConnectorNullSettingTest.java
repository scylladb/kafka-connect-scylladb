package io.connect.scylladb;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * An explicit null fails validation with the setting named, instead of escaping validate() as an exception.
 */
@RunWith(Parameterized.class)
public class ScyllaDbSinkConnectorNullSettingTest {

  @Parameterized.Parameters(name = "{0}")
  public static List<String> settings() {
    return Arrays.asList(
            "topic.t.ks.t1.mapping",
            "topic.t.ks.t1.consistencyLevel",
            "topic.t.ks.t1.ttlSeconds",
            "topic.t.ks.t1.deletesEnabled",
            "topic.t.ks.t1.timestampField",
            "topic.t.ks.t1.timestampUnit",
            "topic.t.ks.t1.deletesField",
            "topic.t.ks.t1.deletesValues",
            ScyllaDbSinkConnectorConfig.DELETES_VALUES_CONFIG);
  }

  @Parameterized.Parameter
  public String setting;

  @Test
  public void shouldReportNullSettingByName() {
    final Map<String, String> settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    // Valid delete settings, so that each null is the only error.
    settings.put("topic.t.ks.t1.deletesField", "value.op");
    settings.put("topic.t.ks.t1.deletesValues", "d");
    settings.put(setting, null);

    final List<String> errors = new ScyllaDbSinkConnector().validate(settings).configValues().stream()
            .flatMap(value -> value.errorMessages().stream())
            .collect(Collectors.toList());

    assertTrue(errors.toString(), errors.stream()
            .anyMatch(m -> m.contains("Invalid value null for configuration " + setting + ":")));
  }
}
