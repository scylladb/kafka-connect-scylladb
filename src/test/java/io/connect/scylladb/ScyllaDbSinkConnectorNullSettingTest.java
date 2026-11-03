package io.connect.scylladb;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
    // Every per-topic setting, so that one added later is covered too.
    final List<String> settings = ScyllaDbSinkConnectorConfig.TOPIC_SETTINGS.stream()
            .map(suffix -> "topic.t.ks.t1." + suffix)
            .collect(Collectors.toList());
    settings.add(ScyllaDbSinkConnectorConfig.DELETES_VALUES_CONFIG);
    return settings;
  }

  @Parameterized.Parameter
  public String setting;

  @Test
  public void shouldReportNullSettingByName() {
    final Map<String, String> settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    // Valid delete and expiry settings, so that each null is the only error.
    settings.put("topic.t.ks.t1.deletesField", "value.op");
    settings.put("topic.t.ks.t1.deletesValues", "d");
    settings.put("topic.t.ks.t1.mapping", "id=key.id, __expiration=value.exp");
    settings.put("topic.t.ks.t1.expirationUnit", "SECONDS");
    settings.put(setting, null);

    final List<String> errors = new ScyllaDbSinkConnector().validate(settings).configValues().stream()
            .flatMap(value -> value.errorMessages().stream())
            .collect(Collectors.toList());

    assertTrue(errors.toString(), errors.stream()
            .anyMatch(m -> m.contains("Invalid value null for configuration " + setting + ":")));
  }
}
