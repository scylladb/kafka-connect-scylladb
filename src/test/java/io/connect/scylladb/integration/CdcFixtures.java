package io.connect.scylladb.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Loads the recorded change-stream messages under {@code src/test/resources/cdc} as sink records,
 * converted the way a worker with {@code JsonConverter} and {@code schemas.enable=false} would.
 */
final class CdcFixtures {

  public static final String JSON = "json";
  public static final String FLAT_JSON = "flat-json";

  /** Kafka timestamp of the first message of a case; each later message is one second later. */
  public static final long BASE_TIMESTAMP = 1_700_000_000_000L;

  public static final List<String> CASES = Collections.unmodifiableList(Arrays.asList(
      "01-create",
      "02-full-update",
      "03-field-removed",
      "04-field-removed-changed-only",
      "05-field-added",
      "06-durable-delete",
      "07-delete",
      "08-key-not-stored",
      "09-collection-element-ops",
      "10-hyperloglog",
      "11-geojson",
      "12-expiry-default",
      "13-expiry-shipped",
      "14-truncate",
      "15-scalar-types"));

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CdcFixtures() {
  }

  public static List<SinkRecord> load(String format, String caseName, String topic) {
    String resource = "/cdc/" + format + "/" + caseName + ".json";
    JsonNode messages;
    try (InputStream in = CdcFixtures.class.getResourceAsStream(resource)) {
      if (in == null) {
        throw new IllegalArgumentException("No fixture " + resource);
      }
      messages = MAPPER.readTree(in).get("messages");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    JsonConverter keyConverter = converter(true);
    JsonConverter valueConverter = converter(false);
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < messages.size(); i++) {
      JsonNode message = messages.get(i);
      SchemaAndValue key = keyConverter.toConnectData(topic, bytes(message.get("key")));
      SchemaAndValue value = valueConverter.toConnectData(topic, bytes(message.get("value")));
      records.add(new SinkRecord(topic, 0, key.schema(), key.value(), value.schema(), value.value(),
          i, BASE_TIMESTAMP + i * 1000L, TimestampType.CREATE_TIME));
    }
    return records;
  }

  private static JsonConverter converter(boolean isKey) {
    JsonConverter converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", "false"), isKey);
    return converter;
  }

  private static byte[] bytes(JsonNode raw) {
    return raw.isNull() ? null : raw.asText().getBytes(StandardCharsets.UTF_8);
  }
}
