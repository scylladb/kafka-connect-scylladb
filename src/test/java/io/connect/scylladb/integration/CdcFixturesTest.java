package io.connect.scylladb.integration;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CdcFixturesTest {

  // The operations each case recorded, in topic order; both layouts carry the same sequence.
  // A durable delete is a delete whose metadata has "durable": true.
  private static final Map<String, List<String>> OPERATIONS = new LinkedHashMap<>();

  static {
    OPERATIONS.put("01-create", Arrays.asList("write"));
    OPERATIONS.put("02-full-update", Arrays.asList("write", "write"));
    OPERATIONS.put("03-field-removed", Arrays.asList("write", "write", "write"));
    OPERATIONS.put("04-field-removed-changed-only", Arrays.asList("write", "write", "write"));
    OPERATIONS.put("05-field-added", Arrays.asList("write", "write"));
    OPERATIONS.put("06-durable-delete", Arrays.asList("write", "durable delete"));
    OPERATIONS.put("07-delete", Arrays.asList("write", "delete"));
    OPERATIONS.put("08-key-not-stored", Arrays.asList("write", "durable delete"));
    OPERATIONS.put("09-collection-element-ops", Arrays.asList("write", "write", "write"));
    OPERATIONS.put("10-hyperloglog", Arrays.asList("write"));
    OPERATIONS.put("11-geojson", Arrays.asList("write"));
    OPERATIONS.put("12-expiry-default", Arrays.asList("write"));
    OPERATIONS.put("13-expiry-shipped", Arrays.asList("write", "delete"));
    OPERATIONS.put("14-truncate", Arrays.asList("write"));
    OPERATIONS.put("15-scalar-types", Arrays.asList("write"));
  }

  @Test
  public void casesMatchTheResourceFiles() throws URISyntaxException {
    for (String format : Arrays.asList(CdcFixtures.JSON, CdcFixtures.FLAT_JSON)) {
      File dir = new File(CdcFixtures.class.getResource("/cdc/" + format).toURI());
      List<String> files = Arrays.stream(dir.list())
          .map(name -> name.replaceFirst("\\.json$", ""))
          .sorted()
          .collect(Collectors.toList());
      assertEquals(format, CdcFixtures.CASES, files);
    }
  }

  @Test
  public void everyCaseHasItsRecordedOperations() {
    assertEquals(CdcFixtures.CASES, new ArrayList<>(OPERATIONS.keySet()));
    for (String format : Arrays.asList(CdcFixtures.JSON, CdcFixtures.FLAT_JSON)) {
      for (String caseName : CdcFixtures.CASES) {
        List<String> operations = CdcFixtures.load(format, caseName, "t").stream()
            .map(record -> operation(format, record))
            .collect(Collectors.toList());
        assertEquals(format + "/" + caseName, OPERATIONS.get(caseName), operations);
      }
    }
  }

  private static String operation(String format, SinkRecord record) {
    Map<?, ?> value = (Map<?, ?>) record.value();
    Map<?, ?> metadata = CdcFixtures.JSON.equals(format) ? value : (Map<?, ?>) value.get("metadata");
    String msg = (String) metadata.get("msg");
    return Boolean.TRUE.equals(metadata.get("durable")) ? "durable " + msg : msg;
  }

  @Test
  public void jsonKeysAreListsAndValuesAreMaps() {
    for (String caseName : CdcFixtures.CASES) {
      for (SinkRecord record : CdcFixtures.load(CdcFixtures.JSON, caseName, "t")) {
        assertTrue(caseName, record.key() instanceof List);
        assertTrue(caseName, record.value() instanceof Map);
        assertNull(caseName, record.valueSchema());
      }
    }
  }

  @Test
  public void flatJsonKeysAndValuesAreMaps() {
    for (String caseName : CdcFixtures.CASES) {
      for (SinkRecord record : CdcFixtures.load(CdcFixtures.FLAT_JSON, caseName, "t")) {
        assertTrue(caseName, record.key() instanceof Map);
        assertTrue(caseName, record.value() instanceof Map);
        assertTrue(caseName, ((Map<?, ?>) record.value()).get("metadata") instanceof Map);
      }
    }
  }

  @Test
  public void recordsCarryTopicOffsetsAndTimestamps() {
    List<SinkRecord> records = CdcFixtures.load(CdcFixtures.FLAT_JSON, "03-field-removed", "t");
    assertEquals(3, records.size());
    for (int i = 0; i < records.size(); i++) {
      assertEquals("t", records.get(i).topic());
      assertEquals(i, records.get(i).kafkaOffset());
      assertEquals(CdcFixtures.BASE_TIMESTAMP + i * 1000L, (long) records.get(i).timestamp());
    }
  }
}
