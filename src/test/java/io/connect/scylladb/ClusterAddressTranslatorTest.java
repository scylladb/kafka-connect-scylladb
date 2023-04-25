package io.connect.scylladb;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;

public class ClusterAddressTranslatorTest {

  @Test
  public void setMapTest() throws JsonProcessingException {
    ClusterAddressTranslator addressTranslator = new ClusterAddressTranslator();
    final String ADDRESS_MAP_STRING = "{\"10.0.24.69:9042\": \"sl-eu-lon-2-portal.3.dblayer.com:15227\", "
        + "\"10.0.24.71:9042\": \"sl-eu-lon-2-portal.2.dblayer.com:15229\", "
        + "\"10.0.24.70:9042\": \"sl-eu-lon-2-portal.1.dblayer.com:15228\"}"; // String from CONTACT_POINTS_DOC
    addressTranslator.setMap(ADDRESS_MAP_STRING);
  }
}
