package io.connect.scylladb.integration.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import io.connect.scylladb.codec.StringTimeUuidCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StringTimeUuidCodecTest {

  private static final String NON_UUID_STR = "some-non-uuid-string";

  private static final UUID UUID_V1 = UUID.fromString("5fc03087-d265-11e7-b8c6-83e29cd24f4c");
  private static final UUID UUID_V4 = UUID.randomUUID();
  private static final String UUID_STR1 = UUID_V1.toString();
  private static final String UUID_STR2 = UUID_V4.toString();

  private StringTimeUuidCodec codec;

  @BeforeEach
  void setUp() {
    codec = new StringTimeUuidCodec();
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  public void shouldBeUuidType1() {
    assertEquals(1, UUID_V1.version());
  }

  @Test
  public void shouldFormatNullString() {
    assertEquals("NULL", codec.format(null));
  }

  @Test
  public void shouldFormatEmptyString() {
    assertEquals("", codec.format(""));
  }

  @Test
  public void shouldFormatUuidString() {
    assertEquals(UUID_STR1, codec.format(UUID_STR1));
    assertEquals(UUID_STR2, codec.format(UUID_STR2));
  }

  @Test
  public void shouldFormatNonUuidString() {
    assertEquals(NON_UUID_STR, codec.format(NON_UUID_STR));
  }

  @Test
  public void shouldSerializeAndDeserializeNullString() {
    assertSerializeAndDeserialize(null);
  }

  @Test
  public void shouldSerializeAndDeserializeUuidType1Strings() {
    assertSerializeAndDeserialize(UUID_STR1);
  }

  @Test
  public void shouldFailToSerializeNonType1Strings() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      assertSerializeAndDeserialize(UUID_STR2);
    });
  }

  @Test
  public void shouldFailToSerializeNonUuidString() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      codec.encode(NON_UUID_STR, ProtocolVersion.V4);
    });
  }

  protected void assertSerializeAndDeserialize(String uuidStr) {
    ByteBuffer buffer = codec.encode(uuidStr, ProtocolVersion.V4);
    String deserialized = codec.decode(buffer, ProtocolVersion.V4);
    assertEquals(uuidStr, deserialized);
  }
}
