package io.connect.scylladb.integration.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import io.connect.scylladb.codec.StringUuidCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StringUuidCodecTest {

  private static final String NON_UUID_STR = "some-non-uuid-string";

  private static final String UUID_STR1 = UUID.randomUUID().toString();
  private static final String UUID_STR2 = UUID.randomUUID().toString();
  private static final String UUID_STR3 = UUID.randomUUID().toString();

  private StringUuidCodec codec;

  @BeforeEach
  void setUp() {
    codec = new StringUuidCodec();
  }

  @AfterEach
  void tearDown() {
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
    assertEquals(UUID_STR3, codec.format(UUID_STR3));
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
  public void shouldSerializeAndDeserializeUuidStrings() {
    assertSerializeAndDeserialize(UUID_STR1);
    assertSerializeAndDeserialize(UUID_STR2);
    assertSerializeAndDeserialize(UUID_STR3);
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
