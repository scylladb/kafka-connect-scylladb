package io.connect.scylladb.codec;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * A {@link TypeCodec} that converts from {@link DataType#uuid()} to {@link String}.
 */
public class StringUuidCodec extends TypeCodec<String> {

  /**
   * A shared immutable instance.
   */
  public static final StringUuidCodec INSTANCE = new StringUuidCodec();

  protected final TypeCodec<UUID> uuidCodec;

  public StringUuidCodec() {
    this(DataType.uuid(), TypeCodec.uuid());
  }

  protected StringUuidCodec(DataType dataType, TypeCodec<UUID> baseCodec) {
    super(dataType, String.class);
    Objects.requireNonNull(baseCodec);
    this.uuidCodec = baseCodec;
  }

  protected UUID parseAsUuid(String value) {
    return uuidCodec.parse(value);
  }

  @Override
  public String parse(String value) {
    try {
      UUID uuid = parseAsUuid(value);
      return uuid != null ? uuid.toString() : null;
    } catch (InvalidTypeException e) {
      throw new InvalidTypeException(
          String.format("Cannot parse string UUID value from \"%s\"", value),
          e
      );
    }
  }

  @Override
  public String format(String value) {
    return value == null ? "NULL" : value;
  }

  @Override
  public ByteBuffer serialize(
      String value,
      ProtocolVersion protocolVersion
  ) throws InvalidTypeException {
    UUID uuid = parseAsUuid(value);
    return uuid == null ? null : uuidCodec.serialize(uuid, protocolVersion);
  }

  @Override
  public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    UUID uuid = uuidCodec.deserialize(bytes, protocolVersion);
    return uuid == null ? null : uuid.toString();
  }
}
