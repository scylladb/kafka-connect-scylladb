package io.connect.scylladb.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.util.UUID;

/**
 * A {@link TypeCodec} that converts from {@link DataType#timeuuid()} to {@link String}.
 */
public class StringTimeUuidCodec extends StringUuidCodec {

  /**
   * A shared immutable instance.
   */
  public static final StringTimeUuidCodec INSTANCE = new StringTimeUuidCodec();

  public StringTimeUuidCodec() {
    super(DataType.timeuuid(), timeUUID());
  }

  @Override
  protected UUID parseAsUuid(String value) {
    UUID uuid = super.parseAsUuid(value);
    if (uuid != null && uuid.version() != 1) {
      throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", value));
    }
    return uuid;
  }
}
