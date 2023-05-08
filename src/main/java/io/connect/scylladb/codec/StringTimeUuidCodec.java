package io.connect.scylladb.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.UUID;

public class StringTimeUuidCodec implements TypeCodec<String> {
  public static final StringTimeUuidCodec INSTANCE = new StringTimeUuidCodec();

  @NonNull
  @Override
  public GenericType<String> getJavaType() {
    return GenericType.STRING;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.TIMEUUID;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable String value, @NonNull ProtocolVersion protocolVersion) {
    UUID uuid = TypeCodecs.TIMEUUID.parse(value);
    return uuid == null ? null : TypeCodecs.TIMEUUID.encode(uuid, protocolVersion);
  }

  @Nullable
  @Override
  public String decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    UUID uuid = TypeCodecs.TIMEUUID.decode(bytes, protocolVersion);
    return uuid == null ? null : uuid.toString();
  }

  @NonNull
  @Override
  public String format(@Nullable String value) {
    return value == null ? "NULL" : value;
  }

  @Nullable
  @Override
  public String parse(@Nullable String value) {
    try {
      UUID uuid = TypeCodecs.TIMEUUID.parse(value);
      return uuid != null ? uuid.toString() : null;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Cannot parse string TIMEUUID value from \"%s\"", value), e);
    }

  }
}
