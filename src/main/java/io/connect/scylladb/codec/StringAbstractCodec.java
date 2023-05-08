package io.connect.scylladb.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Objects;

public class StringAbstractCodec<T> implements TypeCodec<String> {
  protected final TypeCodec<T> TCodec;
  protected final DataType dataType;

  protected StringAbstractCodec(DataType dataType, TypeCodec<T> baseCodec) {
    Objects.requireNonNull(baseCodec);
    this.TCodec = baseCodec;
    this.dataType = dataType;
  }

  protected T parseAsT(String value) {
    return TCodec.parse(value);
  }

  @Override
  public String parse(String value) {
    try {
      T t = parseAsT(value);
      return t != null ? t.toString() : null;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Cannot parse string value from \"%s\"", value), e);
    }
  }

  @NonNull
  @Override
  public GenericType<String> getJavaType() {
    return GenericType.STRING;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return dataType;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable String value, @NonNull ProtocolVersion protocolVersion) {
    T t = parseAsT(value);
    return t == null ? null : TCodec.encode(t, protocolVersion);
  }

  @Nullable
  @Override
  public String decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    T t = TCodec.decode(bytes, protocolVersion);
    return t == null ? null : t.toString();
  }

  @NonNull
  @Override
  public String format(String value) {
    return value == null ? "NULL" : value;
  }
}
