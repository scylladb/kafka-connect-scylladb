package io.connect.scylladb.codec;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

import java.math.BigInteger;

public class StringVarintCodec extends StringAbstractCodec<BigInteger> {
  public static final  StringVarintCodec INSTANCE = new StringVarintCodec();

  public StringVarintCodec() {
    super(DataTypes.VARINT, TypeCodecs.VARINT);
  }
}
