package io.connect.scylladb.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import java.math.BigInteger;

public class StringVarintCodec extends StringAbstractCodec<BigInteger> {
    public static final StringVarintCodec INSTANCE = new StringVarintCodec();

    public StringVarintCodec() {
        super(DataType.varint(), TypeCodec.varint());
    }
}
