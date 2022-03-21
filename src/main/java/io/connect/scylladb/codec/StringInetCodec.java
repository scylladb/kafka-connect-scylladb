package io.connect.scylladb.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;

import java.net.InetAddress;

public class StringInetCodec extends StringAbstractCodec<InetAddress> {

    public static final StringInetCodec INSTANCE = new StringInetCodec();

    public StringInetCodec() {
        super(DataType.inet(), TypeCodec.inet());
    }
}
