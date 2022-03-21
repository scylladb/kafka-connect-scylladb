package io.connect.scylladb.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.TypeCodec;

public class StringDurationCodec extends StringAbstractCodec<Duration> {

    public static final StringDurationCodec INSTANCE = new StringDurationCodec();

    public StringDurationCodec() {
        super(DataType.duration(), TypeCodec.duration());
    }
}
