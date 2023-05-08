package io.connect.scylladb.codec;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

public class StringDurationCodec extends StringAbstractCodec<CqlDuration> {
  public static final StringDurationCodec INSTANCE = new StringDurationCodec();

  public StringDurationCodec() {
    super(DataTypes.DURATION, TypeCodecs.DURATION);
  }
}
