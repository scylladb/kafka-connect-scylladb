package io.connect.scylladb.codec;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

import java.net.InetAddress;

public class StringInetCodec extends StringAbstractCodec<InetAddress> {
  public static final StringInetCodec INSTANCE = new StringInetCodec();
  public StringInetCodec() {
    super(DataTypes.INET, TypeCodecs.INET);
  }
}
