package io.connect.scylladb.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class StringAbstractCodec<T> extends TypeCodec<String> {

    protected final TypeCodec<T> TCodec;

    protected StringAbstractCodec(DataType dataType, TypeCodec<T> baseCodec) {
        super(dataType, String.class);
        Objects.requireNonNull(baseCodec);
        this.TCodec = baseCodec;
    }

    protected T parseAsT(String value) {
        return TCodec.parse(value);
    }

    @Override
    public String parse(String value) {
        try {
            T t = parseAsT(value);
            return t != null ? t.toString() : null;
        } catch (InvalidTypeException e) {
            throw new InvalidTypeException(
                    String.format("Cannot parse string value from \"%s\"", value),
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
        T t = parseAsT(value);
        return t == null ? null : TCodec.serialize(t, protocolVersion);
    }

    @Override
    public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        T t = TCodec.deserialize(bytes, protocolVersion);
        return t == null ? null : t.toString();
    }
}
