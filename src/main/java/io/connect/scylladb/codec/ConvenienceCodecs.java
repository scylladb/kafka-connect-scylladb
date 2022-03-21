package io.connect.scylladb.codec;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.MappingCodec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class ConvenienceCodecs {
    public static class ByteToBigintCodec extends MappingCodec<Byte, Long> {

        public static final ByteToBigintCodec INSTANCE = new ByteToBigintCodec();

        public ByteToBigintCodec() { super(TypeCodec.bigint(), Byte.class); }

        @Override
        protected Long serialize(Byte value) { return value.longValue(); }

        @Override
        protected Byte deserialize(Long value) { return value.byteValue(); }
    }

    public static class ShortToBigintCodec extends MappingCodec<Short, Long> {

        public static final ShortToBigintCodec INSTANCE = new ShortToBigintCodec();

        public ShortToBigintCodec() { super(TypeCodec.bigint(), Short.class); }

        @Override
        protected Long serialize(Short value) { return value.longValue(); }

        @Override
        protected Short deserialize(Long value) { return value.shortValue(); }
    }

    public static class IntegerToBigintCodec extends MappingCodec<Integer, Long> {

        public static final IntegerToBigintCodec INSTANCE = new IntegerToBigintCodec();

        public IntegerToBigintCodec() { super(TypeCodec.bigint(), Integer.class); }

        @Override
        protected Long serialize(Integer value) { return value.longValue(); }

        @Override
        protected Integer deserialize(Long value) { return value.intValue(); }
    }

    public static class FloatToDecimalCodec extends MappingCodec<Float, BigDecimal> {

        public static final FloatToDecimalCodec INSTANCE = new FloatToDecimalCodec();

        public FloatToDecimalCodec() { super(TypeCodec.decimal(), Float.class); }

        @Override
        protected BigDecimal serialize(Float value) { return new BigDecimal(value.toString()); }

        @Override
        protected Float deserialize(BigDecimal value) { return value.floatValue(); }
    }

    public static class DoubleToDecimalCodec extends MappingCodec<Double, BigDecimal> {

        public static final DoubleToDecimalCodec INSTANCE = new DoubleToDecimalCodec();

        public DoubleToDecimalCodec() { super(TypeCodec.decimal(), Double.class); }

        @Override
        protected BigDecimal serialize(Double value) { return new BigDecimal(value.toString()); }

        @Override
        protected Double deserialize(BigDecimal value) { return value.doubleValue(); }
    }

    public static class IntegerToDecimalCodec extends MappingCodec<Integer, BigDecimal> {

        public static final IntegerToDecimalCodec INSTANCE = new IntegerToDecimalCodec();

        public IntegerToDecimalCodec() { super(TypeCodec.decimal(), Integer.class); }

        @Override
        protected BigDecimal serialize(Integer value) { return new BigDecimal(value.toString()); }

        @Override
        protected Integer deserialize(BigDecimal value) { return value.intValueExact(); }
    }

    public static class LongToDecimalCodec extends MappingCodec<Long, BigDecimal> {

        public static final LongToDecimalCodec INSTANCE = new LongToDecimalCodec();

        public LongToDecimalCodec() { super(TypeCodec.decimal(), Long.class); }

        @Override
        protected BigDecimal serialize(Long value) { return new BigDecimal(value.toString()); }

        @Override
        protected Long deserialize(BigDecimal value) { return value.longValueExact(); }
    }

    public static class FloatToDoubleCodec extends MappingCodec<Float, Double> {

        public static final FloatToDoubleCodec INSTANCE = new FloatToDoubleCodec();

        public FloatToDoubleCodec() { super(TypeCodec.cdouble(), Float.class); }

        @Override
        protected Double serialize(Float value) { return Double.valueOf(value.toString()); }

        @Override
        protected Float deserialize(Double value) { return value.floatValue(); }
    }

    public static class ByteToIntCodec extends MappingCodec<Byte, Integer> {

        public static final ByteToIntCodec INSTANCE = new ByteToIntCodec();

        public ByteToIntCodec() { super(TypeCodec.cint(), Byte.class); }

        @Override
        protected Integer serialize(Byte value) { return Integer.valueOf(value); }

        @Override
        protected Byte deserialize(Integer value) { return value.byteValue(); }
    }

    public static class ShortToIntCodec extends MappingCodec<Short, Integer> {

        public static final ShortToIntCodec INSTANCE = new ShortToIntCodec();

        public ShortToIntCodec() { super(TypeCodec.cint(), Short.class); }

        @Override
        protected Integer serialize(Short value) { return Integer.valueOf(value); }

        @Override
        protected Short deserialize(Integer value) { return value.shortValue(); }
    }

    public static class LongToIntCodec extends MappingCodec<Long, Integer> {

        public static final LongToIntCodec INSTANCE = new LongToIntCodec();

        public LongToIntCodec() { super(TypeCodec.cint(), Long.class); }

        @Override
        protected Integer serialize(Long value) { return value.intValue(); }

        @Override
        protected Long deserialize(Integer value) { return value.longValue(); }
    }

    public static class StringToTinyintCodec extends MappingCodec<String, Byte> {

        public static final StringToTinyintCodec INSTANCE = new StringToTinyintCodec();

        public StringToTinyintCodec() { super(TypeCodec.tinyInt(), String.class); }

        @Override
        protected Byte serialize(String value) { return Byte.parseByte(value); }

        @Override
        protected String deserialize(Byte value) { return value.toString(); }
    }
    
    public static class StringToSmallintCodec extends MappingCodec<String, Short> {

        public static final StringToSmallintCodec INSTANCE = new StringToSmallintCodec();

        public StringToSmallintCodec() { super(TypeCodec.smallInt(), String.class); }

        @Override
        protected Short serialize(String value) { return Short.parseShort(value); }

        @Override
        protected String deserialize(Short value) { return value.toString(); }
    }

    public static class StringToIntCodec extends MappingCodec<String, Integer> {

        public static final StringToIntCodec INSTANCE = new StringToIntCodec();

        public StringToIntCodec() { super(TypeCodec.cint(), String.class); }

        @Override
        protected Integer serialize(String value) { return Integer.parseInt(value); }

        @Override
        protected String deserialize(Integer value) { return value.toString(); }
    }

    public static class StringToBigintCodec extends MappingCodec<String, Long> {

        public static final StringToBigintCodec INSTANCE = new StringToBigintCodec();

        public StringToBigintCodec() { super(TypeCodec.bigint(), String.class); }

        @Override
        protected Long serialize(String value) { return Long.parseLong(value); }

        @Override
        protected String deserialize(Long value) { return value.toString(); }
    }

    public static class ByteToSmallintCodec extends MappingCodec<Byte, Short> {

        public static final ByteToSmallintCodec INSTANCE = new ByteToSmallintCodec();

        public ByteToSmallintCodec() { super(TypeCodec.smallInt(), Byte.class); }

        @Override
        protected Short serialize(Byte value) { return value.shortValue(); }

        @Override
        protected Byte deserialize(Short value) { return value.byteValue(); }
    }

    public static class LongToVarintCodec extends MappingCodec<Long, BigInteger> {

        public static final LongToVarintCodec INSTANCE = new LongToVarintCodec();

        public LongToVarintCodec() { super(TypeCodec.varint(), Long.class); }

        @Override
        protected BigInteger serialize(Long value) { return BigInteger.valueOf(value); }

        @Override
        protected Long deserialize(BigInteger value) { return value.longValueExact(); }
    }

    public static class StringToTimestampCodec extends MappingCodec<String, Date> {

        public static final StringToTimestampCodec INSTANCE = new StringToTimestampCodec();

        public StringToTimestampCodec() { super(TypeCodec.timestamp(), String.class); }

        @Override
        protected Date serialize(String value) { return Date.from(Instant.parse(value)); }

        @Override
        protected String deserialize(Date value) { return value.toString(); }
    }
    
    public static final List<TypeCodec<?>> ALL_INSTANCES = new ArrayList<>(Arrays.asList(
            ByteToBigintCodec.INSTANCE,
            ShortToBigintCodec.INSTANCE,
            IntegerToBigintCodec.INSTANCE,
            FloatToDecimalCodec.INSTANCE,
            DoubleToDecimalCodec.INSTANCE,
            IntegerToDecimalCodec.INSTANCE,
            LongToDecimalCodec.INSTANCE,
            FloatToDoubleCodec.INSTANCE,
            ByteToIntCodec.INSTANCE,
            ShortToIntCodec.INSTANCE,
            LongToIntCodec.INSTANCE, //consider removing
            StringToTinyintCodec.INSTANCE,
            StringToSmallintCodec.INSTANCE,
            StringToIntCodec.INSTANCE,
            StringToBigintCodec.INSTANCE,
            ByteToSmallintCodec.INSTANCE,
            LongToVarintCodec.INSTANCE,
            StringToTimestampCodec.INSTANCE
    ));
}
