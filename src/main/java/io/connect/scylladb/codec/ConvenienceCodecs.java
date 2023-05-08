package io.connect.scylladb.codec;

import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class ConvenienceCodecs {
  public static class ByteToBigintCodec extends MappingCodec<Long, Byte> {

    public static final ByteToBigintCodec INSTANCE = new ByteToBigintCodec();

    public ByteToBigintCodec() { super(TypeCodecs.BIGINT, GenericType.BYTE); }

    @Nullable
    @Override
    protected Byte innerToOuter(@Nullable Long value) {
      return value == null ? null : value.byteValue();
    }

    @Nullable
    @Override
    protected Long outerToInner(@Nullable Byte value) {
      return value == null ? null : value.longValue();
    }
  }

  public static class ShortToBigintCodec extends MappingCodec<Long, Short> {

    public static final ShortToBigintCodec INSTANCE = new ShortToBigintCodec();

    public ShortToBigintCodec() { super(TypeCodecs.BIGINT, GenericType.SHORT); }

    @Nullable
    @Override
    protected Short innerToOuter(@Nullable Long value) {
      return value == null ? null : value.shortValue();
    }

    @Nullable
    @Override
    protected Long outerToInner(@Nullable Short value) {
      return value == null ? null : value.longValue();
    }
  }

  public static class IntegerToBigintCodec extends MappingCodec<Long, Integer> {

    public static final IntegerToBigintCodec INSTANCE = new IntegerToBigintCodec();

    public IntegerToBigintCodec() { super(TypeCodecs.BIGINT, GenericType.INTEGER); }

    @Nullable
    @Override
    protected Integer innerToOuter(@Nullable Long value) {
      return value == null ? null : value.intValue();
    }

    @Nullable
    @Override
    protected Long outerToInner(@Nullable Integer value) {
      return value == null ? null : value.longValue();
    }
  }

  public static class FloatToDecimalCodec extends MappingCodec<BigDecimal, Float> {

    public static final FloatToDecimalCodec INSTANCE = new FloatToDecimalCodec();

    public FloatToDecimalCodec() { super(TypeCodecs.DECIMAL, GenericType.FLOAT); }

    @Nullable
    @Override
    protected Float innerToOuter(@Nullable BigDecimal value) {
      return value == null ? null : value.floatValue();
    }

    @Nullable
    @Override
    protected BigDecimal outerToInner(@Nullable Float value) {
      return value == null ? null : BigDecimal.valueOf(value.doubleValue());
    }
  }

  public static class DoubleToDecimalCodec extends MappingCodec<BigDecimal, Double> {

    public static final DoubleToDecimalCodec INSTANCE = new DoubleToDecimalCodec();

    public DoubleToDecimalCodec() { super(TypeCodecs.DECIMAL, GenericType.DOUBLE); }

    @Nullable
    @Override
    protected Double innerToOuter(@Nullable BigDecimal value) {
      return value == null ? null : value.doubleValue();
    }

    @Nullable
    @Override
    protected BigDecimal outerToInner(@Nullable Double value) {
      return value == null ? null : BigDecimal.valueOf(value);
    }
  }

  public static class IntegerToDecimalCodec extends MappingCodec<BigDecimal, Integer> {

    public static final IntegerToDecimalCodec INSTANCE = new IntegerToDecimalCodec();

    public IntegerToDecimalCodec() { super(TypeCodecs.DECIMAL, GenericType.INTEGER); }

    @Nullable
    @Override
    protected Integer innerToOuter(@Nullable BigDecimal value) {
      return value == null ? null : value.intValue();
    }

    @Nullable
    @Override
    protected BigDecimal outerToInner(@Nullable Integer value) {
      return value == null ? null : BigDecimal.valueOf(value.longValue());
    }
  }

  public static class LongToDecimalCodec extends MappingCodec<BigDecimal, Long> {

    public static final LongToDecimalCodec INSTANCE = new LongToDecimalCodec();

    public LongToDecimalCodec() { super(TypeCodecs.DECIMAL, GenericType.LONG); }

    @Nullable
    @Override
    protected Long innerToOuter(@Nullable BigDecimal value) {
      return value == null ? null : value.longValueExact();
    }

    @Nullable
    @Override
    protected BigDecimal outerToInner(@Nullable Long value) {
      return value == null ? null : BigDecimal.valueOf(value);
    }
  }

  public static class FloatToDoubleCodec extends MappingCodec<Double, Float> {

    public static final FloatToDoubleCodec INSTANCE = new FloatToDoubleCodec();

    public FloatToDoubleCodec() { super(TypeCodecs.DOUBLE, GenericType.FLOAT); }

    @Nullable
    @Override
    protected Float innerToOuter(@Nullable Double value) {
      return value == null ? null : value.floatValue();
    }

    @Nullable
    @Override
    protected Double outerToInner(@Nullable Float value) {
      return value == null ? null : value.doubleValue();
    }
  }

  public static class ByteToIntCodec extends MappingCodec<Integer, Byte> {

    public static final ByteToIntCodec INSTANCE = new ByteToIntCodec();

    public ByteToIntCodec() { super(TypeCodecs.INT, GenericType.BYTE); }

    @Nullable
    @Override
    protected Byte innerToOuter(@Nullable Integer value) {
      return value == null ? null : value.byteValue();
    }

    @Nullable
    @Override
    protected Integer outerToInner(@Nullable Byte value) {
      return value == null ? null : value.intValue();
    }
  }

  public static class ShortToIntCodec extends MappingCodec<Integer, Short> {

    public static final ShortToIntCodec INSTANCE = new ShortToIntCodec();

    public ShortToIntCodec() { super(TypeCodecs.INT, GenericType.SHORT); }

    @Nullable
    @Override
    protected Short innerToOuter(@Nullable Integer value) {
      return value == null ? null : value.shortValue();
    }

    @Nullable
    @Override
    protected Integer outerToInner(@Nullable Short value) {
      return value == null ? null : value.intValue();
    }
  }

  public static class LongToIntCodec extends MappingCodec<Integer, Long> {

    public static final LongToIntCodec INSTANCE = new LongToIntCodec();

    public LongToIntCodec() { super(TypeCodecs.INT, GenericType.LONG); }

    @Nullable
    @Override
    protected Long innerToOuter(@Nullable Integer value) {
      return value == null ? null : value.longValue();
    }

    @Nullable
    @Override
    protected Integer outerToInner(@Nullable Long value) {
      return value == null ? null : value.intValue();
    }
  }

  public static class StringToTinyintCodec extends MappingCodec<Byte, String> {

    public static final StringToTinyintCodec INSTANCE = new StringToTinyintCodec();

    public StringToTinyintCodec() { super(TypeCodecs.TINYINT, GenericType.STRING); }

    @Nullable
    @Override
    protected String innerToOuter(@Nullable Byte value) {
      return value == null ? null : value.toString();
    }

    @Nullable
    @Override
    protected Byte outerToInner(@Nullable String value) {
      return value == null ? null : Byte.parseByte(value);
    }
  }

  public static class StringToSmallintCodec extends MappingCodec<Short, String> {

    public static final StringToSmallintCodec INSTANCE = new StringToSmallintCodec();

    public StringToSmallintCodec() { super(TypeCodecs.SMALLINT, GenericType.STRING); }

    @Nullable
    @Override
    protected String innerToOuter(@Nullable Short value) {
      return value == null ? null : value.toString();
    }

    @Nullable
    @Override
    protected Short outerToInner(@Nullable String value) {
      return value == null ? null : Short.parseShort(value);
    }
  }

  public static class StringToIntCodec extends MappingCodec<Integer, String> {

    public static final StringToIntCodec INSTANCE = new StringToIntCodec();

    public StringToIntCodec() { super(TypeCodecs.INT, GenericType.STRING); }

    @Nullable
    @Override
    protected String innerToOuter(@Nullable Integer value) {
      return value == null ? null : value.toString();
    }

    @Nullable
    @Override
    protected Integer outerToInner(@Nullable String value) {
      return value == null ? null : Integer.parseInt(value);
    }
  }

  public static class StringToBigintCodec extends MappingCodec<Long, String> {

    public static final StringToBigintCodec INSTANCE = new StringToBigintCodec();

    public StringToBigintCodec() { super(TypeCodecs.BIGINT, GenericType.STRING); }

    @Nullable
    @Override
    protected String innerToOuter(@Nullable Long value) {
      return value == null ? null : value.toString();
    }

    @Nullable
    @Override
    protected Long outerToInner(@Nullable String value) {
      return value == null ? null : Long.parseLong(value);
    }
  }

  public static class ByteToSmallintCodec extends MappingCodec<Short, Byte> {

    public static final ByteToSmallintCodec INSTANCE = new ByteToSmallintCodec();

    public ByteToSmallintCodec() { super(TypeCodecs.SMALLINT, GenericType.BYTE); }

    @Nullable
    @Override
    protected Byte innerToOuter(@Nullable Short value) {
      return value == null ? null : value.byteValue();
    }

    @Nullable
    @Override
    protected Short outerToInner(@Nullable Byte value) {
      return value == null ? null : value.shortValue();
    }
  }

  public static class LongToVarintCodec extends MappingCodec<BigInteger, Long> {

    public static final LongToVarintCodec INSTANCE = new LongToVarintCodec();

    public LongToVarintCodec() { super(TypeCodecs.VARINT, GenericType.LONG); }

    @Nullable
    @Override
    protected Long innerToOuter(@Nullable BigInteger value) {
      return value == null ? null : value.longValue();
    }

    @Nullable
    @Override
    protected BigInteger outerToInner(@Nullable Long value) {
      return value == null ? null : BigInteger.valueOf(value);
    }
  }

  public static class StringToTimestampCodec extends MappingCodec<Instant, String> {

    public static final StringToTimestampCodec INSTANCE = new StringToTimestampCodec();

    public StringToTimestampCodec() { super(TypeCodecs.TIMESTAMP, GenericType.STRING); }

    @Nullable
    @Override
    protected String innerToOuter(@Nullable Instant value) {
      return value == null ? null : value.toString();
    }

    @Nullable
    @Override
    protected Instant outerToInner(@Nullable String value) {
      return value == null ? null : Instant.parse(value);
    }
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
      LongToIntCodec.INSTANCE,
      StringToTinyintCodec.INSTANCE,
      StringToSmallintCodec.INSTANCE,
      StringToIntCodec.INSTANCE,
      StringToBigintCodec.INSTANCE,
      ByteToSmallintCodec.INSTANCE,
      LongToVarintCodec.INSTANCE,
      StringToTimestampCodec.INSTANCE
  ));
}
