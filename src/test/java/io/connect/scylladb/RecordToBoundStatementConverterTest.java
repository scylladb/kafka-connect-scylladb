package io.connect.scylladb;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import io.connect.scylladb.codec.ConvenienceCodecs;
import io.connect.scylladb.utils.ScyllaDbConstants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecordToBoundStatementConverterTest {

  private PreparedStatement preparedStatement;
  private BoundStatement boundStatement;
  private ColumnDefinitions definitions;

  @Before
  public void before() {
    MutableCodecRegistry registry = new DefaultCodecRegistry("test");
    registry.register(ConvenienceCodecs.ALL_INSTANCES);
    preparedStatement = mock(PreparedStatement.class);
    boundStatement = mock(BoundStatement.class);
    definitions = mock(ColumnDefinitions.class);
    when(preparedStatement.getVariableDefinitions()).thenReturn(definitions);
    when(definitions.firstIndexOf(anyString())).thenReturn(-1);
    when(boundStatement.codecRegistry()).thenReturn(registry);
    when(boundStatement.set(anyInt(), any(), any(TypeCodec.class))).thenReturn(boundStatement);
  }

  @Test
  public void convertsSchemalessListOfMapsToListOfUdtValues() {
    UserDefinedType udtType = new UserDefinedTypeBuilder(
        CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("system_sid"))
        .withField(CqlIdentifier.fromInternal("sid"), DataTypes.listOf(DataTypes.TEXT))
        .withField(CqlIdentifier.fromInternal("system"), DataTypes.TEXT)
        .frozen()
        .build();
    column(0, "systems_sids", DataTypes.listOf(udtType));

    Map<String, Object> first = new LinkedHashMap<>();
    first.put("sid", Collections.emptyList());
    first.put("system", "s1");
    Map<String, Object> second = new LinkedHashMap<>();
    second.put("sid", Collections.emptyList());
    second.put("system", "s2");

    List<?> bound = (List<?>) bind("systems_sids", Arrays.asList(first, second));

    assertEquals(2, bound.size());
    assertEquals("s1", ((UdtValue) bound.get(0)).getString("system"));
    assertEquals(Collections.emptyList(), ((UdtValue) bound.get(0)).getList("sid", String.class));
    assertEquals("s2", ((UdtValue) bound.get(1)).getString("system"));
  }

  @Test
  public void bindsByTheResolvedColumnIndex() {
    column(3, "tags", DataTypes.listOf(DataTypes.TEXT));

    assertEquals(Arrays.asList("a", "b"), bind("tags", Arrays.asList("a", "b")));
    verify(boundStatement).set(eq(3), any(), any(TypeCodec.class));
  }

  @Test
  public void convertsJsonIntegersToTheElementType() {
    column(0, "counts", DataTypes.listOf(DataTypes.INT));

    assertEquals(Arrays.asList(1, 2), bind("counts", Arrays.asList(1L, 2L)));
  }

  @Test
  public void convertsWholeNumbersInDoubleLists() {
    column(0, "coordinates", DataTypes.listOf(DataTypes.DOUBLE));

    assertEquals(Arrays.asList(13.0, 52.5), bind("coordinates", Arrays.asList(13L, 52.5)));
  }

  @Test
  public void convertsMapKeysAndValues() {
    column(0, "attrs", DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("k1", 1L);

    assertEquals(Collections.singletonMap("k1", 1), bind("attrs", value));
  }

  @Test
  public void rejectsMapKeysThatConvertToTheSameValue() {
    column(0, "names", DataTypes.mapOf(DataTypes.INT, DataTypes.TEXT));
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("01", "a");
    value.put("1", "b");

    DataException e = assertThrows(DataException.class, () -> bind("names", value));
    assertTrue(e.getMessage(), e.getMessage().contains("'names' has more than one key that converts to 1"));
  }

  @Test
  public void bindsByteArraysInBlobLists() {
    column(0, "chunks", DataTypes.listOf(DataTypes.BLOB));

    assertEquals(Collections.singletonList(ByteBuffer.wrap(new byte[] {0, -1, 16})),
        bind("chunks", Collections.singletonList(new byte[] {0, -1, 16})));
  }

  @Test
  public void bindsByteArraysAsBlobMapValues() {
    column(0, "parts", DataTypes.mapOf(DataTypes.TEXT, DataTypes.BLOB));

    assertEquals(Collections.singletonMap("p1", ByteBuffer.wrap(new byte[] {1, 2})),
        bind("parts", Collections.singletonMap("p1", new byte[] {1, 2})));
  }

  @Test
  public void bindsByteArraysInBlobUdtFields() {
    UserDefinedType udtType = new UserDefinedTypeBuilder(
        CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("attachment"))
        .withField(CqlIdentifier.fromInternal("data"), DataTypes.BLOB)
        .build();
    column(0, "attachment", udtType);

    UdtValue bound = (UdtValue) bind("attachment", Collections.singletonMap("data", new byte[] {1, 2}));
    assertEquals(ByteBuffer.wrap(new byte[] {1, 2}), bound.getByteBuffer("data"));
  }

  @Test
  public void bindsDatesInTimestampLists() {
    // Connect Timestamp, Date and Time values in a Struct array or map all arrive as java.util.Date.
    column(0, "seen", DataTypes.listOf(DataTypes.TIMESTAMP));

    assertEquals(Collections.singletonList(Instant.ofEpochMilli(1_700_000_000_000L)),
        bind("seen", Collections.singletonList(new Date(1_700_000_000_000L))));
  }

  @Test
  public void bindsDatesInDateSets() {
    column(0, "days", DataTypes.setOf(DataTypes.DATE));

    // Noon UTC is still 1 January 1970 in time zones up to 12 hours from UTC.
    assertEquals(Collections.singleton(LocalDate.of(1970, 1, 1)),
        bind("days", Collections.singletonList(new Date(43_200_000L))));
  }

  @Test
  public void bindsDatesAsTimeMapValues() {
    column(0, "opens", DataTypes.mapOf(DataTypes.TEXT, DataTypes.TIME));

    assertEquals(Collections.singletonMap("mon", LocalTime.of(9, 30)),
        bind("opens", Collections.singletonMap("mon", new Date(34_200_000L))));
  }

  @Test
  public void rejectsUdtKeysThatDifferOnlyInCase() {
    column(0, "systems_sids", DataTypes.listOf(systemSidType()));
    Map<String, Object> sid = new LinkedHashMap<>();
    sid.put("system", "s1");
    sid.put("SYSTEM", "s2");

    DataException e = assertThrows(DataException.class,
        () -> bind("systems_sids", Collections.singletonList(sid)));
    assertTrue(e.getMessage(), e.getMessage().contains("'systems_sids' has more than one key for UDT field 'system'"));
  }

  @Test
  public void namesTheUdtFieldInNestedErrors() {
    column(0, "systems_sids", DataTypes.listOf(systemSidType()));

    DataException e = assertThrows(DataException.class,
        () -> bind("systems_sids", Collections.singletonList(Collections.singletonMap("system", true))));
    assertTrue(e.getMessage(), e.getMessage().contains("Field 'systems_sids.system' expects CQL type TEXT"));
  }

  @Test
  public void decodesBase64StringsIntoBlobs() {
    column(0, "chunks", DataTypes.listOf(DataTypes.BLOB));

    assertEquals(Collections.singletonList(ByteBuffer.wrap(new byte[] {0, -1, 16})),
        bind("chunks", Collections.singletonList("AP8Q")));
  }

  @Test
  public void rejectsInvalidBase64() {
    column(0, "chunks", DataTypes.listOf(DataTypes.BLOB));

    DataException e = assertThrows(DataException.class, () -> bind("chunks", Collections.singletonList("not base64!")));
    assertTrue(e.getMessage(), e.getMessage().contains("'chunks' value cannot be converted"));
    assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void rejectsIntegersOutsideTheIntRange() {
    column(0, "counts", DataTypes.listOf(DataTypes.INT));

    DataException e = assertThrows(DataException.class, () -> bind("counts", Arrays.asList(1L, 3000000000L)));
    assertTrue(e.getMessage(), e.getMessage().contains("'counts' value cannot be converted"));
    assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void rejectsInt64FieldsOutsideTheIntRange() {
    // Records with a schema bind INT64 fields through setLong, which reaches the same int codec.
    when(preparedStatement.bind()).thenReturn(boundStatement);
    when(boundStatement.allIndicesOf("total")).thenReturn(Collections.singletonList(0));
    when(boundStatement.getType(0)).thenReturn(DataTypes.INT);
    when(boundStatement.protocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    when(boundStatement.setLong(anyString(), anyLong())).thenCallRealMethod();
    when(boundStatement.setLong(anyInt(), anyLong())).thenCallRealMethod();
    when(boundStatement.set(anyInt(), any(), any(TypeCodec.class))).thenCallRealMethod();
    when(boundStatement.setBytesUnsafe(anyInt(), any())).thenReturn(boundStatement);

    convertTotal(42L);
    verify(boundStatement).setBytesUnsafe(0, TypeCodecs.INT.encode(42, ProtocolVersion.DEFAULT));

    DataException e = assertThrows(DataException.class, () -> convertTotal(3000000000L));
    assertTrue(e.getMessage(), e.getMessage().contains("'total'"));
    assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof IllegalArgumentException);
  }

  @Test
  public void rejectsNullCollectionElements() {
    column(0, "tags", DataTypes.listOf(DataTypes.TEXT));
    column(1, "attrs", DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT));

    DataException list = assertThrows(DataException.class, () -> bind("tags", Arrays.asList("a", null)));
    assertTrue(list.getMessage(), list.getMessage().contains("'tags' contains a null element"));
    DataException map = assertThrows(DataException.class,
        () -> bind("attrs", Collections.singletonMap("k1", null)));
    assertTrue(map.getMessage(), map.getMessage().contains("'attrs' contains a null element"));
  }

  @Test
  public void writesObjectsInTextListsAsJson() {
    column(0, "systems_sids", DataTypes.listOf(DataTypes.TEXT));
    Map<String, Object> sid = new LinkedHashMap<>();
    sid.put("sid", Collections.emptyList());
    sid.put("system", "s1");

    assertEquals(Collections.singletonList("{\"sid\":[],\"system\":\"s1\"}"),
        bind("systems_sids", Collections.singletonList(sid)));
  }

  @Test
  public void bindsColumnsCaseInsensitively() {
    ColumnDefinition tags = mock(ColumnDefinition.class);
    when(tags.getName()).thenReturn(CqlIdentifier.fromInternal("tags"));
    ColumnDefinition counts = mock(ColumnDefinition.class);
    when(counts.getName()).thenReturn(CqlIdentifier.fromInternal("counts"));
    when(counts.getType()).thenReturn(DataTypes.listOf(DataTypes.INT));
    ColumnDefinitions byName = DefaultColumnDefinitions.valueOf(Arrays.asList(tags, counts));
    when(preparedStatement.getVariableDefinitions()).thenReturn(byName);

    assertEquals(Arrays.asList(1, 2), bind("Counts", Arrays.asList(1L, 2L)));
    verify(boundStatement).set(eq(1), any(), any(TypeCodec.class));
  }

  @Test
  public void bindsArraysToSets() {
    column(0, "labels", DataTypes.setOf(DataTypes.TEXT));

    assertEquals(new LinkedHashSet<>(Arrays.asList("a", "b")), bind("labels", Arrays.asList("a", "b", "a")));
  }

  @Test
  public void writesObjectsToTextColumnsAsJson() {
    column(0, "loc", DataTypes.TEXT);
    Map<String, Object> point = new LinkedHashMap<>();
    point.put("type", "Point");
    point.put("coordinates", Arrays.asList(13.4, 52.5));

    assertEquals("{\"type\":\"Point\",\"coordinates\":[13.4,52.5]}", bind("loc", point));
  }

  @Test
  public void rejectsElementsWithoutAMatchingCodec() {
    column(0, "flags", DataTypes.listOf(DataTypes.BOOLEAN));

    DataException e = assertThrows(DataException.class, () -> bind("flags", Arrays.asList(1.5)));
    assertTrue(e.getMessage(), e.getMessage().contains("'flags'"));
  }

  @Test
  public void rejectsUnknownColumns() {
    DataException e = assertThrows(DataException.class, () -> bind("missing", Arrays.asList("a")));
    assertTrue(e.getMessage(), e.getMessage().contains("'missing'"));
  }

  private static UserDefinedType systemSidType() {
    return new UserDefinedTypeBuilder(
        CqlIdentifier.fromInternal("ks"), CqlIdentifier.fromInternal("system_sid"))
        .withField(CqlIdentifier.fromInternal("sid"), DataTypes.listOf(DataTypes.TEXT))
        .withField(CqlIdentifier.fromInternal("system"), DataTypes.TEXT)
        .frozen()
        .build();
  }

  private void column(int index, String name, DataType type) {
    ColumnDefinition definition = mock(ColumnDefinition.class);
    when(definition.getType()).thenReturn(type);
    when(definitions.firstIndexOf(name)).thenReturn(index);
    when(definitions.get(index)).thenReturn(definition);
  }

  private void convertTotal(long total) {
    Schema schema = SchemaBuilder.struct().field("total", Schema.INT64_SCHEMA).build();
    SinkRecord record = new SinkRecord("topic", 0, null, null, schema, new Struct(schema).put("total", total), 0);
    new RecordToBoundStatementConverter(preparedStatement).convert(record, null, ScyllaDbConstants.INSERT_OPERATION);
  }

  /** Converts {@code value} as a schemaless field and returns what was bound. */
  private Object bind(String fieldName, Object value) {
    RecordToBoundStatementConverter converter = new RecordToBoundStatementConverter(preparedStatement);
    RecordToBoundStatementConverter.State state = new RecordToBoundStatementConverter.State(boundStatement);
    if (value instanceof List) {
      converter.setArray(state, fieldName, null, (List<?>) value);
    } else {
      converter.setMap(state, fieldName, null, (Map<?, ?>) value);
    }
    ArgumentCaptor<Object> bound = ArgumentCaptor.forClass(Object.class);
    verify(boundStatement).set(anyInt(), bound.capture(), any(TypeCodec.class));
    return bound.getValue();
  }
}
