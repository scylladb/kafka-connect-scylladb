package io.connect.scylladb.integration;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.connect.scylladb.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.test.IntegrationTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.connect.scylladb.integration.SinkRecordUtil.write;
import static io.connect.scylladb.integration.TestDataUtil.asMap;
import static io.connect.scylladb.integration.TestDataUtil.struct;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@Category(IntegrationTest.class)
public class ScyllaDbSinkConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSinkConnectorIT.class);

  static final String SCYLLA_DB_CONTACT_POINT = "172.20.0.3";
  static final int SCYLLA_DB_PORT = 9042;
  static final String SCYLLADB_KEYSPACE = "testkeyspace";
  private static final String SCYLLADB_OFFSET_TABLE = "kafka_connect_offsets";
  private ScyllaDbSinkConnector connector;

  static Cluster.Builder clusterBuilder() {
    Cluster.Builder clusterBuilder = Cluster.builder()
            .withPort(SCYLLA_DB_PORT)
            .addContactPoints(SCYLLA_DB_CONTACT_POINT)
            .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED);
    return clusterBuilder;
  }

  static Map<String, String> settings() {
    Map<String, String> result = new LinkedHashMap<>();
    result.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, SCYLLADB_KEYSPACE);
    result.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG, "true");
    result.put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, SCYLLA_DB_CONTACT_POINT);
    result.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(SCYLLA_DB_PORT));
    result.put(ScyllaDbSinkConnectorConfig.KEYSPACE_REPLICATION_FACTOR_CONFIG, "1");
    return result;
  }

  @BeforeAll
  public static void setupKeyspace() throws InterruptedException {
    Cluster.Builder builder = clusterBuilder();

    while (true) {
      try (Cluster cluster = builder.build()) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        assertTrue(stopwatch.elapsed(TimeUnit.SECONDS) <= 30, "30 seconds have elapsed before creating keyspace.");
        try (Session session = cluster.connect()) {
          session.execute("SELECT cql_version FROM system.local");
          break;
        }
      } catch (NoHostAvailableException ex) {
        log.debug("Exception thrown.", ex);
        Thread.sleep(1000);
      }
    }
  }

  ScyllaDbSinkTask task;
  SinkTaskContext sinkTaskContext;
  List<RowValidator> validations;

  @BeforeEach
  public void start() {
    this.task = new ScyllaDbSinkTask();
    this.sinkTaskContext = mock(SinkTaskContext.class);
    this.task.initialize(this.sinkTaskContext);
    this.validations = new ArrayList<>();
  }

  @AfterEach
  public void stop() {
    if (!this.validations.isEmpty()) {
      try (Cluster cluster = clusterBuilder().build()) {
        try (Session session = cluster.connect(SCYLLADB_KEYSPACE)) {
          for (RowValidator validation : validations) {
            assertRow(session, validation);
          }
        }
      }
    }
    String query = "DROP TABLE" + " " + SCYLLADB_OFFSET_TABLE;
    if (IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE)) {
      execute(query);
    }
    this.task.stop();
    this.connector.stop();
  }

  @Test
  public void insert() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "insertTesting";
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(settings);
    List<SinkRecord> records = ImmutableList.of(
            write(
                    topic,
                    struct("key",
                            "id", Schema.Type.INT64, true, 12345L
                    ), struct("key",
                            "id", Schema.Type.INT64, true, 12345L,
                            "firstName", Schema.Type.STRING, true, "test",
                            "lastName", Schema.Type.STRING, true, "user",
                            "age", Schema.Type.INT64, true, 12234L
                    )
            ),
            write(topic,
                    null,
                    asMap(
                            struct("key",
                                    "id", Schema.Type.INT64, true, 67890L
                            )
                    ),
                    null,
                    asMap(
                            struct("key",
                                    "id", Schema.Type.INT64, true, 67890L,
                                    "firstName", Schema.Type.STRING, true, "another",
                                    "lastName", Schema.Type.STRING, true, "user",
                                    "age", Schema.Type.INT64, true, 12324L
                            )
                    )
            )
    );
    this.validations = records.stream()
            .map(RowValidator::of)
            .collect(Collectors.toList());
    this.task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(1)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  public void decimal() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);

    final long keyValue = 1234L;
    final Struct key = struct("key",
            "id", Schema.Type.INT64, true, keyValue
    );
    List<SinkRecord> records = new ArrayList<>();
    Set<TopicPartition> assignment = new HashSet<>();

    for (int i = 0; i < 10; i++) {
      final String topic = String.format("decimalTesting", i);
      final BigDecimal decimalValue = BigDecimal.valueOf(123456789123L, i);

      Schema valueSchema = SchemaBuilder.struct()
              .field("id", Schema.INT64_SCHEMA)
              .field("value", Decimal.schema(i))
              .build();
      Struct value = new Struct(valueSchema)
              .put("id", keyValue)
              .put("value", decimalValue);

      records.add(
              write(
                      topic,
                      key,
                      value
              )
      );
      assignment.add(
              new TopicPartition(topic, 1)
      );
      this.validations.add(
              RowValidator.of(topic, key, value)
      );
    }

    when(this.sinkTaskContext.assignment()).thenReturn(assignment);
    this.task.start(settings);
    this.task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(1)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  public void timestamp() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "timestampTesting";

    List<SinkRecord> records = new ArrayList<>();
    Set<TopicPartition> assignment = ImmutableSet.of(new TopicPartition(topic, 1));

    List<Date> timestamps = Arrays.asList(
            new Date(0L),
            new Date(-911520000000L),
            new Date(323398984123L)
    );

    long index = 0L;
    final Schema valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("value", Timestamp.SCHEMA)
            .build();

    for (final Date date : timestamps) {
      index++;
      final Struct key = struct("key",
              "id", Schema.Type.INT64, true, index
      );
      final Struct value = new Struct(valueSchema)
              .put("id", index)
              .put("value", date);

      records.add(
              write(
                      topic,
                      key,
                      value
              )
      );
    }
    this.validations = records.stream()
            .map(RowValidator::of)
            .collect(Collectors.toList());
    when(this.sinkTaskContext.assignment()).thenReturn(assignment);
    this.task.start(settings);
    this.task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(1)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  public void date() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "dateTesting";

    List<SinkRecord> records = new ArrayList<>();
    Set<TopicPartition> assignment = ImmutableSet.of(new TopicPartition(topic, 1));

    List<Date> timestamps = Arrays.asList(
            new Date(0L),
            new Date(-911520000000L),
            new Date(2051222400000L)
    );

    long index = 0L;
    final Schema valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("value", org.apache.kafka.connect.data.Date.SCHEMA)
            .build();

    for (final Date date : timestamps) {
      index++;
      final Struct key = struct("key",
              "id", Schema.Type.INT64, true, index
      );
      final Struct value = new Struct(valueSchema)
              .put("id", index)
              .put("value", date);

      records.add(
              write(
                      topic,
                      key,
                      value
              )
      );

      this.validations.add(
              new RowValidator(
                      topic,
                      ImmutableMap.of("id", index),
                      ImmutableMap.of("id", index, "value", LocalDate.fromMillisSinceEpoch(date.getTime()))
              )
      );
    }

    when(this.sinkTaskContext.assignment()).thenReturn(assignment);
    this.task.start(settings);
    this.task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(1)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  public void time() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "timeTesting";

    List<SinkRecord> records = new ArrayList<>();
    Set<TopicPartition> assignment = ImmutableSet.of(new TopicPartition(topic, 1));

    List<Date> timestamps = Arrays.asList(
            new Date(0L),
            new Date(60L * 60L * 1000L)
    );

    long index = 0L;
    final Schema valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("value", Time.SCHEMA)
            .build();

    for (final Date date : timestamps) {
      index++;
      final Struct key = struct("key",
              "id", Schema.Type.INT64, true, index
      );
      final Struct value = new Struct(valueSchema)
              .put("id", index)
              .put("value", date);

      records.add(
              write(
                      topic,
                      key,
                      value
              )
      );

      final long nanoseconds = TimeUnit.MILLISECONDS.convert(date.getTime(), TimeUnit.NANOSECONDS);
      this.validations.add(
              new RowValidator(
                      topic,
                      ImmutableMap.of("id", index),
                      ImmutableMap.of("id", index, "value", nanoseconds)
              )
      );
    }

    when(this.sinkTaskContext.assignment()).thenReturn(assignment);
    this.task.start(settings);
    this.task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(1)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  public void uuidWithCreateTable() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);

    final long keyValue = 1234L;
    final Struct key = struct("key",
            "id", Schema.Type.INT64, true, keyValue
    );
    List<SinkRecord> records = new ArrayList<>();
    Set<TopicPartition> assignment = new HashSet<>();

    final String topic = "uuidTestingCreateTable";
    final String uuidStr = UUID.randomUUID().toString();

    Schema valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("value", Schema.STRING_SCHEMA)
            .build();
    Struct value = new Struct(valueSchema)
            .put("id", keyValue)
            .put("value", uuidStr);

    records.add(
            write(
                    topic,
                    key,
                    value
            )
    );
    assignment.add(
            new TopicPartition(topic, 1)
    );
    this.validations.add(
            RowValidator.of(topic, key, value)
    );

    when(this.sinkTaskContext.assignment()).thenReturn(assignment);
    this.task.start(settings);
    this.task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(1)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  public void uuidWithExistingTable() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);

    final long keyValue = 1234L;
    final Struct key = struct("key",
            "id", Schema.Type.INT64, true, keyValue
    );
    List<SinkRecord> records = new ArrayList<>();
    Set<TopicPartition> assignment = new HashSet<>();

    final String topic = "uuidTestingExistingTable";
    final String uuidStr = UUID.randomUUID().toString();
    final UUID timeUuid = UUID.fromString("5fc03087-d265-11e7-b8c6-83e29cd24f4c");
    assertEquals(1, timeUuid.version());

    Schema valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("uuidStr", Schema.STRING_SCHEMA)
            .field("uuidValue", Schema.STRING_SCHEMA)
            .field("timeuuidValue", Schema.STRING_SCHEMA)
            .build();
    Struct value = new Struct(valueSchema)
            .put("id", keyValue)
            .put("uuidStr", uuidStr)
            .put("uuidValue", uuidStr)
            .put("timeuuidValue", timeUuid.toString());

    records.add(
            write(
                    topic,
                    key,
                    value
            )
    );
    assignment.add(
            new TopicPartition(topic, 1)
    );
    this.validations.add(
            RowValidator.of(topic, key, value)
    );

    execute("CREATE TABLE testkeyspace." + topic + "("
            + "id bigint,"
            + "uuidStr varchar,"
            + "uuidValue uuid,"
            + "timeuuidValue timeuuid,"
            + "PRIMARY KEY(id)"
            + ")");

    when(this.sinkTaskContext.assignment()).thenReturn(assignment);
    this.task.start(settings);
    this.task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(1)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  public void deleteMissingTable() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "deleteMissingTable";
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(settings);
    task.put(
            ImmutableList.of(
                    SinkRecordUtil.delete(
                            topic,
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L
                            )
                    )
            )
    );
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  public void deleteNullValueRow() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "deleteNullValueRowTesting";
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(settings);

    task.put(
            ImmutableList.of(
                    write(
                            topic,
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L
                            ),
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L,
                                    "firstName", Schema.Type.STRING, true, "test",
                                    "lastName", Schema.Type.STRING, true, "user"
                            )
                    )
            )
    );

    List<SinkRecord> records = ImmutableList.of(
            SinkRecordUtil.delete(
                    topic,
                    struct("key",
                            "id", Schema.Type.INT64, true, 12345L
                    )
            )
    );

    this.task.put(records);
    this.validations = records.stream()
            .map(RowValidator::of)
            .collect(Collectors.toList());
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(2)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  public void tableExistsNoChange() throws IOException {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "tableExistsNoChange";
    final TopicPartition topicPartition = new TopicPartition(topic, 1);
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(topicPartition));

    this.task.start(settings);
    task.put(
            ImmutableList.of(
                    write(
                            topic,
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L
                            ),
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L,
                                    "firstName", Schema.Type.STRING, true, "test",
                                    "lastName", Schema.Type.STRING, true, "user"
                            )
                    )
            )
    );
    this.task.stop();
    this.task.start(settings);
    task.put(
            ImmutableList.of(
                    write(
                            topic,
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L
                            ),
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L,
                                    "firstName", Schema.Type.STRING, true, "test",
                                    "lastName", Schema.Type.STRING, true, "user"
                            )
                    )
            )
    );
  }

  @Test
  public void tableExistsAlter() throws IOException {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "tableExistsAlter";
    final TopicPartition topicPartition = new TopicPartition(topic, 1);
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(topicPartition));

    this.task.start(settings);
    task.put(
            ImmutableList.of(
                    write(
                            topic,
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L
                            ),
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L,
                                    "firstName", Schema.Type.STRING, true, "test",
                                    "lastName", Schema.Type.STRING, true, "user"
                            )
                    )
            )
    );
    this.task.stop();
    this.task.start(settings);
    task.put(
            ImmutableList.of(
                    write(
                            topic,
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L
                            ),
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L,
                                    "firstName", Schema.Type.STRING, true, "test",
                                    "lastName", Schema.Type.STRING, true, "user",
                                    "city", Schema.Type.STRING, true, "Austin",
                                    "state", Schema.Type.STRING, true, "TX"
                            )
                    )
            )
    );
  }

  @Test
  public void tableMissingManageTableDisabled() throws IOException {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);

    final String topic = "tableMissingManageTableDisabled";
    final TopicPartition topicPartition = new TopicPartition(topic, 1);
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(topicPartition));
    settings.put(ScyllaDbSinkConnectorConfig.TABLE_MANAGE_ENABLED_CONFIG, "false");
    this.task.start(settings);

    DataException ex = assertThrows(DataException.class, () -> {
      this.task.put(
              ImmutableList.of(
                      write(
                              topic,
                              struct("key",
                                      "id", Schema.Type.INT64, true, 12345L
                              ),
                              struct("key",
                                      "id", Schema.Type.INT64, true, 12345L,
                                      "firstName", Schema.Type.STRING, true, "test",
                                      "lastName", Schema.Type.STRING, true, "user",
                                      "city", Schema.Type.STRING, true, "Austin",
                                      "state", Schema.Type.STRING, true, "TX"
                              )
                      )
              )
      );
    });
    log.info("Exception", ex);
    assertTrue(
            ex.getMessage().contains("CREATE TABLE testkeyspace.tableMissingManageTableDisabled"),
            "Exception message should contain create statement."
    );
  }

  @Test
  public void tableExistsAlterManageTableDisabled() throws IOException {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "tableExistsAlterManageTableDisabled";
    final TopicPartition topicPartition = new TopicPartition(topic, 1);
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(topicPartition));

    this.task.start(settings);
    task.put(
            ImmutableList.of(
                    write(
                            topic,
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L
                            ),
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L,
                                    "firstName", Schema.Type.STRING, true, "test",
                                    "lastName", Schema.Type.STRING, true, "user"
                            )
                    )
            )
    );
    this.task.stop();
    settings.put(ScyllaDbSinkConnectorConfig.TABLE_MANAGE_ENABLED_CONFIG, "false");
    this.task.start(settings);

    DataException ex = assertThrows(DataException.class, () -> {
      this.task.put(
              ImmutableList.of(
                      write(
                              topic,
                              struct("key",
                                      "id", Schema.Type.INT64, true, 12345L
                              ),
                              struct("key",
                                      "id", Schema.Type.INT64, true, 12345L,
                                      "firstName", Schema.Type.STRING, true, "test",
                                      "lastName", Schema.Type.STRING, true, "user",
                                      "city", Schema.Type.STRING, true, "Austin",
                                      "state", Schema.Type.STRING, true, "TX"
                              )
                      )
              )
      );
    });
    log.info("Exception", ex);
    assertTrue(
            ex.getMessage().contains("ALTER TABLE testkeyspace.tableExistsAlterManageTableDisabled ADD city varchar;"),
            "Error message should contain alter statement for city"
    );

    assertTrue(
            ex.getMessage().contains("ALTER TABLE testkeyspace.tableExistsAlterManageTableDisabled ADD state varchar;"),
            "Error message should contain alter statement for state"
    );
  }

  @Test
  public void offsetsExistInScylla() throws IOException {
    Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "offsetsExist";
    final TopicPartition topicPartition = new TopicPartition(topic, 1);
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(topicPartition));
    ScyllaDbSinkConnectorConfig config = new ScyllaDbSinkConnectorConfig(settings);
    try (ScyllaDbSession session = new ScyllaDbSessionFactory().newSession(config)) {
      Map<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.of(
              topicPartition, new OffsetAndMetadata(123451234L)
      );

      BatchStatement statement = new BatchStatement();
      session.addOffsetsToBatch(statement, offsets);
      session.executeStatement(statement);
    }
    this.task.start(settings);
    task.put(
            ImmutableList.of(
                    write(
                            topic,
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L
                            ),
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L,
                                    "firstName", Schema.Type.STRING, true, "test",
                                    "lastName", Schema.Type.STRING, true, "user"
                            )
                    )
            )
    );

    task.put(
            ImmutableList.of(
                    SinkRecordUtil.delete(
                            topic,
                            struct("key",
                                    "id", Schema.Type.INT64, true, 12345L
                            )
                    )
            )
    );
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(2)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
    verify(this.sinkTaskContext, times(1)).offset(ImmutableMap.of(topicPartition, 123451234L));
  }

  @Test
  public void testRecordWithTtl() throws InterruptedException {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    settings.put("scylladb.ttl", "10");
    final String topic = "insertTestingttl";
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(settings);
    List<SinkRecord> records = ImmutableList.of(
            write(
                    topic,
                    struct("key",
                            "id", Schema.Type.INT64, true, 12345L
                    ), struct("key",
                            "id", Schema.Type.INT64, true, 12345L,
                            "firstName", Schema.Type.STRING, true, "test",
                            "lastName", Schema.Type.STRING, true, "user"
                    )
            ),
            write(topic,
                    null,
                    asMap(
                            struct("key",
                                    "id", Schema.Type.INT64, true, 67890L
                            )
                    ),
                    null,
                    asMap(
                            struct("key",
                                    "id", Schema.Type.INT64, true, 67890L,
                                    "firstName", Schema.Type.STRING, true, "another",
                                    "lastName", Schema.Type.STRING, true, "user"
                            )
                    )
            )
    );
    this.validations = records.stream()
            .map(RowValidator::of)
            .collect(Collectors.toList());
    this.task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);

    verify(this.sinkTaskContext, times(1)).requestCommit();
    String query = "select * from testkeyspace.insertTestingttl";
    List<Row> beforeTTL = executeSelect(query);
    assertEquals(2, beforeTTL.size());
    /**
     * we are setting the ttl value to 10 second. So we assume data will be deleted after 10 second.
     * Therefore giving a sleep of 12 sec.
     */
    Thread.sleep(12 * 1000);
    List<Row> afterTTL = executeSelect(query);
    assertEquals(0, afterTTL.size());
  }

  @Test
  public void testRecordWithoutTtl() {
    final Map<String, String> settings = settings();
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "insertTestingWithoutTtl";
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 1)));
    this.task.start(settings);
    List<SinkRecord> records = ImmutableList.of(
            write(
                    topic,
                    struct("key",
                            "id", Schema.Type.INT64, true, 12345L
                    ), struct("key",
                            "id", Schema.Type.INT64, true, 12345L,
                            "firstName", Schema.Type.STRING, true, "test",
                            "lastName", Schema.Type.STRING, true, "user"
                    )
            ),
            write(topic,
                    null,
                    asMap(
                            struct("key",
                                    "id", Schema.Type.INT64, true, 67890L
                            )
                    ),
                    null,
                    asMap(
                            struct("key",
                                    "id", Schema.Type.INT64, true, 67890L,
                                    "firstName", Schema.Type.STRING, true, "another",
                                    "lastName", Schema.Type.STRING, true, "user"
                            )
                    )
            )
    );
    this.validations = records.stream()
            .map(RowValidator::of)
            .collect(Collectors.toList());
    this.task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);

    verify(this.sinkTaskContext, times(1)).requestCommit();
    String query = "SELECT TTL(firstName) from testkeyspace.insertTestingWithoutTtl";
    List<Row> getTTLRows = executeSelect(query);
    //TTL value is null i.e. ttl is not set.
    assertNull(getTTLRows.get(0).getObject(0));
    assertNull(getTTLRows.get(1).getObject(0));
  }

  @Test
  public void offsetsExistInKafka() {
    final Map<String, String> settings = settings();
    settings.put("scylladb.offset.storage.table.enable", "false");
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "insertTestingKafka";
    task.start(settings);
    List<SinkRecord> records = ImmutableList.of(
            write(
                    topic,
                    struct("key",
                            "id", Schema.Type.INT64, true, 12345L
                    ), struct("key",
                            "id", Schema.Type.INT64, true, 12345L,
                            "firstName", Schema.Type.STRING, true, "test",
                            "lastName", Schema.Type.STRING, true, "user"
                    )
            ),
            write(topic,
                    null,
                    asMap(
                            struct("key",
                                    "id", Schema.Type.INT64, true, 67890L
                            )
                    ),
                    null,
                    asMap(
                            struct("key",
                                    "id", Schema.Type.INT64, true, 67890L,
                                    "firstName", Schema.Type.STRING, true, "another",
                                    "lastName", Schema.Type.STRING, true, "user"
                            )
                    )
            )
    );
    validations = records.stream()
            .map(RowValidator::of)
            .collect(Collectors.toList());
    task.put(records);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    //scylladb.offset.storage.table does not exist.
    assertEquals(false, tableExists);
    verify(this.sinkTaskContext, times(1)).requestCommit();
    verify(this.sinkTaskContext, times(0)).assignment();
  }

  private void assertRow(Session session, RowValidator validation) {
    String query = validation.toString();

    log.info("Querying for {}", query);
    ResultSet results = session.execute(query);
    Row row = results.one();
    if (validation.rowExists && row != null) {
      assertNotNull(
              row,
              String.format("Could not find result for query. Query = '%s'", query)
      );

      for (String field : validation.value.keySet()) {
        Object expected = validation.value.get(field);
        Object actual = row.getObject(field);
        if (expected != null && actual != null && !expected.getClass().equals(actual.getClass())) {
          // Convert both to a string ...
          expected = expected.toString();
          actual = actual.toString();
          log.debug("Comparing string form of field {}. Query = '{}'", field, query);
        }
        assertEquals(
                expected,
                actual,
                String.format("Field does not match. Query = '%s'", query)
        );
      }
    } else {
      assertNull(
              row,
              String.format("Row should have been deleted. Query = '%s'", query)
      );
    }
  }

  private void execute(String cql) {
    try (Cluster cluster = clusterBuilder().build()) {
      try (Session session = cluster.connect(SCYLLADB_KEYSPACE)) {
        log.info("Executing: '" + cql + "'");
        session.execute(cql);
        log.debug("Executed: '" + cql + "'");
      }
    }
  }

  private List<Row> executeSelect(String query ) {
    try (Cluster cluster = clusterBuilder().build()) {
      try (Session session = cluster.connect(SCYLLADB_KEYSPACE)) {
        return session.execute(query).all();
      }
    }
  }

  private Boolean IsOffsetStorageTableExists(String tableName) {
    try (Cluster cluster = clusterBuilder().build()) {
      KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(SCYLLADB_KEYSPACE);
      TableMetadata table = ks.getTable(tableName);
      return (table == null) ? false : true;
    }
  }


}
