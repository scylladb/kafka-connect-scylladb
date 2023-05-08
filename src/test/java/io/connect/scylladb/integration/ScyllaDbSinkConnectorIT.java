package io.connect.scylladb.integration;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.shaded.guava.common.base.Stopwatch;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import io.connect.scylladb.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.test.IntegrationTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
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


  static String SCYLLA_DB_CONTACT_POINT;
  static final int SCYLLA_DB_PORT = 9042;
  static final String SCYLLADB_KEYSPACE = "testkeyspace";
  private static final String SCYLLADB_OFFSET_TABLE = "kafka_connect_offsets";
  private ScyllaDbSinkConnector connector;
  static final int MAX_CONNECTION_RETRIES = 30;

  static CqlSessionBuilder sessionBuilder() {
    ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, ProtocolVersion.V4.toString())
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1");

    CqlSessionBuilder sessionBuilder = CqlSession.builder()
            .addContactPoint(new InetSocketAddress(SCYLLA_DB_CONTACT_POINT, SCYLLA_DB_PORT))
            .withConfigLoader(driverConfigLoaderBuilder.build());
    return sessionBuilder;
  }

  static Map<String, String> settings() {
    Map<String, String> result = new LinkedHashMap<>();
    result.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, SCYLLADB_KEYSPACE);
    result.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG, "true");
    result.put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, SCYLLA_DB_CONTACT_POINT);
    result.put(ScyllaDbSinkConnectorConfig.LOAD_BALANCING_LOCAL_DC_CONFIG, "datacenter1");
    result.put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(SCYLLA_DB_PORT));
    result.put(ScyllaDbSinkConnectorConfig.KEYSPACE_REPLICATION_FACTOR_CONFIG, "1");
    return result;
  }

  @BeforeAll
  public static void setupKeyspace() throws InterruptedException {
    Properties systemProperties = System.getProperties();
    SCYLLA_DB_CONTACT_POINT = systemProperties.getProperty("scylla.docker.hostname", "localhost");
    CqlSessionBuilder builder = sessionBuilder();
    int attempts = 0;
    while (true) {
      attempts++;
      try (CqlSession session = builder.build()) {
        Stopwatch stopwatch = Stopwatch.createStarted();

        assertTrue(stopwatch.elapsed(TimeUnit.SECONDS) <= 30, "30 seconds have elapsed before creating keyspace.");
        session.execute("SELECT cql_version FROM system.local");
        break;
      } catch (AllNodesFailedException ex) {
        log.debug("Exception thrown.", ex);
        if(attempts >= MAX_CONNECTION_RETRIES){
          throw ex;
        }
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
      try (CqlSession session = sessionBuilder().withKeyspace(SCYLLADB_KEYSPACE).build()) {
        for (RowValidator validation : validations) {
          assertRow(session, validation);
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
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 3)));
    this.task.start(settings);
    List<SinkRecord> finalRecordsList = new ArrayList<>();
    for (int i = 0; i< 1000; i++) {
      List<SinkRecord> records = ImmutableList.of(
              write(
                      topic,
                      struct("key",
                              "id", Schema.Type.INT64, true, 12345L+ i
                      ), struct("key",
                              "id", Schema.Type.INT64, true, 12345L + i,
                              "firstName", Schema.Type.STRING, true, "test",
                              "lastName", Schema.Type.STRING, true, "user",
                              "age", Schema.Type.INT64, true, 1234L + i
                      )
              ),
              write(topic,
                      null,
                      asMap(
                              struct("key",
                                      "id", Schema.Type.INT64, true, 67890L + i
                              )
                      ),
                      null,
                      asMap(
                              struct("key",
                                      "id", Schema.Type.INT64, true, 67890L + i,
                                      "firstName", Schema.Type.STRING, true, "another",
                                      "lastName", Schema.Type.STRING, true, "user",
                                      "age", Schema.Type.INT64, true, 10L + i
                              )
                      )
              )
      );
      finalRecordsList.addAll(records);
    }
    this.validations = finalRecordsList.stream()
            .map(RowValidator::of)
            .collect(Collectors.toList());
    this.task.put(finalRecordsList);
    Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
    assertEquals(true, tableExists);
    verify(this.sinkTaskContext, times(1)).requestCommit();
    verify(this.sinkTaskContext, times(1)).assignment();
  }

  @Test
  @Disabled
  public void insertWithTopicMapping() {
    final Map<String, String> settings = settings();
    //adding mapping related configs
    settings.put("topic.insertTestingWithMapping.testkeyspace.my_table.mapping", "userid=key.id, "
            + "userfirstname=value.firstname, userlastname=value.lastname, userage=value.age, __ttl=value.time");
    settings.put("topic.insertTestingWithMapping.testkeyspace.my_table.consistencyLevel", "LOCAL_ONE");
    settings.put("topic.insertTestingWithMapping.testkeyspace.my_table.ttlSeconds", "3423");
    settings.put("topic.insertTestingWithMapping.testkeyspace.my_table.deletesEnabled", "false");
    connector = new ScyllaDbSinkConnector();
    connector.start(settings);
    final String topic = "insertTestingWithMapping";
    when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(new TopicPartition(topic, 3)));
    this.task.start(settings);
    List<SinkRecord> records = ImmutableList.of(
            write(
                    topic,
                    struct("key",
                            "id", Schema.Type.INT64, true, 12345L
                    ), struct("value",
                            "firstname", Schema.Type.STRING, true, "test",
                            "lastname", Schema.Type.STRING, true, "user",
                            "age", Schema.Type.INT64, true, 1234L,
                            "time", Schema.Type.INT32, true, 1000
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
                                    "firstname", Schema.Type.STRING, true, "another",
                                    "lastname", Schema.Type.STRING, true, "user",
                                    "age", Schema.Type.INT64, true, 10L,
                                    "time", Schema.Type.INT32, true, 10
                            )
                    )
            )
    );
    //TODO: Fix test
    //It seems this validation portion does not account for column name mapping.
    //e.g. checks for column 'id' instead of 'userid'
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


    List<SinkRecord> records = new ArrayList<>();
    Set<TopicPartition> assignment = new HashSet<>();

    for (int i = 0; i < 10; i++) {
      final String topic = "decimalTesting";
      final long keyValue = i;
      final Struct key = struct("key",
              "id", Schema.Type.INT64, true, keyValue
      );
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
                      ImmutableMap.of("id", index, "value", Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate())
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

      this.validations.add(
              new RowValidator(
                      topic,
                      ImmutableMap.of("id", index),
                      ImmutableMap.of("id", index, "value", date)
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

    execute("CREATE TABLE " + SCYLLADB_KEYSPACE + "." + topic + "("
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

  //TODO: Fix this test
  //Fails due to NullPointerException inside ScyllaDbSinkTaskHelper
  @Test
  @Disabled
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

    ConnectException ex = assertThrows(ConnectException.class, () -> {
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
    assertThrows(DataException.class, () -> {throw ex.getCause();});
    log.info("Exception", ex);
    String expected = "CREATE TABLE " + SCYLLADB_KEYSPACE + ".tableMissingManageTableDisabled".toLowerCase();
    assertTrue(
            ex.getCause().getMessage().contains(expected),
            String.format("Exception message should contain create statement." +
                "\n Expected: %s" +
                "\n Actual: %s"
                , expected, ex.getCause().getMessage())
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

    ConnectException ex = assertThrows(ConnectException.class, () -> {
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

    assertThrows(DataException.class, () -> {throw ex.getCause();});
    log.info("Exception", ex);
    assertTrue(
            ex.getCause().getMessage().contains("ALTER TABLE " + SCYLLADB_KEYSPACE + ".tableExistsAlterManageTableDisabled".toLowerCase() + " ADD city text;"),
            "Error message should contain alter statement for city. Actual message: " + ex.getCause().getMessage()
    );

    assertTrue(
            ex.getCause().getMessage().contains("ALTER TABLE " + SCYLLADB_KEYSPACE + ".tableExistsAlterManageTableDisabled".toLowerCase() + " ADD state text;"),
            "Error message should contain alter statement for state. Actual message: " + ex.getCause().getMessage()
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

      offsets.entrySet().stream()
        .map(e -> session.getInsertOffsetStatement(e.getKey(), e.getValue()))
        .forEach(session::executeStatement);
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
    String query = "select * from " + SCYLLADB_KEYSPACE + ".insertTestingttl";
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
    String query = "SELECT TTL(\"firstName\") from " + SCYLLADB_KEYSPACE + ".insertTestingWithoutTtl";
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

  private void assertRow(CqlSession session, RowValidator validation) {
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
        // Quick fix for Kafka Timestamp.SCHEMA
        // It's Java representation is a Date, but CQL timestamp is represented as java.time.Instant
        if ((expected instanceof Float) && (actual instanceof Double)) {
          log.debug("Comparing Double form of Float field {}. Query = '{}'", field, query);
          Double expectedCasted = ((Float) expected).doubleValue();
          assertEquals(
              expectedCasted,
              actual,
              String.format("Field does not match. Query = '%s'", query)
          );
        }
        else if ((expected instanceof java.util.Date) && (actual instanceof Instant)) {
          log.debug("Comparing Instant form of Date field {}. Query = '{}'", field, query);
          Instant expectedInstant = ((java.util.Date) expected).toInstant();
          assertEquals(
              expectedInstant,
              actual,
              String.format("Field does not match. Query = '%s'", query)
          );
        }
        else if ((expected instanceof java.util.Date) && (actual instanceof LocalTime)) {
          log.debug("Comparing LocalTime form of Date field {}. Query = '{}'", field, query);
          final long nanoseconds = TimeUnit.NANOSECONDS.convert(((Date) expected).getTime(), TimeUnit.MILLISECONDS);
          assertEquals(
              LocalTime.ofNanoOfDay(nanoseconds),
              actual,
              String.format("Field does not match. Query = '%s'", query)
          );
        }
        else if (expected != null && actual != null && !expected.getClass().equals(actual.getClass())) {
          // Convert both to a string ...
          expected = expected.toString();
          actual = actual.toString();
          log.debug("Comparing string form of field {}. Query = '{}'", field, query);
          assertEquals(
              expected,
              actual,
              String.format("Field does not match. Query = '%s'", query)
          );
        }
        else {
          assertEquals(
              expected,
              actual,
              String.format("Field does not match. Query = '%s'", query)
          );
        }
      }
    } else {
      assertNull(
              row,
              String.format("Row should have been deleted. Query = '%s'", query)
      );
    }
  }

  private void execute(String cql) {
    try (CqlSession session = sessionBuilder().withKeyspace(SCYLLADB_KEYSPACE).build()) {
      log.info("Executing: '" + cql + "'");
      session.execute(cql);
      log.debug("Executed: '" + cql + "'");
    }
  }

  private List<Row> executeSelect(String query) {
    try (CqlSession session = sessionBuilder().withKeyspace(SCYLLADB_KEYSPACE).build()) {
      return session.execute(query).all();
    }
  }

  private Boolean IsOffsetStorageTableExists(String tableName) {
    try (CqlSession session = sessionBuilder().build()) {
      Optional<KeyspaceMetadata> ks = session.getMetadata().getKeyspace(SCYLLADB_KEYSPACE);
      if (!ks.isPresent()) {
        return false;
      } else {
        Optional<TableMetadata> table = ks.get().getTable(tableName);
        return table.isPresent();
      }
    }
  }


}
