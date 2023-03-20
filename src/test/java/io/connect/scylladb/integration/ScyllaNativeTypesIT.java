package io.connect.scylladb.integration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.connect.scylladb.ScyllaDbSinkConnector;
import io.connect.scylladb.ScyllaDbSinkConnectorConfig;
import io.connect.scylladb.ScyllaDbSinkTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScyllaNativeTypesIT {
    private static final Logger log = LoggerFactory.getLogger(ScyllaNativeTypesIT.class);

    private static final boolean DROP_TEST_TABLES = true; // Set false to later inspect results manually.
    private static String currentTestTable;

    static String SCYLLA_DB_CONTACT_POINT = "localhost"; // Default value, overwritten later by Properties.
    static final int SCYLLA_DB_PORT = 9042;
    static final String SCYLLADB_KEYSPACE = "testkeyspace";
    private static final String SCYLLADB_OFFSET_TABLE = "kafka_connect_offsets";
    private ScyllaDbSinkConnector connector;
    static final int MAX_CONNECTION_RETRIES = 10;

    static Cluster.Builder clusterBuilder() {
        Cluster.Builder clusterBuilder = Cluster.builder()
                .withPort(SCYLLA_DB_PORT)
                .addContactPoints(SCYLLA_DB_CONTACT_POINT)
                .withProtocolVersion(ProtocolVersion.DEFAULT);
        return clusterBuilder;
    }

    private static final Map<String, String> settings = new HashMap<>(ImmutableMap.<String, String>builder()
            .put(ScyllaDbSinkConnectorConfig.TABLE_MANAGE_ENABLED_CONFIG, "false")
            .put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, SCYLLADB_KEYSPACE)
            .put(ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG, "true")
            .put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, SCYLLA_DB_CONTACT_POINT)
            .put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(SCYLLA_DB_PORT))
            .put(ScyllaDbSinkConnectorConfig.KEYSPACE_REPLICATION_FACTOR_CONFIG, "1")
            .put(ScyllaDbSinkConnectorConfig.OFFSET_COMMIT_AFTER_EVERY_INSERT, "true")
            .build());

    @BeforeAll
    public static void setupKeyspace() throws InterruptedException {
        Properties systemProperties = System.getProperties();
        SCYLLA_DB_CONTACT_POINT = systemProperties.getProperty("scylla.docker.hostname", "localhost");
        Cluster.Builder builder = clusterBuilder();
        int attempts = 0;
        while (++attempts < MAX_CONNECTION_RETRIES) {
            try (Cluster cluster = builder.build()) {
                try (Session session = cluster.connect()) {
                    session.execute("SELECT cql_version FROM system.local");
                    break;
                }
            } catch (NoHostAvailableException ex) {
                if(attempts >= MAX_CONNECTION_RETRIES){
                    throw ex;
                }
                else{
                    log.debug("Exception thrown: ", ex);
                    log.debug("Retrying...");
                    Thread.sleep(1000);
                }
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

        connector = new ScyllaDbSinkConnector();
        connector.start(settings);
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
        if(DROP_TEST_TABLES) {
            query = "DROP TABLE IF EXISTS" + " " + currentTestTable;
            execute(query);
        }
        this.task.stop();
        this.connector.stop();
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


    private void makeSimpleTable(String tableName, String colType){
        Properties systemProperties = System.getProperties();
        SCYLLA_DB_CONTACT_POINT = systemProperties.getProperty("scylla.docker.hostname", "localhost");
        Cluster.Builder builder = clusterBuilder();
        try (Cluster cluster = builder.build()) {
            try (Session session = cluster.connect()) {
                session.execute("DROP TABLE IF EXISTS " + SCYLLADB_KEYSPACE + "." + tableName + "");
                session.execute("CREATE TABLE IF NOT EXISTS " + SCYLLADB_KEYSPACE + "." + tableName + " (" +
                        "id int PRIMARY KEY, " +
                        "col_"+colType+ " " + colType +
                        ")");

            }
        } catch (NoHostAvailableException ex) {
            log.debug("Exception thrown.", ex);
        }
    }

    @Test
    public void insertAscii(){
        final String topic = "asciiType";
        final String type = "ascii";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.STRING_SCHEMA, "someText");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(1);
    }

    @Test
    public void insertBigint(){
        final String topic = "bigintType";
        final String type = "bigint";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.INT8_SCHEMA, Byte.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Schema.INT16_SCHEMA, Short.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 3, Schema.INT32_SCHEMA, Integer.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 4, Schema.INT64_SCHEMA, Long.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 5, Schema.STRING_SCHEMA, "12345");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(5);
    }

    @Test
    public void insertBlob(){
        final String topic = "blobType";
        final String type = "blob";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.BYTES_SCHEMA, new byte[] {48, 49, 50});
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        //TODO: Needs custom validation due to different format returned when QUERYing
        this.validations.clear();
        checkCorrectness(1);
    }

    @Test
    public void insertBoolean(){
        final String topic = "booleanType";
        final String type = "boolean";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.BOOLEAN_SCHEMA, true);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Schema.BOOLEAN_SCHEMA, Boolean.FALSE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(2);
    }

    @Test
    @Disabled
    public void insertCounter(){
        final String topic = "counterType";
        final String type = "counter";
        setupTypeTest(topic, type);
        SinkRecord record;

        // This will fail with 'com.datastax.driver.core.exceptions.InvalidQueryException: INSERT statement are not allowed on counter tables, use UPDATE instead'
        // TODO: Add UPDATE statement support then verify it.
        record = setupRecord(topic, type, 1, Schema.INT64_SCHEMA, Long.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));
        
        checkCorrectness(1);
    }

    @Test
    public void insertDate(){
        final String topic = "dateType";
        final String type = "date";
        setupTypeTest(topic, type);
        SinkRecord record;

        // Handled by driver-extras SimpleDateCodec
        record = setupRecord(topic, type, 1, Schema.INT32_SCHEMA, 0);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Date.SCHEMA, java.util.Date.from(Instant.EPOCH));
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        //TODO: Needs custom validation due to different format returned when QUERYing
        this.validations.clear();
        checkCorrectness(2);
    }

    @Test
    public void insertDecimal(){
        final String topic = "decimalType";
        final String type = "decimal";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Decimal.schema(0), BigDecimal.valueOf(123456,0));
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Decimal.schema(4), BigDecimal.valueOf(123456,4));
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 3, Schema.FLOAT64_SCHEMA, Double.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 4, Schema.FLOAT32_SCHEMA, Float.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 5, Schema.INT32_SCHEMA, Integer.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 6, Schema.INT64_SCHEMA, Long.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        //TODO: Needs custom validation due to different format returned when QUERYing
        this.validations.clear();
        checkCorrectness(6);
    }

    @Test
    public void insertDouble(){
        final String topic = "doubleType";
        final String type = "double";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.FLOAT32_SCHEMA, Float.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Schema.FLOAT64_SCHEMA, Double.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(2);
    }

    @Test
    public void insertDuration(){
        final String topic = "durationType";
        final String type = "duration";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.STRING_SCHEMA, "12h30m");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Schema.STRING_SCHEMA, "P3Y6M4DT12H30M5S");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 3, Schema.STRING_SCHEMA, "PT0S");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 4, Schema.STRING_SCHEMA, "P0D");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));
        /*
        // java.lang.IllegalArgumentException: Unable to convert 'P0.5Y' to a duration
        value =
                new Struct(schema)
                        .put("id", 5)
                        .put("col_" + type, "P0.5Y");
        record = new SinkRecord(topic, 0, schema, value, schema, value, 1234L, 1234L, TimestampType.CREATE_TIME);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));
        */
        //TODO: Needs custom validation due to different format returned when QUERYing
        this.validations.clear();
        checkCorrectness(4);
    }

    @Test
    public void insertFloat(){
        final String topic = "floatType";
        final String type = "float";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.FLOAT32_SCHEMA, Float.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(1);
    }

    @Test
    public void insertInet(){
        final String topic = "inetType";
        final String type = "inet";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.STRING_SCHEMA, "'127.0.0.1'");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Schema.STRING_SCHEMA, "'localhost'");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        //TODO: Needs custom validation due to different format returned when QUERYing
        this.validations.clear();
        checkCorrectness(2);
    }

    @Test
    public void insertInt(){
        final String topic = "intType";
        final String type = "int";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.INT8_SCHEMA, Byte.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Schema.INT16_SCHEMA, Short.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 3, Schema.INT32_SCHEMA, Integer.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 4, Schema.STRING_SCHEMA, "1234");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(4);
    }

    @Test
    public void insertSmallint(){
        final String topic = "smallintType";
        final String type = "smallint";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.INT8_SCHEMA, Byte.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Schema.INT16_SCHEMA, Short.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 3, Schema.STRING_SCHEMA, "12345");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(3);
    }

    @Test
    public void insertText(){
        final String topic = "textType";
        final String type = "text";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.STRING_SCHEMA, "some_text");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(1);
    }

    @Test
    public void insertTime(){
        final String topic = "timeType";
        final String type = "time";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.INT64_SCHEMA, 3600L * (long) 1e9);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Time.SCHEMA, new java.util.Date(2L * 60L * 60L * 1000L));
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        //TODO: Needs custom validation due to different format returned when QUERYing
        this.validations.clear();
        checkCorrectness(2);
    }

    @Test
    public void insertTimestamp(){
        final String topic = "timestampType";
        final String type = "timestamp";
        setupTypeTest(topic, type);
        SinkRecord record;

        record = setupRecord(topic, type, 1, Schema.STRING_SCHEMA, "2011-12-03T10:15:30Z");
        //TODO: Needs custom validation because:
        //org.opentest4j.AssertionFailedError: Field does not match. Query = 'SELECT * FROM timestampType WHERE id=1;' ==>
        //Expected :2011-12-03T10:15:30Z
        //Actual   :Sat Dec 03 11:15:30 CET 2011
        //this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Timestamp.SCHEMA, java.util.Date.from(Instant.EPOCH));
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(2);
    }



    @Test
    public void insertTimeuuid() {
        final String topic = "timeuuidType";
        final String type = "timeuuid";
        setupTypeTest(topic, type);
        SinkRecord record;
        
        final UUID colValue = UUIDs.timeBased();
        assertEquals(1, colValue.version());
        
        record = setupRecord(topic, type, 1, Schema.STRING_SCHEMA, colValue.toString());
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));
        
        checkCorrectness(1);
    }

    @Test
    public void insertTinyint(){
        final String topic = "tinyintType";
        final String type = "tinyint";
        setupTypeTest(topic, type);
        SinkRecord record;
        
        record = setupRecord(topic, type, 1, Schema.INT8_SCHEMA, Byte.MAX_VALUE);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Schema.STRING_SCHEMA, "123");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(2);
    }

    @Test
    public void insertUuid(){
        final String topic = "uuidType";
        final String type = "uuid";
        setupTypeTest(topic, type);
        SinkRecord record;
        
        record = setupRecord(topic, type, 1, Schema.STRING_SCHEMA, UUID.randomUUID().toString());
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));
        
        checkCorrectness(1);
    }


    @Test
    public void insertVarint(){
        final String type = "varint";
        final String topic = type + "Test";
        setupTypeTest(topic, type);
        SinkRecord record;
        
        record = setupRecord(topic, type, 1, Schema.STRING_SCHEMA, "123456789012345678901234567890");
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        record = setupRecord(topic, type, 2, Schema.INT64_SCHEMA, 123456789L);
        this.validations.add(RowValidator.of(record));
        task.put(ImmutableList.of(record));

        checkCorrectness(2);
    }

    private void setupTypeTest(String topic, String type){
        final TopicPartition topicPartition = new TopicPartition(topic, 1);
        currentTestTable = topic;
        makeSimpleTable(topic, type);
        when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(topicPartition));
        this.task.start(settings);
    }

    private SinkRecord setupRecord(String topic, String type, int id, Schema colSchema, Object colValue){
        Schema keySchema = SchemaBuilder.struct()
                .name(topic)
                .field("id", Schema.INT32_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .name(topic)
                .field("id", Schema.INT32_SCHEMA)
                .field("col_" + type, colSchema)
                .build();

        Struct key = new Struct(keySchema)
                .put("id", id);

        Struct value = new Struct(valueSchema)
                .put("id", id)
                .put("col_" + type, colValue);

        return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, 1234L, 1234L, TimestampType.CREATE_TIME);
    }

    private void checkCorrectness(int numOfPuts){
        Boolean tableExists = IsOffsetStorageTableExists(SCYLLADB_OFFSET_TABLE);
        assertEquals(true, tableExists);
        verify(this.sinkTaskContext, times(numOfPuts)).requestCommit();
        verify(this.sinkTaskContext, times(1)).assignment();
    }
}
