package io.connect.scylladb.integration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.connect.scylladb.ScyllaDbSinkConnector;
import io.connect.scylladb.ScyllaDbSinkConnectorConfig;
import io.connect.scylladb.ScyllaDbSinkTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

public class ScyllaComplexTypesIT {
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
                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED);
        return clusterBuilder;
    }

    private static final Map<String, String> settings = new HashMap<>(ImmutableMap.<String, String>builder()
            .put(ScyllaDbSinkConnectorConfig.TABLE_MANAGE_ENABLED_CONFIG, "false")
            .put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, SCYLLADB_KEYSPACE)
            .put(ScyllaDbSinkConnectorConfig.KEYSPACE_CREATE_ENABLED_CONFIG, "true")
            .put(ScyllaDbSinkConnectorConfig.CONTACT_POINTS_CONFIG, SCYLLA_DB_CONTACT_POINT)
            .put(ScyllaDbSinkConnectorConfig.PORT_CONFIG, String.valueOf(SCYLLA_DB_PORT))
            .put(ScyllaDbSinkConnectorConfig.KEYSPACE_REPLICATION_FACTOR_CONFIG, "1")
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

    public void startConnector() {
        this.task = new ScyllaDbSinkTask();
        this.sinkTaskContext = mock(SinkTaskContext.class);
        this.task.initialize(this.sinkTaskContext);
        this.validations = new ArrayList<>();

        connector = new ScyllaDbSinkConnector();
        connector.start(settings);
    }
    public void startConnector(Map<String, String> customSettings) {
        this.task = new ScyllaDbSinkTask();
        this.sinkTaskContext = mock(SinkTaskContext.class);
        this.task.initialize(this.sinkTaskContext);
        this.validations = new ArrayList<>();

        connector = new ScyllaDbSinkConnector();
        connector.start(customSettings);
    }

    @AfterEach
    public void stop() {
        String query = "DROP TABLE" + " " + SCYLLADB_OFFSET_TABLE;
        if (IsOffsetStorageTablePresent(SCYLLADB_OFFSET_TABLE)) {
            execute(query);
        }
        if(DROP_TEST_TABLES) {
            query = "DROP TABLE IF EXISTS" + " " + currentTestTable;
            execute(query);
        }
        this.task.stop();
        this.connector.stop();
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

    private Boolean IsOffsetStorageTablePresent(String tableName) {
        try (Cluster cluster = clusterBuilder().build()) {
            KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(SCYLLADB_KEYSPACE);
            TableMetadata table = ks.getTable(tableName);
            return table != null;
        }
    }

    private void startTask(String topic, Map<String,String> customSettings){
        final TopicPartition topicPartition = new TopicPartition(topic, 1);
        currentTestTable = topic;
        when(this.sinkTaskContext.assignment()).thenReturn(ImmutableSet.of(topicPartition));
        this.task.start(customSettings);
    }

    private void startTask(String topic){
        startTask(topic, settings);
    }

    @Test
    public void insertMap(){
        final String name = "map";
        final String topic = name + "Test";
        final Map<String, String> testSettings = new HashMap<>(settings);
        testSettings.put(ScyllaDbSinkConnectorConfig.TABLE_MANAGE_ENABLED_CONFIG, "true");
        startConnector(testSettings);
        startTask(topic, testSettings);
        SinkRecord record;

        Map<String, Integer> colValue1 =
                ImmutableMap.<String, Integer>builder().put("key1", 11).put("key2", 22).build();
        record = setupRecord(topic + "1", name, 1, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(), colValue1);
        task.put(ImmutableList.of(record));

        Map<Integer, Long> colValue2 =
                ImmutableMap.<Integer, Long>builder().put(11, 22L).put(33, 44L).build();
        record = setupRecord(topic + "2", name, 1, SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT64_SCHEMA).build(), colValue2);
        task.put(ImmutableList.of(record));

        Map<String, java.util.Date> colValue3 =
                ImmutableMap.<String, java.util.Date>builder().put("key1", java.util.Date.from(Instant.EPOCH)).build();
        record = setupRecord(topic + "3", name, 1, SchemaBuilder.map(Schema.STRING_SCHEMA, Timestamp.SCHEMA).build(), colValue3);
        task.put(ImmutableList.of(record));

        checkCorrectness(3);
    }

    @Test
    public void insertStruct(){
        final String name = "struct";
        final String topic = name + "Test";

        //Create user type in correct keyspace
        execute("CREATE TYPE IF NOT EXISTS " + SCYLLADB_KEYSPACE + ".testudt4 (\n" +
                "  col1 int,\n" +
                "  col2 bigint,\n" +
                "  col3 text);");

        createTestTable(topic, "col_" + name, "testudt4");
        startConnector();
        startTask(topic);

        Schema colSchema = SchemaBuilder.struct()
                .name("testudt4") //Needs to match the name of existing User Type if the table is managed
                // and created by connector. Otherwise ScyllaDbSchemaBuilder won't know what DataType to use.
                .field("col1", Schema.INT32_SCHEMA)
                .field("col2", Schema.INT64_SCHEMA)
                .field("col3", Schema.STRING_SCHEMA)
                .build();

        Struct colValue = new Struct(colSchema)
                .put("col1", 123)
                .put("col2", 456L)
                .put("col3", "text");

        SinkRecord record = setupRecord(topic, name, 1, colSchema, colValue);
        task.put(ImmutableList.of(record));
        checkCorrectness(1);
    }

    @Test
    public void insertSet(){
        final String name = "set";
        final String topic = name + "Test";
        createTestTable(topic, "col_" + name, "set<text>");
        startConnector();
        startTask(topic);

        Schema colSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        List<String> colValue = new ArrayList<>(ImmutableList.of("a", "b", "c", "a", "b"));

        SinkRecord record = setupRecord(topic, name, 1, colSchema, colValue);
        task.put(ImmutableList.of(record));
        checkCorrectness(1);
    }

    @Test
    public void insertList(){
        final String name = "list";
        final String topic = name + "Test";
        createTestTable(topic, "col_" + name, "list<text>");
        startConnector();
        startTask(topic);

        Schema colSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        List<String> colValue = new ArrayList<>(ImmutableList.of("a", "b", "c", "a", "b"));

        SinkRecord record = setupRecord(topic, name, 1, colSchema, colValue);
        task.put(ImmutableList.of(record));
        checkCorrectness(1);
    }

    @Test
    public void insertTuple(){
        final String name = "tuple";
        final String topic = name + "Test";
        createTestTable(topic, "col_" + name, "tuple<text, int>");
        startConnector();
        startTask(topic);

        Schema colSchema = SchemaBuilder.struct()
                .name("structToTuple")
                .field("col1", Schema.STRING_SCHEMA)
                .field("col2", Schema.INT32_SCHEMA)
                .build();

        Struct colValue = new Struct(colSchema)
                .put("col1", "text")
                .put("col2", 123);

        SinkRecord record = setupRecord(topic, name, 1, colSchema, colValue);
        task.put(ImmutableList.of(record));
        checkCorrectness(1);
    }
    @Test
    public void insertListIntoTuple(){
        final String name = "tupleFromList";
        final String topic = name + "Test";
        createTestTable(topic, "col_tupleFromList", "tuple<text, int>");

        startConnector();
        startTask(topic);

        Schema colSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        List<String> colValue = new ArrayList<>(ImmutableList.of("a", "1"));

        SinkRecord record = setupRecord(topic, name, 1, colSchema, colValue);
        task.put(ImmutableList.of(record));
        checkCorrectness(1);
    }

    @Test
    public void insertMapIntoUDT(){
        final String name = "mapIntoUDT";
        final String topic = name + "Test";
        SinkRecord record;

        execute("CREATE TYPE IF NOT EXISTS " + SCYLLADB_KEYSPACE + ".mapIntoUDTType (\n"
                + "key1 int,\n"
                + "key2 bigint);");

        createTestTable(topic, "col_" + name, "mapIntoUDTType");

        startConnector();
        startTask(topic);

        Map<String, Integer> colValue1 =
                ImmutableMap.<String, Integer>builder().put("key1", 11).put("key2", 22).build();
        record = setupRecord(topic, name, 1, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(), colValue1);
        task.put(ImmutableList.of(record));

        checkCorrectness(1);
    }

    private SinkRecord setupRecord(String topic, String colNameSuffix, int id, Schema colSchema, Object colValue){
        Schema keySchema = SchemaBuilder.struct()
                .name(topic)
                .field("id", Schema.INT32_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .name(topic)
                .field("id", Schema.INT32_SCHEMA)
                .field("col_" + colNameSuffix, colSchema)
                .build();

        Struct key = new Struct(keySchema)
                .put("id", id);

        Struct value = new Struct(valueSchema)
                .put("id", id)
                .put("col_" + colNameSuffix, colValue);

        return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, 1234L, 1234L, TimestampType.CREATE_TIME);
    }

    private void createTestTable(String tableName, String colName, String colType){
        execute("CREATE TABLE IF NOT EXISTS " + SCYLLADB_KEYSPACE + "." + tableName + " (\n"
                + "id int PRIMARY KEY,\n"
                + colName + " " + colType +"\n"
                + ");");
    }

    private void checkCorrectness(int numOfPuts){
        Boolean tableExists = IsOffsetStorageTablePresent(SCYLLADB_OFFSET_TABLE);
        assertEquals(true, tableExists);
        verify(this.sinkTaskContext, times(numOfPuts)).requestCommit();
        verify(this.sinkTaskContext, times(1)).assignment();
    }
}
