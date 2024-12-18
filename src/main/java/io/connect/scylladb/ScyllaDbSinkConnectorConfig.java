package io.connect.scylladb;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import io.connect.scylladb.utils.ListRecommender;
import io.connect.scylladb.utils.NullOrReadableFile;
import io.connect.scylladb.utils.VisibleIfEqual;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;


import io.confluent.kafka.connect.utils.config.ConfigUtils;
import io.confluent.kafka.connect.utils.config.ValidEnum;
import io.confluent.kafka.connect.utils.config.ValidPort;
import io.connect.scylladb.topictotable.TopicConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for {@link ScyllaDbSinkConnector}.
 */
public class ScyllaDbSinkConnectorConfig extends AbstractConfig {

  public final int port;
  public final String contactPoints;
  public final ConsistencyLevel consistencyLevel;
  public final boolean securityEnabled;
  public final String username;
  public final String password;
  public final String compression;
  public final boolean sslEnabled;
  public final boolean sslHostnameVerificationEnabled;
  public final boolean deletesEnabled;
  public final String keyspace;
  public final boolean keyspaceCreateEnabled;
  public final int keyspaceReplicationFactor;
  public final boolean offsetEnabledInScyllaDB;
  public final boolean tableManageEnabled;
  public final String tableCompressionAlgorithm;
  public final char[] trustStorePassword;
  public final File trustStorePath;
  public final char[] keyStorePassword;
  public final File keyStorePath;
  public final String offsetStorageTable;
  public final long statementTimeoutMs;
  public final String loadBalancingLocalDc;
  public final Map<String, TopicConfigs> topicWiseConfigs;
  public final Integer ttl;
  public final BehaviorOnError behaviourOnError;
  public final List<String> cipherSuites;

  private static final Pattern TOPIC_KS_TABLE_SETTING_PATTERN =
          Pattern.compile("topic\\.([a-zA-Z0-9._-]+)\\.([^.]+|\"[\"]+\")\\.([^.]+|\"[\"]+\")\\.(mapping|consistencyLevel|ttlSeconds|deletesEnabled)$");

  private static final String[] TOPIC_WISE_CONFIGS_VALID_SUFFIXES = {".mapping",".consistencyLevel",".ttlSeconds",".deletesEnabled"};

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSinkConnectorConfig.class);

  static final Set<String> CLIENT_COMPRESSION = ImmutableSet.of("none", "lz4", "snappy");

  static final Set<String> TABLE_COMPRESSORS = ImmutableSet.of("SnappyCompressor", "LZ4Compressor", "DeflateCompressor", "none");


  public ScyllaDbSinkConnectorConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.port = getInt(PORT_CONFIG);
    this.contactPoints = getString(CONTACT_POINTS_CONFIG);
    this.consistencyLevel =
            ConfigUtils.getEnum(DefaultConsistencyLevel.class, this, CONSISTENCY_LEVEL_CONFIG);
    this.username = getString(USERNAME_CONFIG);
    this.password = getPassword(PASSWORD_CONFIG) == null ? null : getPassword(PASSWORD_CONFIG).value();
    this.securityEnabled = getBoolean(SECURITY_ENABLE_CONFIG);
    this.sslEnabled = getBoolean(SSL_ENABLED_CONFIG);
    this.sslHostnameVerificationEnabled = getBoolean(SSL_HOSTNAME_VERIFICATION_CONFIG);
    this.deletesEnabled = getBoolean(DELETES_ENABLE_CONFIG);

    this.keyspace = getString(KEYSPACE_CONFIG);
    this.ttl = getInt(TTL_CONFIG);

    final String trustStorePath = this.getString(SSL_TRUSTSTORE_PATH_CONFIG);
    this.trustStorePath = Strings.isNullOrEmpty(trustStorePath) ? null : new File(trustStorePath);
    this.trustStorePassword =
            getPassword(SSL_TRUSTSTORE_PASSWORD_CONFIG) == null ? null : getPassword(SSL_TRUSTSTORE_PASSWORD_CONFIG).value().toCharArray();

    final String keyStorePath = this.getString(SSL_KEYSTORE_PATH_CONFIG);
    this.keyStorePath = Strings.isNullOrEmpty(keyStorePath) ? null : new File(keyStorePath);
    this.keyStorePassword =
            getPassword(SSL_KEYSTORE_PASSWORD_CONFIG) == null ? null : getPassword(SSL_KEYSTORE_PASSWORD_CONFIG).value().toCharArray();

    this.cipherSuites = getList(SSL_CIPHER_SUITES_CONFIG);

    final String compression = getString(COMPRESSION_CONFIG);
    this.compression = compression;
    this.keyspaceCreateEnabled = getBoolean(KEYSPACE_CREATE_ENABLED_CONFIG);
    this.offsetEnabledInScyllaDB = getBoolean(ENABLE_OFFSET_STORAGE_TABLE);
    this.keyspaceReplicationFactor = getInt(KEYSPACE_REPLICATION_FACTOR_CONFIG);
    this.tableManageEnabled = getBoolean(TABLE_MANAGE_ENABLED_CONFIG);
    this.tableCompressionAlgorithm = getString(TABLE_CREATE_COMPRESSION_ALGORITHM_CONFIG);

    this.offsetStorageTable = getString(OFFSET_STORAGE_TABLE_CONF);
    this.statementTimeoutMs = getLong(EXECUTE_STATEMENT_TIMEOUT_MS_CONF);
    this.loadBalancingLocalDc = getString(LOAD_BALANCING_LOCAL_DC_CONFIG);
    this.behaviourOnError = BehaviorOnError.valueOf(getString(BEHAVIOR_ON_ERROR_CONFIG).toUpperCase());

    Map<String, Map<String, String>> topicWiseConfigsMap = new HashMap<>();
    for (final Map.Entry<String, String> entry : ((Map<String, String>) originals).entrySet()) {
      final String name2 = entry.getKey();
      if (name2.startsWith("topic.") && hasTopicWiseConfigSuffix(name2)) {
        final String topicName = this.tryMatchTopicName(name2);
        log.debug("Interpreting " + name2 + " as custom TopicWiseConfig for topic " + topicName);
        final Map<String, String> topicMap = topicWiseConfigsMap.computeIfAbsent(topicName, t -> new HashMap());
        topicMap.put(name2.split("\\.")[name2.split("\\.").length - 1], entry.getValue());
      }
    }
    topicWiseConfigs = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> topicWiseConfig : topicWiseConfigsMap.entrySet()) {
      TopicConfigs topicConfigs = new TopicConfigs(topicWiseConfig.getValue(), this);
      topicWiseConfigs.put(topicWiseConfig.getKey(), topicConfigs);
    }
  }

  public static final String PORT_CONFIG = "scylladb.port";
  private static final String PORT_DOC = "The port the ScyllaDB hosts are listening on. "
          + "Eg. When using a docker image, connect to the port it uses(use docker ps)";

  public static final String CONTACT_POINTS_CONFIG = "scylladb.contact.points";
  static final String CONTACT_POINTS_DOC = "The ScyllaDB hosts to connect to. "
          + "Scylla nodes use this list of hosts to find each other and learn "
          + "the topology of the ring. You must change this if you are running "
          + "multiple nodes. It's essential to put at least 2 hosts in case of "
          + "bigger cluster, since if first host is down, it will contact second "
          + "one and get the state of the cluster from it. Eg. When using the docker "
          + "image, connect to the host it uses. To connect to private Scylla nodes, "
          + "provide a JSON string having all internal private network address:port "
          + "mapped to an external network address:port as key value pairs. "
          + "Need to pass it as {\"private_host1:port1\",\"public_host1:port1\", "
          + "\"private_host2:port2\",\"public_host2:port2\", ...}"
          + "Eg. {\"10.0.24.69:9042\": \"sl-eu-lon-2-portal.3.dblayer.com:15227\", "
          + "\"10.0.24.71:9042\": \"sl-eu-lon-2-portal.2.dblayer.com:15229\", "
          + "\"10.0.24.70:9042\": \"sl-eu-lon-2-portal.1.dblayer.com:15228\"}";

  public static final String CONSISTENCY_LEVEL_CONFIG = "scylladb.consistency.level";
  private static final String CONSISTENCY_LEVEL_DOC = "The requested consistency level "
          + "to use when writing to ScyllaDB. The Consistency Level (CL) determines how "
          + "many replicas in a cluster that must acknowledge read or write operations "
          + "before it is considered successful. Valid values are ANY, ONE, TWO, THREE, "
          + "QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE.";

  public static final String SSL_ENABLED_CONFIG = "scylladb.ssl.enabled";
  private static final String SSL_ENABLED_DOC = "Flag to determine if SSL is enabled when connecting to ScyllaDB.";

  public static final String SECURITY_ENABLE_CONFIG = "scylladb.security.enabled";
  static final String SECURITY_ENABLE_DOC = "To enable security while loading "
          + "the sink connector and connecting to ScyllaDB.";

  public static final String DELETES_ENABLE_CONFIG = "scylladb.deletes.enabled";
  private static final String DELETES_ENABLE_DOC =
          "Flag to determine if the connector should process deletes.";

  public static final String USERNAME_CONFIG = "scylladb.username";
  private static final String USERNAME_DOC = "The username to connect to ScyllaDB with. "
          + "Set scylladb.security.enable = true to use this config.";

  public static final String PASSWORD_CONFIG = "scylladb.password";
  private static final String PASSWORD_DOC = "The password to connect to ScyllaDB with. "
          + "Set scylladb.security.enable = true to use this config.";

  public static final String KEYSPACE_CONFIG = "scylladb.keyspace";
  private static final String KEYSPACE_DOC = "The keyspace to write to. "
          + "This keyspace is like a database in the ScyllaDB cluster.";

  public static final String KEYSPACE_CREATE_ENABLED_CONFIG = "scylladb.keyspace.create.enabled";
  private static final String KEYSPACE_CREATE_ENABLED_DOC = "Flag to determine if the keyspace "
          + "should be created if it does not exist. **Note**: Error if a new keyspace has to "
          + "be created and the config is false.";

  public static final String KEYSPACE_REPLICATION_FACTOR_CONFIG = "scylladb.keyspace.replication.factor";
  private static final String KEYSPACE_REPLICATION_FACTOR_DOC = "The replication factor to use "
          + "if a keyspace is created by the connector. The Replication Factor (RF) is equivalent "
          + "to the number of nodes where data (rows and partitions) are replicated. Data is replicated to multiple (RF=N) nodes";

  public static final String COMPRESSION_CONFIG = "scylladb.compression";
  private static final String COMPRESSION_DOC = "Compression algorithm to use when communicating with ScyllaDB. "
          + "Valid Values are NONE, SNAPPY, LZ4.";

  public static final String TABLE_MANAGE_ENABLED_CONFIG = "scylladb.table.manage.enabled";
  private static final String SCHEMA_MANAGE_CREATE_DOC = "Flag to determine if the connector should manage the table.";

  public static final String TABLE_CREATE_COMPRESSION_ALGORITHM_CONFIG = "scylladb.table.create.compression.algorithm";
  private static final String TABLE_CREATE_COMPRESSION_ALGORITHM_DOC = "Compression algorithm to use when the table is created. "
      + "Valid Values are NONE, SNAPPY, LZ4, DEFLATE.";


  public static final String OFFSET_STORAGE_TABLE_CONF = "scylladb.offset.storage.table";
  private static final String OFFSET_STORAGE_TABLE_DOC = "The table within the ScyllaDB keyspace "
          + "to store the offsets that have been read from Kafka.";

  public static final String ENABLE_OFFSET_STORAGE_TABLE = "scylladb.offset.storage.table.enable";
  private static final Boolean ENABLE_OFFSET_STORAGE_TABLE_DEFAULT = true;
  private static final String ENABLE_OFFSET_STORAGE_TABLE_DOC = "If true, Kafka consumer offsets will "
          + "be stored in ScyllaDB table. If false, connector will skip writing offset information into "
          + "ScyllaDB.";

  public static final String EXECUTE_STATEMENT_TIMEOUT_MS_CONF = "scylladb.execute.timeout.ms";
  public static final Long EXECUTE_STATEMENT_TIMEOUT_MS_DEFAULT = 30000L;
  private static final String EXECUTE_STATEMENT_TIMEOUT_MS_DOC = "The timeout for executing a ScyllaDB statement.";

  public static final String SSL_TRUSTSTORE_PATH_CONFIG = "scylladb.ssl.truststore.path";
  private static final String SSL_TRUSTSTORE_PATH_DOC = "Path to the Java Truststore.";

  public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "scylladb.ssl.truststore.password";
  private static final String SSL_TRUSTSTORE_PASSWORD_DOC = "Password to open the Java Truststore with.";

  public static final String SSL_KEYSTORE_PATH_CONFIG = "scylladb.ssl.keystore.path";
  private static final String SSL_KEYSTORE_PATH_DOC = "Path to the Java Keystore";

  public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "scylladb.ssl.keystore.password";
  private static final String SSL_KEYSTORE_PASSWORD_DOC = "Password to open the Java Keystore with.";

  public static final String SSL_CIPHER_SUITES_CONFIG = "scylladb.ssl.cipherSuites";
  private static final String SSL_CIPHER_SUITES_DOC = "The cipher suites to enable. "
          + "Defaults to none, resulting in a ``minimal quality of service`` according to JDK documentation.";

  private static final String SSL_HOSTNAME_VERIFICATION_CONFIG = "scylladb.ssl.hostname.verification";

  private static final String SSL_HOSTNAME_VERIFICATION_DOC = "Whether or not to require validation that the hostname" +
      " of the server certificate's common name matches the hostname of the server being connected to.";
  public static final String TTL_CONFIG = "scylladb.ttl";
  /*If TTL value is not specified then skip setting ttl value while making insert query*/
  public static final Integer TTL_DEFAULT = null;
  private static final String TTL_DOC = "The retention period for the data in ScyllaDB. "
          + "After this interval elapses, ScyllaDB will remove these records. "
          + "If this configuration is not provided, the Sink Connector will perform "
          + "insert operations in ScyllaDB  without TTL setting.";

  public static final String LOAD_BALANCING_LOCAL_DC_CONFIG = "scylladb.loadbalancing.localdc";
  private static final String LOAD_BALANCING_LOCAL_DC_DEFAULT = "";
  private static final String LOAD_BALANCING_LOCAL_DC_DOC = "The case-sensitive Data Center name "
          + "local to the machine on which the connector is running. It is a recommended config if "
          + "we have more than one DC.";

  public static final String BEHAVIOR_ON_ERROR_CONFIG = "behavior.on.error";
  public static final String BEHAVIOR_ON_ERROR_DEFAULT = BehaviorOnError.FAIL.name();
  private static final String BEHAVIOR_ON_ERROR_DISPLAY = "Behavior On Error";
  private static final String BEHAVIOR_ON_ERROR_DOC = "Error handling behavior setting. "
          + "Must be configured to one of the following:\n"
          + "``fail``\n"
          + "The Connector throws ConnectException and stops processing records "
          + "when an error occurs while processing or inserting records into ScyllaDB.\n"
          + "``ignore``\n"
          + "Continues to process next set of records "
          + "when error occurs while processing or inserting records into ScyllaDB.\n"
          + "``log``\n"
          + "Logs the error via connect-reporter when an error occurs while processing or "
          + "inserting records into ScyllaDB and continues to process next set of records, "
          + "available in the kafka topics.";

  public static final String SCYLLADB_GROUP = "ScyllaDB";
  public static final String CONNECTION_GROUP = "Connection";
  public static final String SSL_GROUP = "SSL";
  public static final String KEYSPACE_GROUP = "Keyspace";
  public static final String TABLE_GROUP = "Table";
  public static final String WRITE_GROUP = "Write";

  public static ConfigDef config() {
    return new ConfigDef()
            .define(
                    CONTACT_POINTS_CONFIG,
                    ConfigDef.Type.STRING,
                    "localhost",
                    ConfigDef.Importance.HIGH,
                    CONTACT_POINTS_DOC,
                    CONNECTION_GROUP,
                    0,
                    ConfigDef.Width.SHORT,
                    "Contact Point(s)")
            .define(
                    PORT_CONFIG,
                    ConfigDef.Type.INT,
                    9042,
                    ValidPort.of(),
                    ConfigDef.Importance.MEDIUM,
                    PORT_DOC,
                    CONNECTION_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    "Port")
            .define(
                    LOAD_BALANCING_LOCAL_DC_CONFIG,
                    ConfigDef.Type.STRING,
                    LOAD_BALANCING_LOCAL_DC_DEFAULT,
                    ConfigDef.Importance.HIGH,
                    LOAD_BALANCING_LOCAL_DC_DOC,
                    CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.LONG,
                    "Load Balancing Local DC")
            .define(
                    SECURITY_ENABLE_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    ConfigDef.Importance.HIGH,
                    SECURITY_ENABLE_DOC,
                    CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    "Security Enabled?")
            .define(
                    USERNAME_CONFIG,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.HIGH,
                    USERNAME_DOC,
                    CONNECTION_GROUP,
                    3,
                    ConfigDef.Width.SHORT,
                    "Username",
                    Arrays.asList(PASSWORD_CONFIG, SECURITY_ENABLE_CONFIG))
            .define(
                    PASSWORD_CONFIG,
                    ConfigDef.Type.PASSWORD,
                    null,
                    ConfigDef.Importance.HIGH,
                    PASSWORD_DOC,
                    CONNECTION_GROUP,
                    4,
                    ConfigDef.Width.SHORT,
                    "Password",
                    Arrays.asList(USERNAME_CONFIG, SECURITY_ENABLE_CONFIG))
            .define(
                    COMPRESSION_CONFIG,
                    ConfigDef.Type.STRING,
                    "none",
                    ConfigDef.ValidString.in(CLIENT_COMPRESSION.toArray(new String[0])),
                    ConfigDef.Importance.LOW,
                    COMPRESSION_DOC,
                    CONNECTION_GROUP,
                    5,
                    ConfigDef.Width.SHORT,
                    "Compression",
                    new ListRecommender(Arrays.asList(CLIENT_COMPRESSION.toArray())))
            .define(
                    SSL_ENABLED_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    ConfigDef.Importance.HIGH,
                    SSL_ENABLED_DOC,
                    CONNECTION_GROUP,
                    6,
                    ConfigDef.Width.SHORT,
                    "SSL Enabled?")
            .define(
                    SSL_TRUSTSTORE_PATH_CONFIG,
                    ConfigDef.Type.STRING,
                    null,
                    new NullOrReadableFile(),
                    ConfigDef.Importance.MEDIUM,
                    SSL_TRUSTSTORE_PATH_DOC,
                    SSL_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    "SSL Truststore Path",
                    Collections.singletonList(SSL_ENABLED_CONFIG),
                    new VisibleIfEqual(SSL_ENABLED_CONFIG, true))
            .define(
                    SSL_TRUSTSTORE_PASSWORD_CONFIG,
                    ConfigDef.Type.PASSWORD,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    SSL_TRUSTSTORE_PASSWORD_DOC,
                    SSL_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    "SSL Truststore Password",
                    Collections.singletonList(SSL_ENABLED_CONFIG),
                    new VisibleIfEqual(SSL_ENABLED_CONFIG, true))
            .define(
                    SSL_KEYSTORE_PATH_CONFIG,
                    ConfigDef.Type.STRING,
                    null,
                    new NullOrReadableFile(),
                    ConfigDef.Importance.MEDIUM,
                    SSL_KEYSTORE_PATH_DOC,
                    SSL_GROUP,
                    3,
                    ConfigDef.Width.SHORT,
                    "SSL Keystore Path",
                    Collections.singletonList(SSL_ENABLED_CONFIG),
                    new VisibleIfEqual(SSL_ENABLED_CONFIG, true))
            .define(
                    SSL_KEYSTORE_PASSWORD_CONFIG,
                    ConfigDef.Type.PASSWORD,
                    null,
                    ConfigDef.Importance.MEDIUM,
                    SSL_KEYSTORE_PASSWORD_DOC,
                    SSL_GROUP,
                    4,
                    ConfigDef.Width.SHORT,
                    "SSL Keystore Password",
                    Collections.singletonList(SSL_ENABLED_CONFIG),
                    new VisibleIfEqual(SSL_ENABLED_CONFIG, true))
            .define(
                    SSL_CIPHER_SUITES_CONFIG,
                    ConfigDef.Type.LIST,
                    (Object) Collections.EMPTY_LIST,
                    ConfigDef.Importance.HIGH,
                    SSL_CIPHER_SUITES_DOC,
                    SSL_GROUP,
                    5,
                    ConfigDef.Width.LONG,
                    "The cipher suites to enable",
                    Collections.singletonList(SSL_ENABLED_CONFIG),
                    new VisibleIfEqual(SSL_ENABLED_CONFIG, true))
            .define(SSL_HOSTNAME_VERIFICATION_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    ConfigDef.Importance.MEDIUM,
                    SSL_HOSTNAME_VERIFICATION_DOC,
                    SSL_GROUP,
                    6,
                    ConfigDef.Width.SHORT,
                    "Hostname verification?",
                    new VisibleIfEqual(SSL_ENABLED_CONFIG, true))
            .define(
                    CONSISTENCY_LEVEL_CONFIG,
                    ConfigDef.Type.STRING,
                    ConsistencyLevel.LOCAL_QUORUM.toString(),
                    ValidEnum.of(DefaultConsistencyLevel.class),
                    ConfigDef.Importance.HIGH,
                    CONSISTENCY_LEVEL_DOC,
                    WRITE_GROUP,
                    0,
                    ConfigDef.Width.SHORT,
                    "Consistency Level",
                    new ListRecommender(Arrays.asList(DefaultConsistencyLevel.values())))
            .define(
                    DELETES_ENABLE_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.HIGH,
                    DELETES_ENABLE_DOC,
                    WRITE_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    "Perform Deletes")
            .define(
                    KEYSPACE_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    KEYSPACE_DOC,
                    KEYSPACE_GROUP,
                    0,
                    ConfigDef.Width.SHORT,
                    "ScyllaDB Keyspace")
            .define(
                    KEYSPACE_CREATE_ENABLED_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.HIGH,
                    KEYSPACE_CREATE_ENABLED_DOC,
                    KEYSPACE_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    "Create Keyspace")
            .define(
                    KEYSPACE_REPLICATION_FACTOR_CONFIG,
                    ConfigDef.Type.INT,
                    3,
                    ConfigDef.Range.atLeast(1),
                    ConfigDef.Importance.HIGH,
                    KEYSPACE_REPLICATION_FACTOR_DOC,
                    KEYSPACE_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    "Keyspace replication factor",
                    Collections.singletonList(KEYSPACE_CREATE_ENABLED_CONFIG),
                    new VisibleIfEqual(KEYSPACE_CREATE_ENABLED_CONFIG, true))
            .define(
                    TABLE_MANAGE_ENABLED_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.HIGH,
                    SCHEMA_MANAGE_CREATE_DOC,
                    TABLE_GROUP,
                    0,
                    ConfigDef.Width.SHORT,
                    "Manage Table Schema(s)?")
            .define(
                    TABLE_CREATE_COMPRESSION_ALGORITHM_CONFIG,
                    ConfigDef.Type.STRING,
                    "none",
                    ConfigDef.ValidString.in(TABLE_COMPRESSORS.toArray(new String[0])),
                    ConfigDef.Importance.MEDIUM,
                    TABLE_CREATE_COMPRESSION_ALGORITHM_DOC,
                    TABLE_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    "Table Compression",
                    Collections.singletonList(TABLE_MANAGE_ENABLED_CONFIG),
                    new VisibleIfEqual(TABLE_MANAGE_ENABLED_CONFIG, true, Arrays.asList(TABLE_COMPRESSORS.toArray(new String[0]))))
            .define(
                    OFFSET_STORAGE_TABLE_CONF,
                    ConfigDef.Type.STRING,
                    "kafka_connect_offsets",
                    ConfigDef.Importance.LOW,
                    OFFSET_STORAGE_TABLE_DOC,
                    TABLE_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    "Offset storage table")
            .define(
                    EXECUTE_STATEMENT_TIMEOUT_MS_CONF,
                    ConfigDef.Type.LONG,
                    EXECUTE_STATEMENT_TIMEOUT_MS_DEFAULT,
                    ConfigDef.Range.atLeast(0),
                    ConfigDef.Importance.LOW,
                    EXECUTE_STATEMENT_TIMEOUT_MS_DOC,
                    WRITE_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    "Execute statement timeout (in ms)")
            .define(
                    TTL_CONFIG,
                    ConfigDef.Type.INT,
                    TTL_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    TTL_DOC,
                    WRITE_GROUP,
                    3,
                    ConfigDef.Width.SHORT,
                    "Time to live (in seconds)")
            .define(
                    ENABLE_OFFSET_STORAGE_TABLE,
                    ConfigDef.Type.BOOLEAN,
                    ENABLE_OFFSET_STORAGE_TABLE_DEFAULT,
                    ConfigDef.Importance.MEDIUM,
                    ENABLE_OFFSET_STORAGE_TABLE_DOC,
                    WRITE_GROUP,
                    4,
                    ConfigDef.Width.SHORT,
                    "Enable offset stored in ScyllaDB")
            .define(
                    BEHAVIOR_ON_ERROR_CONFIG,
                    ConfigDef.Type.STRING,
                    BEHAVIOR_ON_ERROR_DEFAULT,
                    ConfigDef.ValidString.in(toStringArray(BehaviorOnError.values())),
                    ConfigDef.Importance.MEDIUM,
                    BEHAVIOR_ON_ERROR_DOC,
                    SCYLLADB_GROUP,
                    0,
                    ConfigDef.Width.NONE,
                    BEHAVIOR_ON_ERROR_DISPLAY,
                    new ListRecommender(Arrays.asList(toStringArray(BehaviorOnError.values())))
            );
  }

  private String tryMatchTopicName(final String name) {
    final Matcher m = ScyllaDbSinkConnectorConfig.TOPIC_KS_TABLE_SETTING_PATTERN.matcher(name);
    if (m.matches()) {
      return m.group(1);
    }
    throw new IllegalArgumentException("The setting: " + name + " does not match topic.keyspace.table nor topic.codec regular expression pattern");
  }

  private boolean hasTopicWiseConfigSuffix(final String name) {
    for (String suffix : TOPIC_WISE_CONFIGS_VALID_SUFFIXES) {
      if (name.endsWith(suffix)) return true;
    }
    return false;
  }

  private static String[] toStringArray(Object[] arr){
    return Arrays.stream(arr).map(Object::toString).toArray(String[]::new);
  }

  /**
   * Enums for behavior on error.
   */
  public enum BehaviorOnError {
    IGNORE,
    LOG,
    FAIL
  }

  public boolean isOffsetEnabledInScyllaDb() {
    return getBoolean(ENABLE_OFFSET_STORAGE_TABLE);
  }

  public static void main(String[] args) {
    System.out.println(config().toEnrichedRst());
  }

}
