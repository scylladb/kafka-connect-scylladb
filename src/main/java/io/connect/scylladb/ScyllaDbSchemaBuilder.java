package io.connect.scylladb;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.*;

import com.datastax.oss.driver.api.querybuilder.schema.AlterTableAddColumnEnd;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.AlterTableWithOptionsEnd;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.base.MoreObjects;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;

import com.datastax.oss.driver.shaded.guava.common.collect.ComparisonChain;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.connect.scylladb.topictotable.TopicConfigs;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class ScyllaDbSchemaBuilder extends SchemaChangeListenerBase {

  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSchemaBuilder.class);

  private static final Object DEFAULT = new Object();

  private final ScyllaDbSinkConnectorConfig config;
  private final Cache<ScyllaDbSchemaKey, Object> schemaLookup;

  final ScyllaDbSession session;

  public ScyllaDbSchemaBuilder(ScyllaDbSession session, ScyllaDbSinkConnectorConfig config) {
    this.session = session;
    this.config = config;
    this.schemaLookup = CacheBuilder.newBuilder()
        .expireAfterWrite(500L, TimeUnit.SECONDS)
        .build();
  }

  @Override
  public void onTableUpdated(com.datastax.oss.driver.api.core.metadata.schema.TableMetadata current, com.datastax.oss.driver.api.core.metadata.schema.TableMetadata previous) {
    final com.datastax.oss.driver.api.core.metadata.schema.TableMetadata actual;
    if (null != current) {
      actual = current;
    } else if (null != previous) {
      actual = previous;
    } else {
      actual = null;
    }

    if (null != actual) {
      final String keyspace = actual.getKeyspace().toString();
      if (this.config.keyspace.equalsIgnoreCase(keyspace)) {
        ScyllaDbSchemaKey key =
            ScyllaDbSchemaKey.of(actual.getKeyspace().toString(), actual.getName().toString());
        log.info("onTableChanged() - {} changed. Invalidating...", key);
        this.schemaLookup.invalidate(key);
        this.session.onTableChanged(actual.getKeyspace().toString(), actual.getName().toString());
      }
    }
  }

  DataType dataType(Schema schema) {
    final DataType dataType;

    if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
      dataType = DataTypes.TIMESTAMP;
    } else if (Time.LOGICAL_NAME.equals(schema.name())) {
      dataType = DataTypes.TIME;
    } else if (Date.LOGICAL_NAME.equals(schema.name())) {
      dataType = DataTypes.DATE;
    } else if (Decimal.LOGICAL_NAME.equals(schema.name())) {
      dataType = DataTypes.DECIMAL;
    } else {
      switch (schema.type()) {
        case MAP:
          final DataType mapKeyType = dataType(schema.keySchema());
          final DataType mapValueType = dataType(schema.valueSchema());
          dataType = DataTypes.mapOf(mapKeyType, mapValueType);
          break;
        case ARRAY:
          final DataType listValueType = dataType(schema.valueSchema());
          dataType = DataTypes.listOf(listValueType);
          break;
        case BOOLEAN:
          dataType = DataTypes.BOOLEAN;
          break;
        case BYTES:
          dataType = DataTypes.BLOB;
          break;
        case FLOAT32:
          dataType = DataTypes.FLOAT;
          break;
        case FLOAT64:
          dataType = DataTypes.DOUBLE;
          break;
        case INT8:
          dataType = DataTypes.TINYINT;
          break;
        case INT16:
          dataType = DataTypes.SMALLINT;
          break;
        case INT32:
          dataType = DataTypes.INT;
          break;
        case INT64:
          dataType = DataTypes.BIGINT;
          break;
        case STRING:
          dataType = DataTypes.TEXT;
          break;
        default:
          throw new DataException(
              String.format("Unsupported type %s", schema.type())
          );
      }
    }
    return dataType;
  }

  void alter(
          final ScyllaDbSchemaKey key,
          String tableName,
          SinkRecord record,
          TableMetadata.Table tableMetadata,
          TopicConfigs topicConfigs
  ) {
    Preconditions.checkNotNull(tableMetadata, "tableMetadata cannot be null.");
    Preconditions.checkNotNull(record.valueSchema(), "valueSchema cannot be null.");
    log.trace("alter() - tableMetadata = '{}' ", tableMetadata);

    Map<String, DataType> addedColumns = new LinkedHashMap<>();

    if (topicConfigs != null && topicConfigs.isScyllaColumnsMapped()) {
      if (topicConfigs.getTablePartitionKeyMap().keySet().size() != tableMetadata.primaryKey().size()) {
        throw new DataException(
                String.format(
                        "Cannot alter primary key of a ScyllaDb Table. Existing primary key: '%s', "
                                + "Primary key mapped in 'topic.my_topic.my_ks.my_table.mapping' config: '%s",
                        Joiner.on("', '").join(tableMetadata.primaryKey()),
                        Joiner.on("', '").join(topicConfigs.getTablePartitionKeyMap().keySet())
                )
        );
      }

      for (Map.Entry<String, TopicConfigs.KafkaScyllaColumnMapper> entry: topicConfigs.getTableColumnMap().entrySet()) {
        String columnName = entry.getValue().getScyllaColumnName();
        log.trace("alter for mapping() - Checking if table has '{}' column.", columnName);
        final TableMetadata.Column columnMetadata = tableMetadata.columnMetadata(columnName);

        if (null == columnMetadata) {
          log.debug("alter for mapping() - Adding column '{}'", columnName);
          final DataType dataType = dataType(entry.getValue().getKafkaRecordField().schema());
          addedColumns.put(CqlIdentifier.fromInternal(columnName).asCql(true), dataType);

        } else {
          log.trace("alter for mapping() - Table already has '{}' column.", columnName);
        }
      }
    } else {
      for (final Field field : record.valueSchema().fields()) {
        log.trace("alter() - Checking if table has '{}' column.", field.name());
        final TableMetadata.Column columnMetadata = tableMetadata.columnMetadata(field.name());

        if (null == columnMetadata) {
          log.debug("alter() - Adding column '{}'", field.name());
          DataType dataType = dataType(field.schema());
          addedColumns.put(CqlIdentifier.fromInternal(field.name()).asCql(true), dataType);
        } else {
          log.trace("alter() - Table already has '{}' column.", field.name());
        }
      }
    }

    /*
    ScyllaDb is a little weird. It will not allow more than one column in an alter statement. It
    looks like this is a limitation of CQL in general. Check out this issue for more.
    https://datastax-oss.atlassian.net/browse/JAVA-731
     */

    if (!addedColumns.isEmpty()) {
      final AlterTableStart alterTable = alterTable(this.config.keyspace, tableName);
      if (!this.config.tableManageEnabled) {
        List<String> requiredAlterStatements = addedColumns.entrySet().stream()
                .map(e -> alterTable.addColumn(e.getKey(), e.getValue()).asCql())
                .collect(Collectors.toList());

        throw new DataException(
                String.format(
                        "Alter statement(s) needed. Missing column(s): '%s'\n%s;",
                        Joiner.on("', '").join(addedColumns.keySet()),
                        Joiner.on(';').join(requiredAlterStatements)
                )
        );
      } else {
        AlterTableWithOptionsEnd alterTableWithOptionsEnd = null;
        if (config.tableCompressionAlgorithm.equals("none")) {
          alterTableWithOptionsEnd = alterTable.withNoCompression();
        }
        else {
          alterTableWithOptionsEnd = alterTable.withOption("compression", ImmutableMap.of("sstable_compression", config.tableCompressionAlgorithm));
        }
        String query = alterTableWithOptionsEnd.asCql();
        this.session.executeQuery(query);
        for (Map.Entry<String, DataType> e : addedColumns.entrySet()) {
          final String columnName = e.getKey();
          final DataType dataType = e.getValue();
          final AlterTableAddColumnEnd alterStatement = alterTable.addColumn(columnName, dataType);
          this.session.executeStatement(alterStatement.build());
        }
        this.session.onTableChanged(this.config.keyspace, tableName);
      }
    }

    this.schemaLookup.put(key, DEFAULT);
  }

  public void build(String tableName, SinkRecord record, TopicConfigs topicConfigs) {
    log.trace("build() - tableName = '{}'", tableName);
    final ScyllaDbSchemaKey key = ScyllaDbSchemaKey.of(this.config.keyspace, tableName);
    if (null != this.schemaLookup.getIfPresent(key)) {
      return;
    }
    if (null == record.keySchema() || null == record.valueSchema()) {
      log.warn(
              "build() - Schemaless mode detected. Cannot generate DDL so assuming table is correct."
      );
      this.schemaLookup.put(key, DEFAULT);
      return;
    }


    final TableMetadata.Table tableMetadata = this.session.tableMetadata(tableName);

    if (null != tableMetadata) {
      alter(key, tableName, record, tableMetadata, topicConfigs);
    } else {
      create(key, tableName, record, topicConfigs);
    }
  }

  void create(
          final ScyllaDbSchemaKey key,
          String tableName,
          SinkRecord record,
          TopicConfigs topicConfigs
  ) {
    Schema keySchema = record.keySchema();
    Schema valueSchema = record.valueSchema();
    log.trace("create() - tableName = '{}'", tableName);
    Preconditions.checkState(
            Schema.Type.STRUCT == keySchema.type(),
            "record.keySchema() must be a struct. Received '%s'",
            keySchema.type()
    );
    Preconditions.checkState(
            !keySchema.fields().isEmpty(),
            "record.keySchema() must have some fields."
    );
    if (topicConfigs != null && topicConfigs.isScyllaColumnsMapped()) {
      Preconditions.checkState(
              Schema.Type.STRUCT == valueSchema.type(),
              "record.valueSchema() must be a struct. Received '%s'",
              valueSchema.type()
      );
      Preconditions.checkState(
              !valueSchema.fields().isEmpty(),
              "record.valueSchema() must have some fields."
      );
    } else {
      for (final Field keyField : keySchema.fields()) {
        log.trace(
                "create() - Checking key schema against value schema. fieldName={}",
                keyField.name()
        );
        final Field valueField = valueSchema.field(keyField.name());

        if (null == valueField) {
          throw new DataException(
                  String.format(
                          "record.valueSchema() must contain all of the fields in record.keySchema(). "
                                  + "record.keySchema() is used by the connector to determine the key for the "
                                  + "table. record.valueSchema() is missing field '%s'. record.valueSchema() is "
                                  + "used by the connector to persist data to the table in ScyllaDb. Here are "
                                  + "the available fields for record.valueSchema(%s) and record.keySchema(%s).",
                          keyField.name(),
                          Joiner.on(", ").join(
                                  valueSchema.fields().stream().map(Field::name).collect(Collectors.toList())
                          ),
                          Joiner.on(", ").join(
                                  keySchema.fields().stream().map(Field::name).collect(Collectors.toList())
                          )
                  )
          );
        }
      }
    }

    CreateTableStart createStart = createTable(this.config.keyspace, tableName);
    CreateTable create = null;

    if (topicConfigs != null && topicConfigs.isScyllaColumnsMapped()) {
      for (Map.Entry<String, TopicConfigs.KafkaScyllaColumnMapper> entry: topicConfigs.getTablePartitionKeyMap().entrySet()) {
        final DataType dataType = dataType(entry.getValue().getKafkaRecordField().schema());
        final String columnName = CqlIdentifier.fromInternal(entry.getValue().getScyllaColumnName()).asCql(true);
        if (create == null) {
          create = createStart.withPartitionKey(columnName, dataType);
        }
        else {
          create = create.withPartitionKey(columnName, dataType);
        }
      }
      for (Map.Entry<String, TopicConfigs.KafkaScyllaColumnMapper> entry: topicConfigs.getTableColumnMap().entrySet()) {
        final DataType dataType = dataType(entry.getValue().getKafkaRecordField().schema());
        final String columnName = CqlIdentifier.fromInternal(entry.getValue().getScyllaColumnName()).asCql(true);
        create = create.withColumn(columnName, dataType);
      }
    } else {
      Set<String> fields = new HashSet<>();
      for (final Field keyField : keySchema.fields()) {
        final DataType dataType = dataType(keyField.schema());
        final String columnName = CqlIdentifier.fromInternal(keyField.name()).asCql(true);
        if (create == null) {
          create = createStart.withPartitionKey(columnName, dataType);
        }
        else {
          create = create.withPartitionKey(columnName, dataType);
        }
        fields.add(columnName);
      }

      for (final Field valueField : valueSchema.fields()) {
        final String columnName = CqlIdentifier.fromInternal(valueField.name()).asCql(true);
        final DataType dataType = dataType(valueField.schema());
        if (fields.contains(columnName)) {
          log.trace("create() - Skipping '{}' because it's already in the key.", valueField.name());
          continue;
        }
        create = create.withColumn(columnName, dataType);
      }
    }

    CreateTableWithOptions createWithOptions;

    if (!Strings.isNullOrEmpty(valueSchema.doc())) {
      createWithOptions = create.withComment(valueSchema.doc());
    } else {
      createWithOptions = create.withComment("Record valueSchema.doc() was null or empty");
    }


    if (this.config.tableManageEnabled) {
      if (config.tableCompressionAlgorithm.equals("none")) {
        createWithOptions = createWithOptions.withNoCompression();
      }
      else {
        createWithOptions = createWithOptions.withOption("compression", ImmutableMap.of("sstable_compression", config.tableCompressionAlgorithm));
      }
      log.info("create() - Adding table {}.{}\n{}", this.config.keyspace, tableName, Arrays.toString(createWithOptions.getOptions().entrySet().toArray()));
      session.executeStatement(createWithOptions.build());
    } else {
      throw new DataException(
              String.format("Create statement needed:\n%s", create)
      );
    }
    this.schemaLookup.put(key, DEFAULT);
  }

  static class ScyllaDbSchemaKey implements Comparable<ScyllaDbSchemaKey> {
    final String tableName;
    final String keyspace;


    private ScyllaDbSchemaKey(String keyspace, String tableName) {
      this.tableName = tableName;
      this.keyspace = keyspace;
    }


    @Override
    public int compareTo(ScyllaDbSchemaKey that) {
      return ComparisonChain.start()
          .compare(this.keyspace, that.keyspace)
          .compare(this.tableName, that.tableName)
          .result();
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.keyspace, this.tableName);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("keyspace", this.keyspace)
          .add("tableName", this.tableName)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ScyllaDbSchemaKey) {
        return 0 == compareTo((ScyllaDbSchemaKey) obj);
      } else {
        return false;
      }
    }

    public static ScyllaDbSchemaKey of(String keyspace, String tableName) {
      return new ScyllaDbSchemaKey(keyspace, tableName);
    }
  }

}
