package io.connect.scylladb;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.connect.scylladb.ScyllaDbSchemaBuilder.ScyllaDbSchemaKey;
import io.connect.scylladb.topictotable.TopicConfigs;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

class ScyllaDbSessionImpl implements ScyllaDbSession {
  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSessionImpl.class);

  private final ScyllaDbSinkConnectorConfig config;
  private final CqlSession session;
  private final ScyllaDbSchemaBuilder schemaBuilder;
  private boolean sessionValid = true;
  private final Map<ScyllaDbSchemaKey, TableMetadata.Table> tableMetadataCache;
  private final Map<ScyllaDbSchemaKey, RecordToBoundStatementConverter> deleteStatementCache;
  private final Map<ScyllaDbSchemaKey, RecordToBoundStatementConverter> insertStatementCache;

  ScyllaDbSessionImpl(ScyllaDbSinkConnectorConfig config, CqlSession session) {
    this.session = session;
    this.config = config;
    this.schemaBuilder = new ScyllaDbSchemaBuilder(this, config);
    this.tableMetadataCache = new HashMap<>();
    this.deleteStatementCache = new HashMap<>();
    this.insertStatementCache = new HashMap<>();
  }

  @Override
  public ResultSet executeStatement(Statement statement) {
    log.trace("executeStatement() - Executing statement\n{}", statement);
    return this.session.execute(statement);
  }

  @Override
  public CompletionStage<AsyncResultSet> executeStatementAsync(Statement statement) {
    log.trace("executeStatement() - Executing statement\n{}", statement);
    return this.session.executeAsync(statement);
  }

  @Override
  public ResultSet executeQuery(String query) {
    log.trace("executeQuery() - Executing query\n{}", query);
    return this.session.execute(query);
  }

  @Override
  public KeyspaceMetadata keyspaceMetadata(String keyspaceName){
    log.trace("keyspaceMetadata() - keyspaceName = '{}'", keyspaceName);
    return session.getMetadata().getKeyspace(keyspaceName).get();
  }

  @Override
  public boolean keyspaceExists(String keyspaceName){
    return session.getMetadata().getKeyspace(keyspaceName).isPresent();
  }

  @Override
  public TableMetadata.Table tableMetadata(String keyspace, String tableName) {
    log.trace("tableMetadata() - keyspace = '{}', tableName = '{}'", keyspace, tableName);
    final ScyllaDbSchemaKey key = ScyllaDbSchemaKey.of(keyspace, tableName);
    TableMetadata.Table result = this.tableMetadataCache.get(key);

    if (null == result) {
      final KeyspaceMetadata keyspaceMetadata = session.getMetadata().getKeyspace(keyspace)
          .orElseThrow(() -> new ConnectException(String.format(
              "Keyspace '%s' does not exist. It must be created before writing to table '%s'.",
              keyspace, tableName)));
      final Optional<com.datastax.oss.driver.api.core.metadata.schema.TableMetadata> tableMetadata = keyspaceMetadata.getTable(tableName);
      if (tableMetadata.isPresent()) {
        result = new TableMetadataImpl.TableImpl(tableMetadata.get());
        this.tableMetadataCache.put(key, result);
      } else {
        log.warn("Could not find metadata for table {} in keyspace {}. Are you sure the table exists?", tableName, keyspaceMetadata.getName().asCql(false));
        result = null;
      }
    }

    return result;
  }

  /**
   * Metadata of a table that statements are prepared for. The table can be missing because the
   * connector cannot create it for schemaless records, or because it was dropped.
   */
  private TableMetadata.Table existingTableMetadata(String keyspace, String tableName) {
    final TableMetadata.Table result = tableMetadata(keyspace, tableName);
    if (null == result) {
      throw new TableConfigurationException(String.format(
          "Table '%s.%s' does not exist. The connector creates tables only from records with a schema, "
              + "so create it before writing records without one, and check that it was not dropped.",
          keyspace, tableName));
    }
    return result;
  }

  @Override
  public boolean tableExists(String keyspace, String tableName) {
    return null != tableMetadata(keyspace, tableName);
  }

  @Override
  public void createOrAlterTable(String keyspace, String tableName, SinkRecord record, TopicConfigs topicConfigs) {
    this.schemaBuilder.build(keyspace, tableName, record, topicConfigs);
  }

  @Override
  public boolean isValid() {
    return this.sessionValid;
  }

  @Override
  public void setInvalid() {
    this.sessionValid = false;
  }


  @Override
  public RecordToBoundStatementConverter delete(String keyspace, String tableName) {
    return this.deleteStatementCache.computeIfAbsent(
        ScyllaDbSchemaKey.of(keyspace, tableName),
        new Function<ScyllaDbSchemaKey, RecordToBoundStatementConverter>() {
          @Override
          public RecordToBoundStatementConverter apply(ScyllaDbSchemaKey key) {
            DeleteSelection statementStart = QueryBuilder.deleteFrom(keyspace, tableName);
            Delete statement = null;
            TableMetadata.Table tableMetadata = existingTableMetadata(keyspace, tableName);
            for (TableMetadata.Column columnMetadata : tableMetadata.primaryKey()) {
              if (statement == null) {
                statement = statementStart.whereColumn(columnMetadata.getName()).isEqualTo(QueryBuilder.bindMarker(columnMetadata.getName()));
              }
              else {
                statement = statement.whereColumn(columnMetadata.getName()).isEqualTo(QueryBuilder.bindMarker(columnMetadata.getName()));
              }
            }
            assert statement != null;
            log.debug("delete() - Preparing statement. '{}'", statement.asCql());
            PreparedStatement preparedStatement = session.prepare(statement.build());
            return new RecordToBoundStatementConverter(preparedStatement);
          }
        }
    );
  }

  private PreparedStatement createInsertPreparedStatement(String keyspace, String tableName, TopicConfigs topicConfigs) {
    InsertInto insertInto = QueryBuilder.insertInto(keyspace, tableName);
    RegularInsert regularInsert = null;
    TableMetadata.Table tableMetadata = existingTableMetadata(keyspace, tableName);
    for (TableMetadata.Column columnMetadata : tableMetadata.columns()) {
      if (regularInsert == null) {
        regularInsert = insertInto.value(CqlIdentifier.fromInternal(columnMetadata.getName()), QueryBuilder.bindMarker(columnMetadata.getName()));
      } else {
        regularInsert = regularInsert.value(CqlIdentifier.fromInternal(columnMetadata.getName()), QueryBuilder.bindMarker(columnMetadata.getName()));
      }
    }
    assert regularInsert != null;
    log.debug("insert() - Preparing statement. '{}'", regularInsert.asCql());
    if (topicConfigs != null) {
      return (topicConfigs.getTtl() == null) ? session.prepare(regularInsert.build()) :
              session.prepare(regularInsert.usingTtl(topicConfigs.getTtl()).build());
    } else {
      return (config.ttl == null) ? session.prepare(regularInsert.build()) :
              session.prepare(regularInsert.usingTtl(config.ttl).build());
    }
  }

  @Override
  public RecordToBoundStatementConverter insert(String keyspace, String tableName, TopicConfigs topicConfigs) {
    if (topicConfigs != null && topicConfigs.getTtl() != null) {
      PreparedStatement preparedStatement = createInsertPreparedStatement(keyspace, tableName, topicConfigs);
      return new RecordToBoundStatementConverter(preparedStatement);
    } else {
      return this.insertStatementCache.computeIfAbsent(
              ScyllaDbSchemaKey.of(keyspace, tableName),
              new Function<ScyllaDbSchemaKey, RecordToBoundStatementConverter>() {
                @Override
                public RecordToBoundStatementConverter apply(ScyllaDbSchemaKey key) {
                  PreparedStatement preparedStatement = createInsertPreparedStatement(keyspace, tableName, topicConfigs);
                  return new RecordToBoundStatementConverter(preparedStatement);
                }
              }
      );
    }
  }

  private PreparedStatement offsetPreparedStatement;

  @Override
  public BoundStatement getInsertOffsetStatement(
      TopicPartition topicPartition,
      OffsetAndMetadata metadata) {
    if (null == this.offsetPreparedStatement) {
      this.offsetPreparedStatement =
              createInsertPreparedStatement(this.config.keyspace, this.config.offsetStorageTable, null);
    }
    log.debug(
            "getAddOffsetsStatement() - Setting offset to {}:{}:{}",
            topicPartition.topic(),
            topicPartition.partition(),
            metadata.offset()
    );
    BoundStatement statement = offsetPreparedStatement.bind();
    statement = statement.setString("topic", topicPartition.topic());
    statement = statement.setInt("partition", topicPartition.partition());
    statement = statement.setLong("offset", metadata.offset());
    return statement;
  }

  @Override
  public Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> assignment) {
    Map<TopicPartition, Long> result = new HashMap<>();
    if (null != assignment && !assignment.isEmpty()) {
      final Select partitionQuery = QueryBuilder.selectFrom(this.config.keyspace, this.config.offsetStorageTable)
          .column("offset")
          .whereColumn("topic").isEqualTo(QueryBuilder.bindMarker("topic"))
          .whereColumn("partition").isEqualTo(QueryBuilder.bindMarker("partition"));
      log.debug("loadOffsets() - Preparing statement. {}", partitionQuery.asCql());
      final PreparedStatement preparedStatement = this.session.prepare(partitionQuery.build());

      for (final TopicPartition topicPartition : assignment) {
        log.debug("loadOffsets() - Querying for {}", topicPartition);
        BoundStatement boundStatement = preparedStatement.bind();
        boundStatement = boundStatement.setString("topic", topicPartition.topic());
        boundStatement = boundStatement.setInt("partition", topicPartition.partition());

        ResultSet resultSet = this.executeStatement(boundStatement);
        Row row = resultSet.one();
        if (null != row) {
          long offset = row.getLong("offset");
          log.info("Found offset of {} for {}", offset, topicPartition);
          result.put(topicPartition, offset);
        }
      }
    }

    return result;
  }

  @Override
  public void close() throws IOException {
    this.session.close();
  }

  @Override
  public void onTableChanged(String keyspace, String tableName) {
    final ScyllaDbSchemaKey key = ScyllaDbSchemaKey.of(keyspace, tableName);
    this.tableMetadataCache.remove(key);
    this.deleteStatementCache.remove(key);
    this.insertStatementCache.remove(key);
  }
}
