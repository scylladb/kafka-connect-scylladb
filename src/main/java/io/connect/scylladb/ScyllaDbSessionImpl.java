package io.connect.scylladb;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import io.connect.scylladb.topictotable.TopicConfigs;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

class ScyllaDbSessionImpl implements ScyllaDbSession {
  private static final Logger log = LoggerFactory.getLogger(ScyllaDbSessionImpl.class);

  private final ScyllaDbSinkConnectorConfig config;
  private final Cluster cluster;
  private final Session session;
  private final ScyllaDbSchemaBuilder schemaBuilder;
  private boolean sessionValid = true;
  private final Map<String, TableMetadata.Table> tableMetadataCache;
  private final Map<String, RecordToBoundStatementConverter> deleteStatementCache;
  private final Map<String, RecordToBoundStatementConverter> insertStatementCache;

  ScyllaDbSessionImpl(ScyllaDbSinkConnectorConfig config, Cluster cluster, Session session) {
    this.cluster = cluster;
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
  public ResultSetFuture executeStatementAsync(Statement statement) {
    log.trace("executeStatement() - Executing statement\n{}", statement);
    return this.session.executeAsync(statement);
  }

  @Override
  public ResultSet executeQuery(String query) {
    log.trace("executeQuery() - Executing query\n{}", query);
    return this.session.execute(query);
  }

  @Override
  public TableMetadata.Table tableMetadata(String keyspace, String tableName) {
    log.trace("tableMetadata() - tableName = '{}.{}'", keyspace, tableName);
    TableMetadata.Table result = this.tableMetadataCache.get(tableName);

    if (null == result) {
      final KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
      final com.datastax.driver.core.TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);
      if (null != tableMetadata) {
        result = new TableMetadataImpl.TableImpl(tableMetadata);
        this.tableMetadataCache.put(tableName, result);
      } else {
        result = null;
      }
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
        tableName,
        new Function<String, RecordToBoundStatementConverter>() {
          @Override
          public RecordToBoundStatementConverter apply(String tableName) {
            Delete statement = QueryBuilder.delete()
                .from(keyspace, tableName);
            TableMetadata.Table tableMetadata = tableMetadata(keyspace, tableName);
            for (TableMetadata.Column columnMetadata : tableMetadata.primaryKey()) {
              statement.where(
                  QueryBuilder.eq(
                      columnMetadata.getName(), QueryBuilder.bindMarker(columnMetadata.getName())
                  )
              );
            }
            log.debug("delete() - Preparing statement. '{}'", statement);
            PreparedStatement preparedStatement = session.prepare(statement);
            return new RecordToBoundStatementConverter(preparedStatement);
          }
        }
    );
  }

  private PreparedStatement createInsertPreparedStatement(String keyspace, String tableName, TopicConfigs topicConfigs) {
    Insert statement = QueryBuilder.insertInto(keyspace, tableName);
    TableMetadata.Table tableMetadata = tableMetadata(keyspace, tableName);
    for (TableMetadata.Column columnMetadata : tableMetadata.columns()) {
      statement.value(columnMetadata.getName(), QueryBuilder.bindMarker(columnMetadata.getName()));
    }
    log.debug("insert() - Preparing statement. '{}'", statement);
    if (topicConfigs != null) {
      return (topicConfigs.getTtl() == null) ? session.prepare(statement) :
              session.prepare(statement.using(QueryBuilder.ttl(topicConfigs.getTtl())));
    } else {
      return (config.ttl == null) ? session.prepare(statement) :
              session.prepare(statement.using(QueryBuilder.ttl(config.ttl)));
    }
  }

  @Override
  public RecordToBoundStatementConverter insert(String keyspace, String tableName, TopicConfigs topicConfigs) {
    if (topicConfigs != null && topicConfigs.getTtl() != null) {
      PreparedStatement preparedStatement = createInsertPreparedStatement(keyspace, tableName, topicConfigs);
      return new RecordToBoundStatementConverter(preparedStatement);
    } else {
      return this.insertStatementCache.computeIfAbsent(
              tableName,
              new Function<String, RecordToBoundStatementConverter>() {
                @Override
                public RecordToBoundStatementConverter apply(String tableName) {
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
              createInsertPreparedStatement(config.keyspace, this.config.offsetStorageTable, null);
    }
    log.debug(
            "getAddOffsetsStatement() - Setting offset to {}:{}:{}",
            topicPartition.topic(),
            topicPartition.partition(),
            metadata.offset()
    );
    final BoundStatement statement = offsetPreparedStatement.bind();
    statement.setString("topic", topicPartition.topic());
    statement.setInt("partition", topicPartition.partition());
    statement.setLong("offset", metadata.offset());
    return statement;
  }

  @Override
  public Map<TopicPartition, Long> loadOffsets(String keyspace, Set<TopicPartition> assignment) {
    Map<TopicPartition, Long> result = new HashMap<>();
    if (null != assignment && !assignment.isEmpty()) {
      final Select.Where partitionQuery = QueryBuilder.select()
          .column("offset")
          .from(keyspace, this.config.offsetStorageTable)
          .where(QueryBuilder.eq("topic", QueryBuilder.bindMarker("topic")))
          .and(QueryBuilder.eq("partition", QueryBuilder.bindMarker("partition")));
      log.debug("loadOffsets() - Preparing statement. {}", partitionQuery);
      final PreparedStatement preparedStatement = this.session.prepare(partitionQuery);

      for (final TopicPartition topicPartition : assignment) {
        log.debug("loadOffsets() - Querying for {}", topicPartition);
        BoundStatement boundStatement = preparedStatement.bind();
        boundStatement.setString("topic", topicPartition.topic());
        boundStatement.setInt("partition", topicPartition.partition());

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
    this.cluster.close();
  }

  @Override
  public void onTableChanged(String keyspace, String tableName) {
    this.tableMetadataCache.remove(tableName);
    this.deleteStatementCache.remove(tableName);
    this.insertStatementCache.remove(tableName);
  }
}
