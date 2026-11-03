package io.connect.scylladb;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import io.connect.scylladb.topictotable.TopicConfigs;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScyllaDbSessionImplTest {

  ScyllaDbSessionImpl session;
  Metadata metadata;

  @Before
  public void before() {
    Map<String, String> settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
    when(keyspace.getName()).thenReturn(CqlIdentifier.fromCql("ks"));
    when(keyspace.getTable(anyString())).thenReturn(Optional.empty());
    metadata = mock(Metadata.class);
    when(metadata.getKeyspace(anyString())).thenReturn(Optional.of(keyspace));
    CqlSession cqlSession = mock(CqlSession.class);
    when(cqlSession.getMetadata()).thenReturn(metadata);
    session = new ScyllaDbSessionImpl(new ScyllaDbSinkConnectorConfig(settings), cqlSession);
  }

  @Test
  public void shouldNameMissingKeyspace() {
    when(metadata.getKeyspace("other")).thenReturn(Optional.empty());
    ConnectException ex = assertThrows(ConnectException.class, () -> session.tableMetadata("other", "t"));
    assertTrue(ex.getMessage(), ex.getMessage().contains("Keyspace 'other' does not exist"));
  }

  @Test
  public void shouldNameMissingTableOnInsert() {
    DataException ex = assertThrows(DataException.class, () -> session.insert("ks", "missing", null));
    assertTrue(ex.getMessage(), ex.getMessage().contains("'ks.missing' does not exist"));
  }

  @Test
  public void shouldNameMissingTableOnDelete() {
    DataException ex = assertThrows(DataException.class, () -> session.delete("ks", "missing"));
    assertTrue(ex.getMessage(), ex.getMessage().contains("'ks.missing' does not exist"));
  }

  @Test
  public void shouldShareCachedMetadataBetweenSpellingsOfOneTable() {
    KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
    when(keyspace.getTable(anyString())).thenReturn(Optional.of(
        mock(com.datastax.oss.driver.api.core.metadata.schema.TableMetadata.class, RETURNS_MOCKS)));
    when(metadata.getKeyspace(anyString())).thenReturn(Optional.of(keyspace));

    session.tableMetadata("KS", "TBL");
    session.tableMetadata("ks", "tbl");
    verify(keyspace, times(1)).getTable(anyString());

    session.onTableChanged("ks", "tbl");
    session.tableMetadata("KS", "TBL");
    verify(keyspace, times(2)).getTable(anyString());
  }

  @Test
  public void shouldReportMissingTableAsAbsent() {
    assertEquals(false, session.tableExists("ks", "missing"));
  }

  // A session over table ks.t with primary key id, preparing on the given driver session.
  ScyllaDbSessionImpl sessionWithTable(CqlSession cqlSession, ScyllaDbSinkConnectorConfig config) {
    ColumnMetadata id = mock(ColumnMetadata.class);
    when(id.getName()).thenReturn(CqlIdentifier.fromInternal("id"));
    com.datastax.oss.driver.api.core.metadata.schema.TableMetadata table =
        mock(com.datastax.oss.driver.api.core.metadata.schema.TableMetadata.class);
    when(table.getName()).thenReturn(CqlIdentifier.fromInternal("t"));
    when(table.getKeyspace()).thenReturn(CqlIdentifier.fromInternal("ks"));
    when(table.getPrimaryKey()).thenReturn(Collections.singletonList(id));
    when(table.getColumns()).thenReturn(Collections.singletonMap(CqlIdentifier.fromInternal("id"), id));
    KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
    when(keyspace.getTable(anyString())).thenReturn(Optional.of(table));
    when(metadata.getKeyspace(anyString())).thenReturn(Optional.of(keyspace));
    when(cqlSession.getMetadata()).thenReturn(metadata);
    return new ScyllaDbSessionImpl(config, cqlSession);
  }

  List<String> preparedQueries(CqlSession cqlSession, int count) {
    ArgumentCaptor<SimpleStatement> prepared = ArgumentCaptor.forClass(SimpleStatement.class);
    verify(cqlSession, times(count)).prepare(prepared.capture());
    return prepared.getAllValues().stream().map(SimpleStatement::getQuery).collect(Collectors.toList());
  }

  @Test
  public void shouldBindTheInheritedTtlForTopicsWithSettingsOfTheirOwn() {
    Map<String, String> settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    settings.put(ScyllaDbSinkConnectorConfig.TTL_CONFIG, "10");
    ScyllaDbSinkConnectorConfig config = new ScyllaDbSinkConnectorConfig(settings);
    CqlSession cqlSession = mock(CqlSession.class);
    ScyllaDbSessionImpl withConnectorTtl = sessionWithTable(cqlSession, config);
    TopicConfigs mappingOnly = new TopicConfigs("ks", "t", Collections.singletonMap("mapping", "id=key.id"), config);
    assertEquals(Integer.valueOf(10), mappingOnly.getTtl());

    withConnectorTtl.insert("ks", "t", null);
    withConnectorTtl.insert("ks", "t", mappingOnly);
    withConnectorTtl.insert("ks", "t", mappingOnly);

    // One statement per kind: the TTL is bound per record, not prepared into the statement.
    List<String> queries = preparedQueries(cqlSession, 2);
    assertTrue(queries.get(0), queries.get(0).endsWith("USING TTL 10"));
    assertTrue(queries.get(1), queries.get(1).endsWith("USING TTL :\"__ttl\""));
  }

  @Test
  public void shouldPrepareTtlInsertAgainAfterTableChanged() {
    Map<String, String> settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    ScyllaDbSinkConnectorConfig config = new ScyllaDbSinkConnectorConfig(settings);
    CqlSession cqlSession = mock(CqlSession.class);
    ScyllaDbSessionImpl cached = sessionWithTable(cqlSession, config);
    Map<String, String> topicSettings = new HashMap<>();
    topicSettings.put("mapping", "id=key.id");
    topicSettings.put("ttlSeconds", "10");
    TopicConfigs withTtl = new TopicConfigs("ks", "t", topicSettings, config);

    cached.insert("ks", "t", withTtl);
    cached.insert("ks", "t", withTtl);
    cached.onTableChanged("ks", "t");
    cached.insert("ks", "t", withTtl);

    // A new column must reach the insert that binds the TTL, too.
    List<String> queries = preparedQueries(cqlSession, 2);
    assertTrue(queries.get(1), queries.get(1).endsWith("USING TTL :\"__ttl\""));
  }

  @Test
  public void shouldWriteWithoutTtlWhenNoneIsSet() {
    Map<String, String> settings = new HashMap<>();
    settings.put(ScyllaDbSinkConnectorConfig.KEYSPACE_CONFIG, "ks");
    ScyllaDbSinkConnectorConfig config = new ScyllaDbSinkConnectorConfig(settings);
    CqlSession cqlSession = mock(CqlSession.class);
    ScyllaDbSessionImpl withoutConnectorTtl = sessionWithTable(cqlSession, config);
    TopicConfigs mappingOnly = new TopicConfigs("ks", "t", Collections.singletonMap("mapping", "id=key.id"), config);

    withoutConnectorTtl.insert("ks", "t", null);
    withoutConnectorTtl.insert("ks", "t", mappingOnly);

    // Both kinds share the statement without a TTL.
    List<String> queries = preparedQueries(cqlSession, 1);
    assertFalse(queries.get(0), queries.get(0).contains("TTL"));
  }
}
