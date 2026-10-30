package io.connect.scylladb;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
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
}
