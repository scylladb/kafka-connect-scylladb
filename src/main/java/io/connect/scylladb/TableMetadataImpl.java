package io.connect.scylladb;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.shaded.guava.common.base.MoreObjects;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

class TableMetadataImpl {
  static class ColumnImpl implements TableMetadata.Column {
    final ColumnMetadata columnMetadata;

    ColumnImpl(ColumnMetadata columnMetadata) {
      this.columnMetadata = columnMetadata;
    }

    @Override
    public String getName() {
      return this.columnMetadata.getName().toString();
    }

    @Override
    public DataType getType() {
      return this.columnMetadata.getType();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("name", this.columnMetadata.getName())
          .add("type", this.columnMetadata.getType().asCql(true, true))
          .toString();
    }
  }

  static class TableImpl implements TableMetadata.Table {
    final String name;
    final String keyspace;
    final com.datastax.oss.driver.api.core.metadata.schema.TableMetadata tableMetadata;
    final Map<String, TableMetadata.Column> columns;
    final List<TableMetadata.Column> primaryKey;

    TableImpl(com.datastax.oss.driver.api.core.metadata.schema.TableMetadata tableMetadata) {
      this.tableMetadata = tableMetadata;
      this.name = this.tableMetadata.getName().asInternal();
      this.keyspace = this.tableMetadata.getKeyspace().asInternal();
      this.primaryKey = this.tableMetadata.getPrimaryKey()
          .stream()
          .map(ColumnImpl::new)
          .collect(Collectors.toList());
      List<TableMetadata.Column> allColumns = new ArrayList<>();
      allColumns.addAll(
          this.tableMetadata.getColumns().values().stream()
              .map(ColumnImpl::new)
              .collect(Collectors.toList())
      );
      this.columns = allColumns.stream()
          .collect(Collectors.toMap(
              TableMetadata.Column::getName,
              c -> c,
              (o, n) -> n,
              () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER)
          ));
    }

    @Override
    public String keyspace() {
      return this.keyspace;
    }

    @Override
    public TableMetadata.Column columnMetadata(String columnName) {
      return this.columns.get(columnName);
    }

    @Override
    public List<TableMetadata.Column> columns() {
      return ImmutableList.copyOf(this.columns.values());
    }

    @Override
    public List<TableMetadata.Column> primaryKey() {
      return this.primaryKey;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("keyspace", this.keyspace)
          .add("name", this.name)
          .add("columns", this.columns)
          .add("primaryKey", this.primaryKey)
          .toString();
    }
  }
}
