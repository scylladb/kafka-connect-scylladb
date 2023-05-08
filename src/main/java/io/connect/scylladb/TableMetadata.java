package io.connect.scylladb;

import com.datastax.oss.driver.api.core.type.DataType;

import java.util.List;

interface TableMetadata {

  interface Column {

    String getName();

    DataType getType();
  }

  interface Table {
    /**
     * Keyspace for the table.
     *
     * @return Keyspace for the table.
     */
    String keyspace();

    /**
     * Method is used to return the metadata for a column.
     *
     * @param columnName getName of the column to return metadata for.
     * @return Null if the column does not exist.
     */
    Column columnMetadata(String columnName);

    /**
     * Method is used to return all of the columns for a table.
     *
     * @return List of columns for the table.
     */
    List<Column> columns();

    /**
     * Method is used to return the primary key columns for a table.
     * @return List of columns in the primary key.
     */
    List<Column> primaryKey();
  }
}
