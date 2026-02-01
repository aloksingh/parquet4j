package org.parquet.model;

import java.util.List;

/**
 * Represents a single row with values from all columns
 */
public interface RowColumnGroup {
  /**
   * Get the schema descriptor for this row
   */
  SchemaDescriptor getSchema();

  /**
   * Get all column descriptors
   */
  List<ColumnDescriptor> getColumns();

  /**
   * Get a column value by index
   */
  Object getColumnValue(int columnIndex);

  /**
   * Get a column value by column descriptor, cast to the specified type
   */
  <T> T getColumnValue(ColumnDescriptor column, Class<T> typeClass);

  /**
   * Get a column value by column name
   */
  Object getColumnValue(String columnName);

  /**
   * Get the number of columns in this row
   */
  int getColumnCount();

}