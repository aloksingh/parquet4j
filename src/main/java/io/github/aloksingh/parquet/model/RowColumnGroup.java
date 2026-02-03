package io.github.aloksingh.parquet.model;

import java.util.List;

/**
 * Represents a single row with values from all columns in a Parquet file.
 * <p>
 * This interface provides methods to access column values and metadata for a single row
 * of data read from a Parquet file. It allows retrieval of column values by index, name,
 * or column descriptor, and provides access to schema information.
 * </p>
 *
 * @see ColumnDescriptor
 * @see SchemaDescriptor
 */
public interface RowColumnGroup {
  /**
   * Returns the schema descriptor for this row.
   * <p>
   * The schema descriptor contains metadata about the structure and types of all columns
   * in the row group.
   * </p>
   *
   * @return the {@link SchemaDescriptor} for this row, never {@code null}
   */
  SchemaDescriptor getSchema();

  /**
   * Returns all column descriptors for this row.
   * <p>
   * Each column descriptor contains metadata about a specific column, including its name,
   * type, and encoding information.
   * </p>
   *
   * @return an immutable list of {@link ColumnDescriptor} objects, never {@code null}
   */
  List<ColumnDescriptor> getColumns();

  /**
   * Returns the value of the column at the specified index.
   *
   * @param columnIndex the zero-based index of the column
   * @return the value of the column, or {@code null} if the column value is null
   * @throws IndexOutOfBoundsException if the column index is out of range
   *         ({@code columnIndex < 0 || columnIndex >= getColumnCount()})
   */
  Object getColumnValue(int columnIndex);

  /**
   * Returns the value of the specified column, cast to the given type.
   *
   * @param <T> the type to cast the column value to
   * @param column the column descriptor identifying the column to retrieve
   * @param typeClass the class object representing the type to cast to
   * @return the column value cast to type {@code T}, or {@code null} if the column value is null
   * @throws ClassCastException if the column value cannot be cast to the specified type
   * @throws IllegalArgumentException if the column descriptor does not match any column in this row
   */
  <T> T getColumnValue(ColumnDescriptor column, Class<T> typeClass);

  /**
   * Returns the value of the column with the specified name.
   *
   * @param columnName the name of the column to retrieve
   * @return the value of the column, or {@code null} if the column value is null
   * @throws IllegalArgumentException if no column with the specified name exists
   */
  Object getColumnValue(String columnName);

  /**
   * Returns the number of columns in this row.
   *
   * @return the column count, always non-negative
   */
  int getColumnCount();

}