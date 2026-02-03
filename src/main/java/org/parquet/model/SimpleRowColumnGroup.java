package org.parquet.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple implementation of RowColumnGroup that holds column values for a single row.
 *
 * <p>Column values represent LOGICAL columns (user-facing), which may be primitives or complex
 * types like Maps. This class provides a straightforward way to store and access row data
 * from a Parquet file, with support for accessing columns by index, name, or descriptor.
 *
 * <p>The implementation maintains a mapping from column names to their logical indices
 * for efficient name-based lookups.
 *
 * @see RowColumnGroup
 * @see SchemaDescriptor
 * @see LogicalColumnDescriptor
 */
public class SimpleRowColumnGroup implements RowColumnGroup {
  private final SchemaDescriptor schema;
  private final Object[] logicalColumnValues;  // Values for logical columns
  private final Map<String, Integer> columnNameToLogicalIndex;

  /**
   * Constructs a new SimpleRowColumnGroup with the given schema and column values.
   *
   * @param schema the schema descriptor defining the structure of the row
   * @param logicalColumnValues array of values for each logical column in the row
   */
  public SimpleRowColumnGroup(SchemaDescriptor schema, Object[] logicalColumnValues) {
    this.schema = schema;
    this.logicalColumnValues = logicalColumnValues;
    this.columnNameToLogicalIndex = new HashMap<>();

    // Build column name to logical index mapping
    for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
      LogicalColumnDescriptor col = schema.getLogicalColumn(i);
      columnNameToLogicalIndex.put(col.getName(), i);
    }
  }

  /**
   * Returns the schema descriptor for this row.
   *
   * @return the schema descriptor
   */
  @Override
  public SchemaDescriptor getSchema() {
    return schema;
  }

  /**
   * Returns the list of column descriptors for all columns in this row.
   *
   * @return list of column descriptors
   */
  @Override
  public List<ColumnDescriptor> getColumns() {
    return schema.columns();
  }

  /**
   * Gets the value of the column at the specified logical index.
   *
   * @param columnIndex the zero-based logical column index
   * @return the column value, or {@code null} if the column value is null
   * @throws IndexOutOfBoundsException if the column index is out of bounds
   */
  @Override
  public Object getColumnValue(int columnIndex) {
    if (columnIndex < 0 || columnIndex >= logicalColumnValues.length) {
      throw new IndexOutOfBoundsException(
          "Column index out of bounds: " + columnIndex);
    }
    return logicalColumnValues[columnIndex];
  }

  /**
   * Gets the value of the specified column with type checking.
   *
   * <p>This method finds the logical column by matching the physical column path,
   * then retrieves and casts the value to the specified type.
   *
   * @param <T> the expected type of the column value
   * @param column the column descriptor identifying the column to retrieve
   * @param typeClass the expected class of the column value
   * @return the column value cast to the specified type, or {@code null} if the column value is null
   * @throws IllegalArgumentException if the column is not found in the schema
   * @throws ClassCastException if the column value cannot be cast to the specified type
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T getColumnValue(ColumnDescriptor column, Class<T> typeClass) {
    // Find the logical column index by matching the physical column path
    int index = -1;
    for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
      LogicalColumnDescriptor logicalCol = schema.getLogicalColumn(i);
      if (logicalCol.isPrimitive() &&
          logicalCol.getPhysicalDescriptor().getPathString().equals(column.getPathString())) {
        index = i;
        break;
      }
    }

    if (index == -1) {
      throw new IllegalArgumentException(
          "Column not found: " + column.getPathString());
    }

    Object value = logicalColumnValues[index];
    if (value == null) {
      return null;
    }

    if (!typeClass.isInstance(value)) {
      throw new ClassCastException(
          "Cannot cast column value of type " + value.getClass().getName() +
              " to " + typeClass.getName());
    }

    return (T) value;
  }

  /**
   * Gets the value of the column with the specified name.
   *
   * @param columnName the name of the column to retrieve
   * @return the column value, or {@code null} if the column value is null
   * @throws IllegalArgumentException if no column with the given name exists
   */
  @Override
  public Object getColumnValue(String columnName) {
    Integer index = columnNameToLogicalIndex.get(columnName);
    if (index == null) {
      throw new IllegalArgumentException("Column not found: " + columnName);
    }
    return logicalColumnValues[index];
  }

  /**
   * Returns the number of logical columns in this row.
   *
   * @return the column count
   */
  @Override
  public int getColumnCount() {
    return logicalColumnValues.length;
  }

  /**
   * Returns a string representation of this row showing all column names and values.
   *
   * @return a string in the format "RowColumnGroup{col1=val1, col2=val2, ...}"
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("RowColumnGroup{");
    for (int i = 0; i < logicalColumnValues.length; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      LogicalColumnDescriptor col = schema.getLogicalColumn(i);
      sb.append(col.getName())
          .append("=")
          .append(logicalColumnValues[i]);
    }
    sb.append("}");
    return sb.toString();
  }
}
