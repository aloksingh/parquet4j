package org.parquet.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple implementation of RowColumnGroup that holds column values for a single row.
 * Column values represent LOGICAL columns (user-facing), which may be primitives or complex types like Maps.
 */
public class SimpleRowColumnGroup implements RowColumnGroup {
  private final SchemaDescriptor schema;
  private final Object[] logicalColumnValues;  // Values for logical columns
  private final Map<String, Integer> columnNameToLogicalIndex;

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

  @Override
  public SchemaDescriptor getSchema() {
    return schema;
  }

  @Override
  public List<ColumnDescriptor> getColumns() {
    return schema.columns();
  }

  @Override
  public Object getColumnValue(int columnIndex) {
    if (columnIndex < 0 || columnIndex >= logicalColumnValues.length) {
      throw new IndexOutOfBoundsException(
          "Column index out of bounds: " + columnIndex);
    }
    return logicalColumnValues[columnIndex];
  }

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

  @Override
  public Object getColumnValue(String columnName) {
    Integer index = columnNameToLogicalIndex.get(columnName);
    if (index == null) {
      throw new IllegalArgumentException("Column not found: " + columnName);
    }
    return logicalColumnValues[index];
  }

  @Override
  public int getColumnCount() {
    return logicalColumnValues.length;
  }

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
