package io.github.aloksingh.parquet.model;

import io.github.aloksingh.parquet.model.SchemaDescriptor.GroupNode;
import io.github.aloksingh.parquet.model.SchemaDescriptor.LeafNode;
import io.github.aloksingh.parquet.model.SchemaDescriptor.Repetition;
import io.github.aloksingh.parquet.model.SchemaDescriptor.SchemaNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Simple implementation of RowColumnGroup that holds column values for a single row.
 *
 * <p>Column values represent LOGICAL columns (user-facing top-level fields), which may be
 * primitives or reconstructed containers like Maps, Lists and Structs. The logical column
 * descriptors returned by {@link #getColumns()} are aligned with the value order of
 * {@link #getColumnValue(int)}; physical leaf descriptors are available through
 * {@link #getPhysicalColumns()}.
 *
 * @see RowColumnGroup
 * @see SchemaDescriptor
 * @see LogicalColumnDescriptor
 */
public class SimpleRowColumnGroup implements RowColumnGroup {
  private final SchemaDescriptor schema;
  private final Object[] logicalColumnValues;  // Values for logical columns
  private final Map<String, Integer> columnNameToLogicalIndex;
  private final java.util.function.IntFunction<Object> valueResolver;  // nullable
  private final boolean[] valueLoaded;                                 // nullable

  /**
   * Constructs a new SimpleRowColumnGroup with the given schema and column values.
   *
   * @param schema the schema descriptor defining the structure of the row
   * @param logicalColumnValues array of values for each logical column in the row
   */
  public SimpleRowColumnGroup(SchemaDescriptor schema,
                              Map<String, Integer> columnNameToLogicalIndex,
                              Object[] logicalColumnValues) {
    this.schema = schema;
    this.logicalColumnValues = logicalColumnValues;
    this.columnNameToLogicalIndex = columnNameToLogicalIndex;
    this.valueResolver = null;
    this.valueLoaded = null;
  }

  public SimpleRowColumnGroup(SchemaDescriptor schema, Object[] logicalColumnValues) {
    this.schema = schema;
    this.logicalColumnValues = logicalColumnValues;
    this.columnNameToLogicalIndex = new HashMap<>();
    this.valueResolver = null;
    this.valueLoaded = null;

    // Build column name to logical index mapping (first occurrence wins for duplicates)
    for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
      LogicalColumnDescriptor col = schema.getLogicalColumn(i);
      columnNameToLogicalIndex.putIfAbsent(col.getName(), i);
    }
  }

  /**
   * Constructs a row whose logical column values materialize on access. Each logical index is
   * resolved exactly once, so rejecting conversions (e.g. INT96) throw only when their column
   * is actually read.
   */
  public SimpleRowColumnGroup(SchemaDescriptor schema,
                              java.util.function.IntFunction<Object> valueResolver) {
    this.schema = schema;
    this.logicalColumnValues = new Object[schema.getNumLogicalColumns()];
    this.columnNameToLogicalIndex = new HashMap<>();
    this.valueResolver = valueResolver;
    this.valueLoaded = new boolean[logicalColumnValues.length];

    for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
      LogicalColumnDescriptor col = schema.getLogicalColumn(i);
      columnNameToLogicalIndex.putIfAbsent(col.getName(), i);
    }
  }

  private Object resolve(int columnIndex) {
    if (valueResolver == null) {
      return logicalColumnValues[columnIndex];
    }
    if (!valueLoaded[columnIndex]) {
      logicalColumnValues[columnIndex] = valueResolver.apply(columnIndex);
      valueLoaded[columnIndex] = true;
    }
    return logicalColumnValues[columnIndex];
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
   * Returns the logical column descriptors for this row, aligned with the logical values
   * returned by {@link #getColumnValue(int)}.
   *
   * @return list of logical column descriptors
   */
  @Override
  public List<LogicalColumnDescriptor> getColumns() {
    return schema.logicalColumns();
  }

  /**
   * Returns the physical leaf descriptors of this row's schema in depth-first schema order.
   *
   * @return list of physical column descriptors
   */
  @Override
  public List<ColumnDescriptor> getPhysicalColumns() {
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
    return resolve(columnIndex);
  }

  /**
   * Gets the value of the leaf column identified by its physical descriptor, cast to the
   * given type. Nested leaves are extracted from their container values.
   *
   * @param <T> the expected type of the column value
   * @param column the physical column descriptor identifying the leaf to retrieve
   * @param typeClass the expected class of the column value
   * @return the column value cast to the specified type, or {@code null} if the value is null
   * @throws IllegalArgumentException if the column is not found in the schema
   * @throws ClassCastException if the column value cannot be cast to the specified type
   */
  @Override
  @SuppressWarnings("unchecked")
  public <T> T getColumnValue(ColumnDescriptor column, Class<T> typeClass) {
    Objects.requireNonNull(column, "column");
    SchemaNode target = schema.node(String.join(".", column.path()));
    if (!(target instanceof LeafNode)) {
      throw new IllegalArgumentException("Column not found: " + column.getPathString());
    }
    Object value = valueOfNode(target);
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
   * Gets the value of the column with the specified name. Names are matched case-sensitively
   * against the logical column names first; a dot-separated path addresses a nested field
   * inside a reconstructed STRUCT column.
   *
   * @param columnName the name of the column to retrieve
   * @return the column value, or {@code null} if the column value is null
   * @throws IllegalArgumentException if no column with the given name exists
   */
  @Override
  public Object getColumnValue(String columnName) {
    Integer index = columnNameToLogicalIndex.get(columnName);
    if (index != null) {
      return resolve(index);
    }
    SchemaNode target = schema.node(columnName);
    if (target == null) {
      throw new IllegalArgumentException("Column not found: " + columnName);
    }
    return valueOfNode(target);
  }

  /**
   * Resolves a schema node to this row's value: the owning logical column's value directly
   * (logical columns are leaves or collapsed MAP groups), or a nested extraction from a MAP
   * column's reconstructed value for nodes inside it.
   */
  private Object valueOfNode(SchemaNode target) {
    for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
      SchemaNode node = schema.getLogicalColumn(i).node();
      if (node == null) {
        continue;
      }
      if (node.path().equals(target.path())) {
        return resolve(i);
      }
      if (startsWith(target.path(), node.path())) {
        return extract(resolve(i), node, target);
      }
    }
    throw new IllegalArgumentException(
        "Column not found: " + String.join(".", target.path()));
  }

  private static boolean startsWith(List<String> path, List<String> prefix) {
    if (path.size() < prefix.size()) {
      return false;
    }
    for (int i = 0; i < prefix.size(); i++) {
      if (!path.get(i).equals(prefix.get(i))) {
        return false;
      }
    }
    return true;
  }

  /** Recursively extracts a nested node value from a materialized container value. */
  private static Object extract(Object value, SchemaNode node, SchemaNode target) {
    if (node.path().equals(target.path())) {
      return value;
    }
    if (node.repetition() == Repetition.REPEATED && node.kind() != LogicalType.LIST
        && node.kind() != LogicalType.MAP) {
      if (value == null) {
        return null;
      }
      List<Object> out = new ArrayList<>();
      for (Object instance : (List<?>) value) {
        out.add(extractInstance(instance, node, target));
      }
      return out;
    }
    return extractInstance(value, node, target);
  }

  private static Object extractInstance(Object value, SchemaNode node, SchemaNode target) {
    if (node.path().equals(target.path())) {
      return value;
    }
    if (value == null) {
      return null;
    }
    if (node instanceof LeafNode) {
      throw new IllegalArgumentException(
          "Column " + String.join(".", target.path()) + " is not inside "
              + String.join(".", node.path()));
    }
    GroupNode group = (GroupNode) node;
    switch (group.kind()) {
      case LIST -> {
        SchemaNode element = group.children().get(0);
        List<Object> out = new ArrayList<>();
        for (Object item : (List<?>) value) {
          out.add(extract(item, element, target));
        }
        return out;
      }
      case MAP -> {
        SchemaNode key = group.children().get(0);
        SchemaNode mapValue = group.children().size() > 1 ? group.children().get(1) : null;
        boolean inKeys = startsWith(target.path(), key.path());
        List<Object> out = new ArrayList<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          out.add(inKeys ? extract(entry.getKey(), key, target)
              : extract(entry.getValue(), mapValue, target));
        }
        return out;
      }
      default -> {
        for (SchemaNode child : group.children()) {
          if (startsWith(target.path(), child.path())) {
            return extract(((Map<?, ?>) value).get(child.name()), child, target);
          }
        }
        throw new IllegalArgumentException(
            "Column " + String.join(".", target.path()) + " is not inside "
                + String.join(".", node.path()));
      }
    }
  }

  /**
   * Returns a string representation of this row showing all column names and values.
   *
   * @return a string in the format "RowColumnGroup{col1=val1, col2=val2, ...}"
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
      Object value = resolve(i);
      sb.append(col.getName())
          .append("=")
          .append(value instanceof byte[] bytes ? java.util.Arrays.toString(bytes) : value);
    }
    sb.append("}");
    return sb.toString();
  }
}
