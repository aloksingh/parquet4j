package io.github.aloksingh.parquet.model;

import java.util.HashMap;
import java.util.Map;

public class RowColumnGroupFactory {

  private final SchemaDescriptor schema;
  private final Map<String, Integer> columnNameToLogicalIndex;

  public RowColumnGroupFactory(SchemaDescriptor schema) {
    this.schema = schema;
    this.columnNameToLogicalIndex = new HashMap<>();

    // Build column name to logical index mapping
    for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
      LogicalColumnDescriptor col = schema.getLogicalColumn(i);
      columnNameToLogicalIndex.put(col.getName(), i);
    }
  }

  public RowColumnGroup createRowColumnGroup(Object[] logicalColumnValues) {
    return new SimpleRowColumnGroup(schema, columnNameToLogicalIndex, logicalColumnValues);
  }
}
