package org.parquet.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Schema descriptor for Parquet file
 *
 * @param columns        Physical columns
 * @param logicalColumns Logical columns (user-facing)
 */
public record SchemaDescriptor(String name, List<ColumnDescriptor> columns,
                               List<LogicalColumnDescriptor> logicalColumns) {

  /**
   * Create a SchemaDescriptor from logical columns only.
   * Physical columns are automatically derived from the logical columns.
   * Column indices for MAPs and LISTs are automatically assigned.
   *
   * @param name           Schema name
   * @param logicalColumns Logical column descriptors
   * @return SchemaDescriptor with physical columns derived from logical columns
   */
  public static SchemaDescriptor fromLogicalColumns(String name,
                                                    List<LogicalColumnDescriptor> logicalColumns) {
    // Extract physical columns from logical columns
    List<ColumnDescriptor> physicalColumns = new ArrayList<>();
    List<LogicalColumnDescriptor> updatedLogicalColumns = new ArrayList<>();

    for (LogicalColumnDescriptor logicalCol : logicalColumns) {
      if (logicalCol.isPrimitive() || logicalCol.isList()) {
        // For primitives and lists, add physical column(s) directly
        physicalColumns.addAll(logicalCol.getPhysicalColumns());
        updatedLogicalColumns.add(logicalCol);
      } else if (logicalCol.isMap()) {
        // For maps, we need to update the column indices in MapMetadata
        int keyColumnIndex = physicalColumns.size();
        int valueColumnIndex = keyColumnIndex + 1;

        MapMetadata oldMapMeta = logicalCol.getMapMetadata();
        MapMetadata newMapMeta = new MapMetadata(
            keyColumnIndex,
            valueColumnIndex,
            oldMapMeta.keyType(),
            oldMapMeta.valueType(),
            oldMapMeta.keyDescriptor(),
            oldMapMeta.valueDescriptor()
        );

        LogicalColumnDescriptor updatedLogicalCol = new LogicalColumnDescriptor(
            logicalCol.getName(),
            LogicalType.MAP,
            newMapMeta
        );

        physicalColumns.addAll(updatedLogicalCol.getPhysicalColumns());
        updatedLogicalColumns.add(updatedLogicalCol);
      } else {
        // Unknown type - just add as-is
        physicalColumns.addAll(logicalCol.getPhysicalColumns());
        updatedLogicalColumns.add(logicalCol);
      }
    }

    return new SchemaDescriptor(name, physicalColumns, updatedLogicalColumns);
  }

  public int getNumColumns() {
    return columns.size();
  }

  public ColumnDescriptor getColumn(int index) {
    return columns.get(index);
  }

  /**
   * Get logical columns (user-facing columns that may map to multiple physical columns)
   */
  @Override
  public List<LogicalColumnDescriptor> logicalColumns() {
    return logicalColumns;
  }

  /**
   * Get the number of logical columns
   */
  public int getNumLogicalColumns() {
    return logicalColumns.size();
  }

  /**
   * Get a logical column by index
   */
  public LogicalColumnDescriptor getLogicalColumn(int index) {
    return logicalColumns.get(index);
  }

  /**
   * Get a logical column by name
   */
  public LogicalColumnDescriptor getLogicalColumn(String name) {
    return logicalColumns.stream()
        .filter(col -> col.getName().equals(name))
        .findFirst()
        .orElse(null);
  }


  /**
   * Check if logical columns are defined (vs using physical columns directly)
   * Note: This always returns true now since logicalColumns is never null.
   */
  public boolean hasLogicalColumns() {
    return !logicalColumns.isEmpty();
  }

  /**
   * Find the logical column descriptor for a given physical column index.
   * For PRIMITIVE columns, there's a 1:1 mapping.
   * For MAP columns, both key and value physical columns map to the same logical column.
   * For LIST columns, there's a 1:1 mapping to the element column.
   *
   * @param physicalColumnIndex The index of the physical column
   * @return The logical column descriptor, or null if not found
   */
  public LogicalColumnDescriptor findLogicalColumnByPhysicalIndex(int physicalColumnIndex) {
    // Get the physical column descriptor
    if (physicalColumnIndex < 0 || physicalColumnIndex >= columns.size()) {
      return null;
    }
    ColumnDescriptor physicalDescriptor = columns.get(physicalColumnIndex);

    // Search through logical columns
    for (LogicalColumnDescriptor logicalCol : logicalColumns) {
      if (logicalCol.isPrimitive()) {
        // For primitive columns, check if the physical descriptor matches
        if (logicalCol.getPhysicalDescriptor().equals(physicalDescriptor)) {
          return logicalCol;
        }
      } else if (logicalCol.isMap()) {
        // For map columns, check if this is the key or value column
        MapMetadata mapMetadata = logicalCol.getMapMetadata();
        if (mapMetadata.keyColumnIndex() == physicalColumnIndex ||
            mapMetadata.valueColumnIndex() == physicalColumnIndex) {
          return logicalCol;
        }
      } else if (logicalCol.isList()) {
        // For list columns, check if this is the element column
        ListMetadata listMetadata = logicalCol.getListMetadata();
        if (listMetadata.elementColumnIndex() == physicalColumnIndex) {
          return logicalCol;
        }
      }
      // TODO: Add support for STRUCT types
    }

    return null;
  }

  /**
   * Create logical columns from physical columns.
   * Each physical column becomes a PRIMITIVE logical column.
   *
   * @param columns Physical columns
   * @return List of logical columns
   */
  public static List<LogicalColumnDescriptor> createLogicalColumnsFromPhysical(
      List<ColumnDescriptor> columns) {
    return columns.stream()
        .map(col -> new LogicalColumnDescriptor(
            col.getPathString(),
            LogicalType.PRIMITIVE,
            col.physicalType(),
            col))
        .toList();
  }

  /**
   * Create a MAP logical column with String keys and String values.
   * This is a convenience method for the most common map type.
   *
   * @param name     Name of the map column
   * @param optional Whether the map itself can be NULL
   * @return LogicalColumnDescriptor for the map
   */
  public static LogicalColumnDescriptor createStringMapColumn(String name, boolean optional) {
    return createMapColumn(name, Type.BYTE_ARRAY, Type.BYTE_ARRAY, optional, false);
  }

  /**
   * Create a MAP logical column.
   *
   * @param name           Name of the map column
   * @param keyType        Physical type for keys
   * @param valueType      Physical type for values
   * @param optional       Whether the map itself can be NULL
   * @param valuesOptional Whether values can be NULL
   * @return LogicalColumnDescriptor for the map
   */
  public static LogicalColumnDescriptor createMapColumn(
      String name,
      Type keyType,
      Type valueType,
      boolean optional,
      boolean valuesOptional) {

    // MAP structure:
    // optional group <name> (MAP) {
    //   repeated group key_value {
    //     required <keyType> key;
    //     optional/required <valueType> value;
    //   }
    // }

    // Definition levels:
    // - Map NULL: 0
    // - Map empty: 1
    // - Key present: 2 (keys are always required)
    // - Value NULL: 2 (if values are optional)
    // - Value present: 3 (if values are optional) or 2 (if required)

    int mapMaxDef = optional ? 1 : 0;
    int keyMaxDef = mapMaxDef + 1;  // Keys are always required within the repeated group
    int valueMaxDef = valuesOptional ? mapMaxDef + 2 : mapMaxDef + 1;

    // Repetition level: 1 (repeated group)
    int maxRep = 1;

    // Create physical column descriptors
    ColumnDescriptor keyDescriptor = new ColumnDescriptor(
        keyType,
        new String[] {name, "key_value", "key"},
        keyMaxDef,
        maxRep,
        0
    );

    ColumnDescriptor valueDescriptor = new ColumnDescriptor(
        valueType,
        new String[] {name, "key_value", "value"},
        valueMaxDef,
        maxRep,
        0
    );

    // Create MapMetadata (we'll assign column indices later when building full schema)
    MapMetadata mapMetadata = new MapMetadata(
        -1, -1,  // Column indices will be set when adding to schema
        keyType,
        valueType,
        keyDescriptor,
        valueDescriptor
    );

    return new LogicalColumnDescriptor(name, LogicalType.MAP, mapMetadata);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("message ").append(name).append(" {\n");
    for (ColumnDescriptor col : columns) {
      sb.append("  ").append(col).append("\n");
    }
    sb.append("}");
    return sb.toString();
  }

}
