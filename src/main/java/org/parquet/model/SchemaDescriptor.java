package org.parquet.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Describes the schema structure of a Parquet file, including both physical and logical column representations.
 * <p>
 * Physical columns represent the actual storage layout in the Parquet file, while logical columns provide
 * a user-facing view that may map to multiple physical columns (e.g., a MAP logical column maps to separate
 * key and value physical columns).
 *
 * @param name           the schema name (typically "message" for root schema)
 * @param columns        the list of physical column descriptors that define the actual storage layout
 * @param logicalColumns the list of logical column descriptors that define the user-facing schema
 */
public record SchemaDescriptor(String name, List<ColumnDescriptor> columns,
                               List<LogicalColumnDescriptor> logicalColumns) {

  /**
   * Creates a SchemaDescriptor from logical columns only, automatically deriving physical columns.
   * <p>
   * This factory method simplifies schema creation by automatically extracting physical columns
   * from logical column definitions and assigning proper column indices for complex types like
   * MAPs and LISTs.
   *
   * @param name           the schema name
   * @param logicalColumns the list of logical column descriptors
   * @return a SchemaDescriptor with physical columns derived from logical columns
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

  /**
   * Gets the number of physical columns in the schema.
   *
   * @return the count of physical columns
   */
  public int getNumColumns() {
    return columns.size();
  }

  /**
   * Gets a physical column descriptor by index.
   *
   * @param index the zero-based index of the physical column
   * @return the column descriptor at the specified index
   * @throws IndexOutOfBoundsException if the index is out of range
   */
  public ColumnDescriptor getColumn(int index) {
    return columns.get(index);
  }

  /**
   * Gets the list of logical columns (user-facing columns that may map to multiple physical columns).
   * <p>
   * For example, a MAP logical column maps to two physical columns (key and value).
   *
   * @return the list of logical column descriptors
   */
  @Override
  public List<LogicalColumnDescriptor> logicalColumns() {
    return logicalColumns;
  }

  /**
   * Gets the number of logical columns in the schema.
   *
   * @return the count of logical columns
   */
  public int getNumLogicalColumns() {
    return logicalColumns.size();
  }

  /**
   * Gets a logical column descriptor by index.
   *
   * @param index the zero-based index of the logical column
   * @return the logical column descriptor at the specified index
   * @throws IndexOutOfBoundsException if the index is out of range
   */
  public LogicalColumnDescriptor getLogicalColumn(int index) {
    return logicalColumns.get(index);
  }

  /**
   * Gets a logical column descriptor by name.
   *
   * @param name the name of the logical column to find
   * @return the logical column descriptor with the specified name, or null if not found
   */
  public LogicalColumnDescriptor getLogicalColumn(String name) {
    return logicalColumns.stream()
        .filter(col -> col.getName().equals(name))
        .findFirst()
        .orElse(null);
  }


  /**
   * Checks if logical columns are defined in the schema.
   * <p>
   * Returns false if the schema only uses physical columns directly without a logical layer.
   *
   * @return true if logical columns are defined, false otherwise
   */
  public boolean hasLogicalColumns() {
    return !logicalColumns.isEmpty();
  }

  /**
   * Finds the logical column descriptor for a given physical column index.
   * <p>
   * The mapping behavior depends on the logical column type:
   * <ul>
   *   <li>PRIMITIVE columns: 1:1 mapping between physical and logical columns</li>
   *   <li>MAP columns: Both key and value physical columns map to the same logical column</li>
   *   <li>LIST columns: 1:1 mapping to the element physical column</li>
   * </ul>
   *
   * @param physicalColumnIndex the zero-based index of the physical column
   * @return the logical column descriptor, or null if not found or index is out of range
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
   * Creates logical columns from physical columns by wrapping each physical column as a PRIMITIVE logical column.
   * <p>
   * This is useful when working with simple schemas that don't use complex types like MAPs or LISTs.
   *
   * @param columns the list of physical column descriptors
   * @return a list of logical column descriptors, one per physical column
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
   * Creates a MAP logical column with String keys and String values.
   * <p>
   * This is a convenience method for the most common map type (Map&lt;String, String&gt;).
   *
   * @param name     the name of the map column
   * @param optional whether the map itself can be NULL
   * @return a logical column descriptor for the string map
   */
  public static LogicalColumnDescriptor createStringMapColumn(String name, boolean optional) {
    return createMapColumn(name, Type.BYTE_ARRAY, Type.BYTE_ARRAY, optional, false);
  }

  /**
   * Creates a MAP logical column with specified key and value types.
   * <p>
   * The MAP follows the standard Parquet MAP structure with a repeated key_value group
   * containing required keys and optional/required values. Definition and repetition levels
   * are automatically calculated based on the optional flags.
   *
   * @param name           the name of the map column
   * @param keyType        the physical type for map keys
   * @param valueType      the physical type for map values
   * @param optional       whether the map itself can be NULL
   * @param valuesOptional whether individual map values can be NULL
   * @return a logical column descriptor for the map
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

  /**
   * Returns a string representation of the schema in Parquet schema format.
   * <p>
   * The output follows the standard Parquet schema notation, showing the message name
   * and all physical columns with their types and paths.
   *
   * @return a string representation of the schema
   */
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
