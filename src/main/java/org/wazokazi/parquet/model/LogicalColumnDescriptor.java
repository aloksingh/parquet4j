package org.wazokazi.parquet.model;

/**
 * Represents a logical column as seen by the user.
 * A logical column may map to one (primitive) or multiple (map, struct) physical columns.
 */
public class LogicalColumnDescriptor {

  private final String name;
  private final LogicalType logicalType;
  private final Type physicalType;  // For primitives only
  private final ColumnDescriptor physicalDescriptor;  // For primitives
  private final MapMetadata mapMetadata;  // For maps only
  private final ListMetadata listMetadata;  // For lists only

  /**
   * Constructor for primitive columns.
   *
   * @param name the name of the column
   * @param logicalType the logical type of the column
   * @param physicalType the physical type of the column
   * @param physicalDescriptor the physical column descriptor
   */
  public LogicalColumnDescriptor(String name, LogicalType logicalType, Type physicalType,
                                 ColumnDescriptor physicalDescriptor) {
    this.name = name;
    this.logicalType = logicalType;
    this.physicalType = physicalType;
    this.physicalDescriptor = physicalDescriptor;
    this.mapMetadata = null;
    this.listMetadata = null;
  }

  /**
   * Constructor for map columns.
   *
   * @param name the name of the column
   * @param logicalType the logical type of the column
   * @param mapMetadata the map metadata containing key and value descriptors
   */
  public LogicalColumnDescriptor(String name, LogicalType logicalType, MapMetadata mapMetadata) {
    this.name = name;
    this.logicalType = logicalType;
    this.physicalType = null;
    this.physicalDescriptor = null;
    this.mapMetadata = mapMetadata;
    this.listMetadata = null;
  }

  /**
   * Constructor for list columns.
   *
   * @param name the name of the column
   * @param logicalType the logical type of the column
   * @param listMetadata the list metadata containing element descriptor
   */
  public LogicalColumnDescriptor(String name, LogicalType logicalType, ListMetadata listMetadata) {
    this.name = name;
    this.logicalType = logicalType;
    this.physicalType = null;
    this.physicalDescriptor = null;
    this.mapMetadata = null;
    this.listMetadata = listMetadata;
  }

  /**
   * Returns the name of this logical column.
   *
   * @return the column name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the logical type of this column (PRIMITIVE, MAP, LIST, or STRUCT).
   *
   * @return the logical type
   */
  public LogicalType getLogicalType() {
    return logicalType;
  }

  /**
   * Checks if this is a primitive column.
   *
   * @return true if this column is a primitive type
   */
  public boolean isPrimitive() {
    return logicalType == LogicalType.PRIMITIVE;
  }

  /**
   * Checks if this is a map column.
   *
   * @return true if this column is a map type
   */
  public boolean isMap() {
    return logicalType == LogicalType.MAP;
  }

  /**
   * Checks if this is a list column.
   *
   * @return true if this column is a list type
   */
  public boolean isList() {
    return logicalType == LogicalType.LIST;
  }

  /**
   * Checks if this is a struct column.
   *
   * @return true if this column is a struct type
   */
  public boolean isStruct() {
    return logicalType == LogicalType.STRUCT;
  }

  /**
   * Returns the physical type for primitive and list columns.
   *
   * @return the physical type
   * @throws IllegalStateException if this is not a primitive or list column
   */
  public Type getPhysicalType() {
    if (isPrimitive()) {
      return physicalType;
    } else if (isList()) {
      return listMetadata.elementType();
    }
    throw new IllegalStateException("Physical type only available for primitive and list columns");
  }

  /**
   * Returns the physical column descriptor for primitive and list columns.
   *
   * @return the physical column descriptor
   * @throws IllegalStateException if this is not a primitive or list column
   */
  public ColumnDescriptor getPhysicalDescriptor() {
    if (isPrimitive()) {
      return physicalDescriptor;
    } else if (isList()) {
      return listMetadata.elementDescriptor();
    }
    throw new IllegalStateException(
        "Physical descriptor only available for primitive and list columns");
  }

  /**
   * Returns the map metadata for map columns.
   *
   * @return the map metadata containing key and value descriptors
   * @throws IllegalStateException if this is not a map column
   */
  public MapMetadata getMapMetadata() {
    if (!isMap()) {
      throw new IllegalStateException("Map metadata only available for map columns");
    }
    return mapMetadata;
  }

  /**
   * Returns the list metadata for list columns.
   *
   * @return the list metadata containing element descriptor
   * @throws IllegalStateException if this is not a list column
   */
  public ListMetadata getListMetadata() {
    if (!isList()) {
      throw new IllegalStateException("List metadata only available for list columns");
    }
    return listMetadata;
  }

  /**
   * Get the physical columns that this logical column maps to.
   * For PRIMITIVE and LIST columns, returns a single physical column.
   * For MAP columns, returns two physical columns (key and value).
   *
   * @return List of physical column descriptors
   */
  public java.util.List<ColumnDescriptor> getPhysicalColumns() {
    if (isPrimitive()) {
      return java.util.List.of(physicalDescriptor);
    } else if (isList()) {
      return java.util.List.of(listMetadata.elementDescriptor());
    } else if (isMap()) {
      return java.util.List.of(
          mapMetadata.keyDescriptor(),
          mapMetadata.valueDescriptor()
      );
    }
    throw new IllegalStateException("Unknown logical type: " + logicalType);
  }

  @Override
  public String toString() {
    if (isPrimitive()) {
      return "LogicalColumn{name=" + name + ", type=PRIMITIVE, physicalType=" + physicalType + "}";
    } else if (isMap()) {
      return "LogicalColumn{name=" + name + ", type=MAP, " + mapMetadata + "}";
    } else if (isList()) {
      return "LogicalColumn{name=" + name + ", type=LIST, " + listMetadata + "}";
    }
    return "LogicalColumn{name=" + name + ", type=" + logicalType + "}";
  }
}
