package io.github.aloksingh.parquet.model;

/**
 * Describes a column in a Parquet schema.
 * <p>
 * A column descriptor contains the metadata needed to read and interpret a physical
 * column in a Parquet file, including its type, path in the schema hierarchy, and
 * nesting information encoded in definition and repetition levels.
 *
 * @param physicalType        the physical storage type of the column
 * @param path                the dot-separated path to this column in the schema hierarchy
 * @param maxDefinitionLevel  the maximum definition level for this column, indicating
 *                            the depth of nullable fields in the path
 * @param maxRepetitionLevel  the maximum repetition level for this column, indicating
 *                            the depth of repeated fields in the path
 * @param typeLength          the fixed length for FIXED_LEN_BYTE_ARRAY types, or 0 for
 *                            other types
 */
public record ColumnDescriptor(Type physicalType, String[] path, int maxDefinitionLevel,
                               int maxRepetitionLevel,
                               int typeLength, PrimitiveLogicalType annotation) {

  /** Compatibility constructor: no logical annotation is inferred from a physical type. */
  public ColumnDescriptor(Type physicalType, String[] path, int maxDefinitionLevel,
                          int maxRepetitionLevel, int typeLength) {
    this(physicalType, path, maxDefinitionLevel, maxRepetitionLevel, typeLength,
        PrimitiveLogicalType.none());
  }

  public ColumnDescriptor {
    path = path.clone();
    annotation = java.util.Objects.requireNonNull(annotation, "annotation")
        .withPhysicalType(physicalType, typeLength);
  }

  @Override
  public String[] path() {
    return path.clone();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ColumnDescriptor that
        && physicalType == that.physicalType
        && java.util.Arrays.equals(path, that.path)
        && maxDefinitionLevel == that.maxDefinitionLevel
        && maxRepetitionLevel == that.maxRepetitionLevel
        && typeLength == that.typeLength
        && annotation.equals(that.annotation);
  }

  @Override
  public int hashCode() {
    int result = java.util.Objects.hash(physicalType, maxDefinitionLevel, maxRepetitionLevel,
        typeLength, annotation);
    return 31 * result + java.util.Arrays.hashCode(path);
  }

  /**
   * Returns the column path as a dot-separated string.
   *
   * @return the schema path joined with dots (e.g., "user.address.street")
   */
  public String getPathString() {
    return String.join(".", path);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (maxRepetitionLevel > 0) {
      sb.append("repeated ");
    } else if (maxDefinitionLevel > 0) {
      sb.append("optional ");
    } else {
      sb.append("required ");
    }
    sb.append(physicalType.name().toLowerCase());
    if (physicalType == Type.FIXED_LEN_BYTE_ARRAY) {
      sb.append("(").append(typeLength).append(")");
    }
    sb.append(" ").append(getPathString());
    return sb.toString();
  }
}
