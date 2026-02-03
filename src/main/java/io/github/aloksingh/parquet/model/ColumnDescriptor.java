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
                               int typeLength) {

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
