package org.parquet.model;

/**
 * Column descriptor
 *
 * @param typeLength For FIXED_LEN_BYTE_ARRAY
 */
public record ColumnDescriptor(Type physicalType, String[] path, int maxDefinitionLevel,
                               int maxRepetitionLevel,
                               int typeLength) {

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
