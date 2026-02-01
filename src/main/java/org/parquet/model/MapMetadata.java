package org.parquet.model;

/**
 * Metadata for a MAP logical type column.
 * Maps are stored as two physical columns: keys and values.
 */
public record MapMetadata(int keyColumnIndex, int valueColumnIndex, Type keyType, Type valueType,
                          ColumnDescriptor keyDescriptor,
                          ColumnDescriptor valueDescriptor) {

  @Override
  public String toString() {
    return "MapMetadata{keyCol=" + keyColumnIndex + ", valueCol=" + valueColumnIndex +
        ", keyType=" + keyType + ", valueType=" + valueType + "}";
  }
}
