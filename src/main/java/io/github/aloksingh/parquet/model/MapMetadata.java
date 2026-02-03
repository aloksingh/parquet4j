package io.github.aloksingh.parquet.model;

/**
 * Metadata for a MAP logical type column.
 * <p>
 * Maps in Parquet are stored as two physical columns: one for keys and one for values.
 * This record holds the metadata necessary to read and interpret map data from the
 * Parquet file format.
 *
 * @param keyColumnIndex   the column index for map keys in the Parquet schema
 * @param valueColumnIndex the column index for map values in the Parquet schema
 * @param keyType          the data type of the map keys
 * @param valueType        the data type of the map values
 * @param keyDescriptor    the column descriptor for the key column
 * @param valueDescriptor  the column descriptor for the value column
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
