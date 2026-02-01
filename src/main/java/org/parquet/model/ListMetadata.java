package org.parquet.model;

/**
 * Metadata for a LIST logical type column.
 * Lists are stored as a single physical column with repetition.
 */
public record ListMetadata(int elementColumnIndex, Type elementType,
                           ColumnDescriptor elementDescriptor) {

  @Override
  public String toString() {
    return "ListMetadata{elementCol=" + elementColumnIndex + ", elementType=" + elementType + "}";
  }
}
