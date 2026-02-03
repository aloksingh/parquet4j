package io.github.aloksingh.parquet.model;

/**
 * Metadata for a LIST logical type column.
 * <p>
 * Lists in Parquet are stored as a single physical column with repetition levels
 * to track the boundaries between list elements. The repetition level indicates
 * whether a value is part of the current list or starts a new list.
 *
 * @param elementColumnIndex the column index for list elements in the Parquet schema
 * @param elementType        the data type of the list elements
 * @param elementDescriptor  the column descriptor for the element column
 */
public record ListMetadata(int elementColumnIndex, Type elementType,
                           ColumnDescriptor elementDescriptor) {

  @Override
  public String toString() {
    return "ListMetadata{elementCol=" + elementColumnIndex + ", elementType=" + elementType + "}";
  }
}
