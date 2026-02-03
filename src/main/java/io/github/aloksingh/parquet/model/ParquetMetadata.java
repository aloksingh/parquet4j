package io.github.aloksingh.parquet.model;

import java.util.List;
import java.util.Map;

/**
 * Represents the complete metadata structure of a Parquet file.
 * This includes file-level metadata and metadata for all row groups.
 *
 * @param fileMetadata the file-level metadata including schema and row count
 * @param rowGroups    the list of row group metadata entries
 */
public record ParquetMetadata(FileMetadata fileMetadata, List<RowGroupMetadata> rowGroups) {

  /**
   * Gets the number of row groups in this Parquet file.
   *
   * @return the count of row groups
   */
  public int getNumRowGroups() {
    return rowGroups.size();
  }

  /**
   * File-level metadata containing schema information and global properties.
   *
   * @param version          the Parquet format version number
   * @param schema           the schema descriptor defining the file structure
   * @param numRows          the total number of rows across all row groups
   * @param keyValueMetadata custom key-value metadata stored in the file
   */
  public record FileMetadata(int version, SchemaDescriptor schema, long numRows,
                             Map<String, String> keyValueMetadata) {
  }

  /**
   * Metadata for a single row group within the Parquet file.
   * A row group is a logical horizontal partitioning of the data into rows.
   *
   * @param columns       the list of column chunk metadata for all columns in this row group
   * @param totalByteSize the total compressed byte size of all column chunks in this row group
   * @param numRows       the number of rows in this row group
   */
  public record RowGroupMetadata(List<ColumnChunkMetadata> columns, long totalByteSize,
                                 long numRows) {

    /**
     * Gets the number of columns in this row group.
     *
     * @return the count of columns
     */
    public int getNumColumns() {
      return columns.size();
    }
  }

  /**
   * Metadata for a single column chunk within a row group.
   * Contains information about compression, size, and page offsets.
   *
   * @param type                   the data type of this column
   * @param path                   the path to this column in the schema (for nested structures)
   * @param codec                  the compression codec used for this column chunk
   * @param dataPageOffset         the byte offset to the first data page
   * @param dictionaryPageOffset   the byte offset to the dictionary page (0 if no dictionary encoding)
   * @param totalCompressedSize    the total compressed size of this column chunk in bytes
   * @param totalUncompressedSize  the total uncompressed size of this column chunk in bytes
   * @param numValues              the total number of values in this column chunk
   * @param statistics             the column statistics (min, max, null count, distinct count)
   */
  public record ColumnChunkMetadata(Type type, String[] path, CompressionCodec codec,
                                    long dataPageOffset,
                                    long dictionaryPageOffset, long totalCompressedSize,
                                    long totalUncompressedSize,
                                    long numValues, ColumnStatistics statistics) {

    /**
     * Gets the file offset to the first page (dictionary or data) in this column chunk.
     *
     * @return the byte offset to read from; returns dictionary page offset if present, otherwise data page offset
     */
    public long getFirstDataPageOffset() {
      return dictionaryPageOffset > 0 ? dictionaryPageOffset : dataPageOffset;
    }
  }

  /**
   * Statistical information about a column chunk.
   * These statistics can be used for query optimization and data skipping.
   *
   * @param min           the minimum value in this column chunk (encoded as bytes), or null if not available
   * @param max           the maximum value in this column chunk (encoded as bytes), or null if not available
   * @param nullCount     the count of null values, or null if not tracked
   * @param distinctCount the count of distinct values, or null if not tracked
   */
  public record ColumnStatistics(byte[] min, byte[] max, Long nullCount, Long distinctCount) {

    /**
     * Checks if minimum value statistics are available.
     *
     * @return true if min value is present and non-empty
     */
    public boolean hasMin() {
      return min != null && min.length > 0;
    }

    /**
     * Checks if maximum value statistics are available.
     *
     * @return true if max value is present and non-empty
     */
    public boolean hasMax() {
      return max != null && max.length > 0;
    }

    /**
     * Checks if null count statistics are available.
     *
     * @return true if null count is tracked
     */
    public boolean hasNullCount() {
      return nullCount != null;
    }

    /**
     * Checks if distinct count statistics are available.
     *
     * @return true if distinct count is tracked
     */
    public boolean hasDistinctCount() {
      return distinctCount != null;
    }
  }
}
