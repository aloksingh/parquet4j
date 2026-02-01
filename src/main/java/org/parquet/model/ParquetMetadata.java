package org.parquet.model;

import java.util.List;
import java.util.Map;

/**
 * Parquet file metadata
 */
public record ParquetMetadata(FileMetadata fileMetadata, List<RowGroupMetadata> rowGroups) {

  public int getNumRowGroups() {
    return rowGroups.size();
  }

  /**
   * File-level metadata
   */
  public record FileMetadata(int version, SchemaDescriptor schema, long numRows,
                             Map<String, String> keyValueMetadata) {
  }

  /**
   * Row group metadata
   */
  public record RowGroupMetadata(List<ColumnChunkMetadata> columns, long totalByteSize,
                                 long numRows) {

    public int getNumColumns() {
      return columns.size();
    }
  }

  /**
   * Column chunk metadata
   */
  public record ColumnChunkMetadata(Type type, String[] path, CompressionCodec codec,
                                    long dataPageOffset,
                                    long dictionaryPageOffset, long totalCompressedSize,
                                    long totalUncompressedSize,
                                    long numValues, ColumnStatistics statistics) {

    public long getFirstDataPageOffset() {
      return dictionaryPageOffset > 0 ? dictionaryPageOffset : dataPageOffset;
    }
  }

  /**
   * Column statistics
   */
  public record ColumnStatistics(byte[] min, byte[] max, Long nullCount, Long distinctCount) {

    public boolean hasMin() {
      return min != null && min.length > 0;
    }

    public boolean hasMax() {
      return max != null && max.length > 0;
    }

    public boolean hasNullCount() {
      return nullCount != null;
    }

    public boolean hasDistinctCount() {
      return distinctCount != null;
    }
  }
}
