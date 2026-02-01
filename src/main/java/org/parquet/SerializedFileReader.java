package org.parquet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ColumnValues;
import org.parquet.model.LogicalColumnDescriptor;
import org.parquet.model.Page;
import org.parquet.model.ParquetMetadata;
import org.parquet.model.SchemaDescriptor;

/**
 * Main class for reading Parquet files
 */
public class SerializedFileReader implements AutoCloseable {
  private final ChunkReader chunkReader;
  private final ParquetMetadata metadata;
  private final boolean ownsChunkReader;

  /**
   * Create a reader from a file path
   */
  public SerializedFileReader(String filePath) throws IOException {
    this(Path.of(filePath));
  }

  /**
   * Create a reader from a Path
   */
  public SerializedFileReader(Path path) throws IOException {
    this.chunkReader = new FileChunkReader(path);
    this.ownsChunkReader = true;
    this.metadata = ParquetMetadataReader.readMetadata(chunkReader);
  }

  /**
   * Create a reader from an existing ChunkReader
   */
  public SerializedFileReader(ChunkReader chunkReader) throws IOException {
    this.chunkReader = chunkReader;
    this.ownsChunkReader = false;
    this.metadata = ParquetMetadataReader.readMetadata(chunkReader);
  }

  /**
   * Get the file metadata
   */
  public ParquetMetadata getMetadata() {
    return metadata;
  }

  /**
   * Get the file schema
   */
  public SchemaDescriptor getSchema() {
    return metadata.fileMetadata().schema();
  }

  /**
   * Get the number of row groups
   */
  public int getNumRowGroups() {
    return metadata.getNumRowGroups();
  }

  /**
   * Get a specific row group
   */
  public RowGroupReader getRowGroup(int index) {
    if (index < 0 || index >= metadata.getNumRowGroups()) {
      throw new IndexOutOfBoundsException(
          "Row group index out of bounds: " + index);
    }
    return new RowGroupReader(chunkReader, metadata.rowGroups().get(index), getSchema());
  }

  /**
   * Get the total number of rows in the file
   */
  public long getTotalRowCount() {
    return metadata.fileMetadata().numRows();
  }

  /**
   * Create an iterator to read the file row by row
   * The iterator will close this file reader when iteration is complete
   *
   * @return An iterator over all rows in the file
   */
  public RowColumnGroupIterator rowIterator() {
    return new ParquetRowIterator(this, false);
  }

  /**
   * Create an iterator to read the file row by row
   *
   * @param closeOnComplete Whether to close this file reader when iteration is complete
   * @return An iterator over all rows in the file
   */
  public RowColumnGroupIterator rowIterator(boolean closeOnComplete) {
    return new ParquetRowIterator(this, closeOnComplete);
  }

  /**
   * Reader for a single row group
   */
  public static class RowGroupReader {
    private final ChunkReader chunkReader;
    private final ParquetMetadata.RowGroupMetadata rowGroupMeta;
    private final SchemaDescriptor schema;

    public RowGroupReader(ChunkReader chunkReader,
                          ParquetMetadata.RowGroupMetadata rowGroupMeta,
                          SchemaDescriptor schema) {
      this.chunkReader = chunkReader;
      this.rowGroupMeta = rowGroupMeta;
      this.schema = schema;
    }

    /**
     * Get the row group metadata
     */
    public ParquetMetadata.RowGroupMetadata getMetadata() {
      return rowGroupMeta;
    }

    /**
     * Get the number of columns in this row group
     */
    public int getNumColumns() {
      return rowGroupMeta.getNumColumns();
    }

    /**
     * Get the number of rows in this row group
     */
    public long getNumRows() {
      return rowGroupMeta.numRows();
    }

    /**
     * Get a page reader for a specific column
     */
    public PageReader getColumnPageReader(int columnIndex) {
      if (columnIndex < 0 || columnIndex >= rowGroupMeta.getNumColumns()) {
        throw new IndexOutOfBoundsException(
            "Column index out of bounds: " + columnIndex);
      }

      ParquetMetadata.ColumnChunkMetadata columnMeta =
          rowGroupMeta.columns().get(columnIndex);

      // Get the column descriptor from the schema
      ColumnDescriptor columnDescriptor =
          schema.getColumn(columnIndex);

      return new PageReader(chunkReader, columnMeta, columnDescriptor);
    }

    /**
     * Read all values from a column (simplified - reads PLAIN encoded data only)
     */
    public ColumnValues readColumn(int columnIndex) throws IOException {
      PageReader pageReader = getColumnPageReader(columnIndex);
      List<Page> pages = pageReader.readAllPages();

      ParquetMetadata.ColumnChunkMetadata columnMeta =
          rowGroupMeta.columns().get(columnIndex);

      ColumnDescriptor columnDescriptor =
          schema.getColumn(columnIndex);

      // Find the logical column descriptor for this physical column
      LogicalColumnDescriptor logicalColumnDescriptor =
          schema.findLogicalColumnByPhysicalIndex(columnIndex);

      return new ColumnValues(columnMeta.type(), pages, columnDescriptor, logicalColumnDescriptor);
    }
  }

  @Override
  public void close() throws IOException {
    if (ownsChunkReader && chunkReader instanceof AutoCloseable) {
      try {
        ((AutoCloseable) chunkReader).close();
      } catch (Exception e) {
        if (e instanceof IOException) {
          throw (IOException) e;
        }
        throw new IOException("Failed to close chunk reader", e);
      }
    }
  }

  /**
   * Print file metadata information
   */
  public void printMetadata() {
    System.out.println("=== Parquet File Metadata ===");
    System.out.println("Version: " + metadata.fileMetadata().version());
    System.out.println("Total rows: " + metadata.fileMetadata().numRows());
    System.out.println("Number of row groups: " + metadata.getNumRowGroups());
    System.out.println("\nSchema:");
    System.out.println(metadata.fileMetadata().schema());

    System.out.println("\nRow Groups:");
    for (int i = 0; i < metadata.getNumRowGroups(); i++) {
      ParquetMetadata.RowGroupMetadata rg = metadata.rowGroups().get(i);
      System.out.printf("  Row Group %d: %d rows, %d bytes%n",
          i, rg.numRows(), rg.totalByteSize());

      for (int j = 0; j < rg.getNumColumns(); j++) {
        ParquetMetadata.ColumnChunkMetadata col = rg.columns().get(j);
        System.out.printf("    Column %d (%s): %s, codec=%s, values=%d%n",
            j, String.join(".", col.path()),
            col.type(), col.codec(), col.numValues());
      }
    }

    if (!metadata.fileMetadata().keyValueMetadata().isEmpty()) {
      System.out.println("\nKey-Value Metadata:");
      metadata.fileMetadata().keyValueMetadata().forEach(
          (k, v) -> System.out.printf("  %s: %s%n", k, v));
    }
  }
}
