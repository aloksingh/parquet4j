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
 * Main class for reading Parquet files.
 *
 * <p>This class provides the primary interface for reading Parquet files, supporting
 * both file-based and chunk-based reading. It allows access to file metadata, schema,
 * row groups, and individual columns.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (SerializedFileReader reader = new SerializedFileReader("data.parquet")) {
 *   System.out.println("Total rows: " + reader.getTotalRowCount());
 *   RowGroupReader rowGroup = reader.getRowGroup(0);
 *   ColumnValues column = rowGroup.readColumn(0);
 * }
 * }</pre>
 *
 * @see ParquetMetadata
 * @see RowGroupReader
 * @see RowColumnGroupIterator
 */
public class SerializedFileReader implements AutoCloseable {
  private final ChunkReader chunkReader;
  private final ParquetMetadata metadata;
  private final boolean ownsChunkReader;

  /**
   * Creates a reader from a file path.
   *
   * <p>This constructor will open the file and read its metadata. The file
   * will be closed when {@link #close()} is called.
   *
   * @param filePath the path to the Parquet file
   * @throws IOException if an I/O error occurs while reading the file or metadata
   */
  public SerializedFileReader(String filePath) throws IOException {
    this(Path.of(filePath));
  }

  /**
   * Creates a reader from a Path.
   *
   * <p>This constructor will open the file and read its metadata. The file
   * will be closed when {@link #close()} is called.
   *
   * @param path the path to the Parquet file
   * @throws IOException if an I/O error occurs while reading the file or metadata
   */
  public SerializedFileReader(Path path) throws IOException {
    this.chunkReader = new FileChunkReader(path);
    this.ownsChunkReader = true;
    this.metadata = ParquetMetadataReader.readMetadata(chunkReader);
  }

  /**
   * Creates a reader from an existing ChunkReader.
   *
   * <p>This constructor allows for custom chunk reading implementations. The
   * ChunkReader will NOT be closed when {@link #close()} is called, as it is
   * assumed to be managed externally.
   *
   * @param chunkReader the chunk reader to use for reading file data
   * @throws IOException if an I/O error occurs while reading metadata
   */
  public SerializedFileReader(ChunkReader chunkReader) throws IOException {
    this.chunkReader = chunkReader;
    this.ownsChunkReader = false;
    this.metadata = ParquetMetadataReader.readMetadata(chunkReader);
  }

  /**
   * Returns the file metadata.
   *
   * <p>The metadata includes file version, schema, row groups, and key-value metadata.
   *
   * @return the Parquet file metadata
   */
  public ParquetMetadata getMetadata() {
    return metadata;
  }

  /**
   * Returns the file schema.
   *
   * <p>The schema describes the structure of the data, including all columns
   * and their types.
   *
   * @return the schema descriptor for this file
   */
  public SchemaDescriptor getSchema() {
    return metadata.fileMetadata().schema();
  }

  /**
   * Returns the number of row groups in the file.
   *
   * <p>Row groups are the largest unit of data organization in a Parquet file.
   * Each row group contains a subset of the total rows.
   *
   * @return the number of row groups
   */
  public int getNumRowGroups() {
    return metadata.getNumRowGroups();
  }

  /**
   * Returns a reader for a specific row group.
   *
   * <p>Row groups are indexed from 0 to {@code getNumRowGroups() - 1}.
   *
   * @param index the index of the row group to read
   * @return a reader for the specified row group
   * @throws IndexOutOfBoundsException if the index is out of bounds
   */
  public RowGroupReader getRowGroup(int index) {
    if (index < 0 || index >= metadata.getNumRowGroups()) {
      throw new IndexOutOfBoundsException(
          "Row group index out of bounds: " + index);
    }
    return new RowGroupReader(chunkReader, metadata.rowGroups().get(index), getSchema());
  }

  /**
   * Returns the total number of rows in the file.
   *
   * <p>This is the sum of rows across all row groups.
   *
   * @return the total number of rows
   */
  public long getTotalRowCount() {
    return metadata.fileMetadata().numRows();
  }

  /**
   * Creates an iterator to read the file row by row.
   *
   * <p>The iterator will close this file reader when iteration is complete.
   *
   * @return an iterator over all rows in the file
   */
  public RowColumnGroupIterator rowIterator() {
    return new ParquetRowIterator(this, false);
  }

  /**
   * Creates an iterator to read the file row by row.
   *
   * @param closeOnComplete whether to close this file reader when iteration is complete
   * @return an iterator over all rows in the file
   */
  public RowColumnGroupIterator rowIterator(boolean closeOnComplete) {
    return new ParquetRowIterator(this, closeOnComplete);
  }

  /**
   * Reader for a single row group.
   *
   * <p>A row group contains a subset of rows from the file and provides access
   * to individual columns within that row group.
   *
   * @see SerializedFileReader#getRowGroup(int)
   */
  public static class RowGroupReader {
    private final ChunkReader chunkReader;
    private final ParquetMetadata.RowGroupMetadata rowGroupMeta;
    private final SchemaDescriptor schema;

    /**
     * Constructs a RowGroupReader.
     *
     * @param chunkReader the chunk reader for reading file data
     * @param rowGroupMeta the metadata for this row group
     * @param schema the file schema
     */
    public RowGroupReader(ChunkReader chunkReader,
                          ParquetMetadata.RowGroupMetadata rowGroupMeta,
                          SchemaDescriptor schema) {
      this.chunkReader = chunkReader;
      this.rowGroupMeta = rowGroupMeta;
      this.schema = schema;
    }

    /**
     * Returns the row group metadata.
     *
     * <p>The metadata includes information about the number of rows, columns,
     * and total byte size of this row group.
     *
     * @return the row group metadata
     */
    public ParquetMetadata.RowGroupMetadata getMetadata() {
      return rowGroupMeta;
    }

    /**
     * Returns the number of columns in this row group.
     *
     * @return the number of columns
     */
    public int getNumColumns() {
      return rowGroupMeta.getNumColumns();
    }

    /**
     * Returns the number of rows in this row group.
     *
     * @return the number of rows
     */
    public long getNumRows() {
      return rowGroupMeta.numRows();
    }

    /**
     * Returns a page reader for a specific column.
     *
     * <p>The page reader provides low-level access to the pages that make up
     * the column data.
     *
     * @param columnIndex the index of the column (0-based)
     * @return a page reader for the specified column
     * @throws IndexOutOfBoundsException if the column index is out of bounds
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
     * Reads all values from a column.
     *
     * <p>This method reads all pages for the specified column and returns them
     * as a {@link ColumnValues} object, which provides methods for accessing
     * the typed values.
     *
     * <p><strong>Note:</strong> This is a simplified implementation that primarily
     * supports PLAIN encoding. Other encodings may not be fully supported.
     *
     * @param columnIndex the index of the column to read (0-based)
     * @return the column values
     * @throws IOException if an I/O error occurs while reading the column
     * @throws IndexOutOfBoundsException if the column index is out of bounds
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

  /**
   * Closes this reader and releases any system resources associated with it.
   *
   * <p>If this reader was created with a file path or Path, the underlying file
   * will be closed. If it was created with an external ChunkReader, that reader
   * will NOT be closed (it must be managed externally).
   *
   * @throws IOException if an I/O error occurs while closing
   */
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
   * Prints file metadata information to standard output.
   *
   * <p>This is a utility method for debugging and inspection. It prints:
   * <ul>
   *   <li>File version</li>
   *   <li>Total row count</li>
   *   <li>Number of row groups</li>
   *   <li>Schema structure</li>
   *   <li>Row group details (rows, size, columns)</li>
   *   <li>Key-value metadata (if present)</li>
   * </ul>
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
