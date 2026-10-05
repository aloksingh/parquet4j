package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ColumnBatch;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Main class for reading Parquet files.
 *
 * <p>This class provides the primary interface for reading Parquet files, supporting
 * both file-based and chunk-based reading. It allows access to file metadata, schema,
 * row groups, and individual columns.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (ParquetFileReader reader = new ParquetFileReader("data.parquet")) {
 *   System.out.println("Total rows: " + reader.getTotalRowCount());
 *   RowGroupReader rowGroup = reader.getRowGroup(0);
 *   ColumnValues column = rowGroup.readColumn(0);
 *   ColumnBatch batch = rowGroup.readColumnBatch(0);  // primitive columnar view
 * }
 * }</pre>
 *
 * @see ParquetMetadata
 * @see RowGroupReader
 * @see RowColumnGroupIterator
 */
public class ParquetFileReader implements AutoCloseable {
  private final ChunkReader chunkReader;
  private final ParquetMetadata metadata;
  private final boolean ownsChunkReader;
  private final String sourceDescription;

  /**
   * Creates a reader from a file path.
   *
   * <p>This constructor will open the file and read its metadata. The file
   * will be closed when {@link #close()} is called.
   *
   * @param filePath the path to the Parquet file
   * @throws IOException if an I/O error occurs while reading the file or metadata
   */
  public ParquetFileReader(String filePath) throws IOException {
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
  public ParquetFileReader(Path path) throws IOException {
    this.chunkReader = new FileChunkReader(path);
    this.ownsChunkReader = true;
    this.sourceDescription = path.toString();
    try {
      this.metadata = ParquetMetadataReader.readMetadata(chunkReader);
    } catch (IOException | RuntimeException | Error failure) {
      try {
        ((FileChunkReader) chunkReader).close();
      } catch (IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
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
  public ParquetFileReader(ChunkReader chunkReader) throws IOException {
    this.chunkReader = chunkReader;
    this.ownsChunkReader = false;
    this.sourceDescription = chunkReader instanceof FileChunkReader fileChunk
        ? fileChunk.getPath().toString()
        : chunkReader.getClass().getSimpleName() + "@" + Integer.toHexString(
            System.identityHashCode(chunkReader));
    this.metadata = ParquetMetadataReader.readMetadata(chunkReader);
  }

  /**
   * A short description of the underlying source (the file path when this reader was opened
   * from one). Surfaced in read/decode error messages so failures name their file.
   */
  public String getSourceDescription() {
    return sourceDescription;
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

  /** Creates a lazy scan with projection and bounded read options. */
  public ParquetRowIterator rowIterator(ReadOptions options) {
    return new ParquetRowIterator(this, false, options);
  }

  /**
   * Reader for a single row group.
   *
   * <p>A row group contains a subset of rows from the file and provides access
   * to individual columns within that row group.
   *
   * @see ParquetFileReader#getRowGroup(int)
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

    /**
     * Reads all values of a column as one owning primitive {@link ColumnBatch}.
     *
     * <p>This is the columnar companion to {@link #readColumn(int)}: both views are
     * adapters over the same lazily decoded pages. The pages of the column chunk are
     * decoded lazily and materialized one page at a time (the same cached per-page
     * decode {@link ColumnValues} uses), without per-value boxing — required
     * nonrepeated columns go straight to primitive arrays, binary values flow through
     * one shared offsets+payload buffer, and dictionary indexes are preserved until
     * the caller materializes a value.
     *
     * <p>The returned batch is an independent copy (copy-on-construct): it never
     * aliases page buffers, numeric accessors hand out read-only views, and the batch
     * remains valid after this reader is closed. For repeated columns the batch holds
     * one entry per level event, matching {@link ColumnValues#decodeAsInt32()} and
     * friends; repetition levels remain available through
     * {@link ColumnValues#decodedPages()}.
     *
     * @param columnIndex the index of the column to read (0-based)
     * @return one column batch for this column chunk
     * @throws IOException if an I/O error occurs while reading the column
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     */
    public ColumnBatch readColumnBatch(int columnIndex) throws IOException {
      return readColumn(columnIndex).toBatch();
    }

    /**
     * Reads a column as one owning primitive {@link ColumnBatch} by logical or
     * physical column name (the schema path such as {@code "a.b.element"}, or the
     * logical column name). See {@link #readColumnBatch(int)} for the materialization
     * and lifetime contract.
     *
     * @param columnName the logical column name or physical column path
     * @return one column batch for this column chunk
     * @throws IOException if an I/O error occurs while reading the column
     * @throws IllegalArgumentException if no column matches the name
     */
    public ColumnBatch readColumnBatch(String columnName) throws IOException {
      return readColumnBatch(physicalColumnIndex(columnName));
    }

    /**
     * Reads a column as one owning {@link ColumnBatch} per data page, decoded page
     * by page in order. Concatenating the page batches reproduces
     * {@link #readColumnBatch(int)} exactly.
     *
     * @param columnIndex the index of the column to read (0-based)
     * @return one column batch per data page of this column chunk
     * @throws IOException if an I/O error occurs while reading the column
     * @throws IndexOutOfBoundsException if the column index is out of bounds
     */
    public List<ColumnBatch> readColumnPageBatches(int columnIndex) throws IOException {
      return readColumn(columnIndex).toPageBatches();
    }

    /** Resolves a logical column name or physical column path to a physical index. */
    private int physicalColumnIndex(String columnName) {
      java.util.Objects.requireNonNull(columnName, "columnName");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        if (schema.getColumn(i).getPathString().equals(columnName)) {
          return i;
        }
      }
      LogicalColumnDescriptor logical = schema.getLogicalColumn(columnName);
      if (logical != null && logical.isPrimitive()) {
        String path = logical.getPhysicalDescriptor().getPathString();
        for (int i = 0; i < schema.getNumColumns(); i++) {
          if (schema.getColumn(i).getPathString().equals(path)) {
            return i;
          }
        }
      }
      throw new IllegalArgumentException("Column not found: " + columnName);
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
