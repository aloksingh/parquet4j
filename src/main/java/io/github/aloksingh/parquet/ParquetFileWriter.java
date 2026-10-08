package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.*;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.writer.*;
import org.apache.parquet.format.*;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Parquet file writer that implements the ParquetWriter interface.
 * This writer supports basic data types, MAP types, and PLAIN encoding with optional compression.
 *
 * <p>Features:</p>
 * <ul>
 *   <li>Supports primitive types: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY</li>
 *   <li>Supports MAP type with proper hierarchical schema encoding</li>
 *   <li>PLAIN encoding for values, optional RLE_DICTIONARY encoding with PLAIN fallback
 *       (one dictionary page per column chunk; BOOLEAN columns are never dictionary encoded)</li>
 *   <li>RLE/Bit-Packing Hybrid encoding for definition and repetition levels</li>
 *   <li>Optional compression: UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD, LZ4_RAW</li>
 *   <li>Automatic row group management</li>
 *   <li>Column statistics generation</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * SchemaDescriptor schema = SchemaDescriptor.builder()
 *     .name("example")
 *     .addColumn(ColumnDescriptor.primitive("id", Type.INT32))
 *     .addColumn(ColumnDescriptor.primitive("name", Type.BYTE_ARRAY))
 *     .build();
 *
 * try (ParquetFileWriter writer = new ParquetFileWriter(
 *         Paths.get("output.parquet"),
 *         schema,
 *         CompressionCodec.SNAPPY)) {
 *     writer.addRow(RowColumnGroup.builder(schema)
 *         .add("id", 1)
 *         .add("name", "Alice")
 *         .build());
 * }
 * }</pre>
 */
public class ParquetFileWriter implements ParquetWriter {
  private static final byte[] PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
  private static final int DEFAULT_PAGE_SIZE = 1024 * 1024; // 1MB
  private static final int DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024; // 128MB

  private final Path filePath;
  private final SchemaDescriptor schema;
  private final CompressionCodec compressionCodec;
  private final int pageSize;
  private final int rowGroupSize;

  private OutputStream outputStream;
  private Path temporaryPath;
  private long currentPosition;
  private final List<RowGroup> rowGroups;
  private final WriterColumnBuilder[] columns;
  private final WriterColumnBuffer[] stagedRow;
  private long pendingRowCount;
  private long totalRowCount;
  private enum State { NEW, OPEN, FAILED, CLOSED }
  private State state;
  private final Compressor compressor;
  private final double minCompressionRatio;

  /**
   * Create a new ParquetFileWriter with default settings.
   *
   * @param filePath Path to the output Parquet file
   * @param schema   Schema descriptor for the file
   */
  public ParquetFileWriter(Path filePath, SchemaDescriptor schema) {
    this(filePath, schema, CompressionCodec.UNCOMPRESSED, DEFAULT_PAGE_SIZE,
        DEFAULT_ROW_GROUP_SIZE);
  }

  /**
   * Create a new ParquetFileWriter with custom settings.
   *
   * @param filePath         Path to the output Parquet file
   * @param schema           Schema descriptor for the file
   * @param compressionCodec Compression codec to use
   * @param pageSize         Target page size in bytes
   * @param rowGroupSize     Target row group size in bytes
   */
  public ParquetFileWriter(Path filePath, SchemaDescriptor schema,
                           CompressionCodec compressionCodec, int pageSize,
                           int rowGroupSize) {
    this(filePath, schema, compressionCodec, pageSize, rowGroupSize,
        DEFAULT_MIN_COMPRESSION_RATIO);
  }

  private static final double DEFAULT_MIN_COMPRESSION_RATIO = 0.90;

  /**
   * Create a new ParquetFileWriter with custom settings and compression ratio threshold.
   *
   * @param filePath              Path to the output Parquet file
   * @param schema                Schema descriptor for the file
   * @param compressionCodec      Compression codec to use
   * @param pageSize              Target page size in bytes
   * @param rowGroupSize          Target row group size in bytes
   * @param minCompressionRatio   Minimum ratio of compressed/uncompressed size required
   *                              to keep compression (0.0 to 1.0). 0.90 means compression
   *                              is kept only when it achieves at least 10% reduction.
   */
  public ParquetFileWriter(Path filePath, SchemaDescriptor schema,
                           CompressionCodec compressionCodec, int pageSize,
                           int rowGroupSize, double minCompressionRatio) {
    this(filePath, schema, compressionCodec, pageSize, rowGroupSize, minCompressionRatio,
            DictionaryOptions.disabled());
  }

  /**
   * Create a new ParquetFileWriter with custom settings, compression ratio threshold,
   * and dictionary encoding options.
   *
   * @param filePath            Path to the output Parquet file
   * @param schema              Schema descriptor for the file
   * @param compressionCodec    Compression codec to use
   * @param pageSize            Target page size in bytes
   * @param rowGroupSize        Target row group size in bytes
   * @param minCompressionRatio Minimum ratio of compressed/uncompressed size required
   *                            to keep compression (0.0 to 1.0). 0.90 means compression
   *                            is kept only when it achieves at least 10% reduction.
   * @param dictionary          RLE_DICTIONARY encoding options; when enabled, eligible
   *                            columns are dictionary encoded with PLAIN fallback
   */
  public ParquetFileWriter(Path filePath, SchemaDescriptor schema,
                           CompressionCodec compressionCodec, int pageSize,
                           int rowGroupSize, double minCompressionRatio,
                           DictionaryOptions dictionary) {
    WriterSchema.validate(schema);
    if (filePath == null) throw new IllegalArgumentException("Output path must not be null");
    if (compressionCodec == null) throw new IllegalArgumentException("Compression codec must not be null");
    if (dictionary == null) throw new IllegalArgumentException("Dictionary options must not be null");
    if (pageSize <= 0 || rowGroupSize <= 0) throw new IllegalArgumentException("Byte targets must be positive");
    if (!Double.isFinite(minCompressionRatio) || minCompressionRatio < 0 || minCompressionRatio > 1) {
      throw new IllegalArgumentException("Compression ratio must be finite and within [0, 1]");
    }
    this.filePath = filePath;
    this.schema = schema;
    this.compressionCodec = compressionCodec;
    this.pageSize = pageSize;
    this.rowGroupSize = rowGroupSize;
    this.rowGroups = new ArrayList<>();
    this.columns = new WriterColumnBuilder[schema.getNumColumns()];
    this.stagedRow = new WriterColumnBuffer[schema.getNumColumns()];
    int byteLimit = pageBodyLimit();
    int valueLimit = pageValueLimit();
    for (int i = 0; i < schema.getNumColumns(); i++) {
      columns[i] = new WriterColumnBuilder(schema.getColumn(i), pageSize, byteLimit, valueLimit, dictionary);
      stagedRow[i] = new WriterColumnBuffer(schema.getColumn(i), byteLimit, valueLimit);
    }
    this.currentPosition = 0;
    this.totalRowCount = 0;
    this.state = State.NEW;
    this.compressor = compressionCodec == CompressionCodec.UNCOMPRESSED || minCompressionRatio == 0.0
        ? null : Compressor.create(compressionCodec);
    this.minCompressionRatio = minCompressionRatio;
  }

  /**
   * Initialize the writer and write the file header.
   *
   * @throws IOException if file cannot be created or header cannot be written
   */
  public void start() throws IOException {
    if (state != State.NEW) {
      throw new IllegalStateException("Writer cannot start in state " + state);
    }

    try {
      temporaryPath = Files.createTempFile(filePath.toAbsolutePath().getParent(),
          ".parquet4j-", ".tmp");
      outputStream = openSink(temporaryPath);
      outputStream.write(PARQUET_MAGIC);
      currentPosition += PARQUET_MAGIC.length;
      state = State.OPEN;
    } catch (IOException | RuntimeException | Error failure) {
      fail(failure);
      throw failure;
    }
  }

  // Package-private format-limit seams allow deterministic boundary tests without huge allocations.
  int pageBodyLimit() { return WriterColumnBuilder.MAX_PAGE_BODY_SIZE; }
  int pageValueLimit() { return Integer.MAX_VALUE; }

  // Package-private seam for deterministic sink failure tests.
  OutputStream openSink(Path path) throws IOException {
    return Files.newOutputStream(path, StandardOpenOption.WRITE);
  }

  private void fail(Throwable failure) {
    state = State.FAILED;
    OutputStream failedSink = outputStream;
    outputStream = null;
    if (failedSink != null) {
      try {
        failedSink.close();
      } catch (IOException | RuntimeException | Error cleanupFailure) {
        if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
      }
    }
    if (temporaryPath != null) {
      try {
        Files.deleteIfExists(temporaryPath);
      } catch (IOException | RuntimeException | Error cleanupFailure) {
        if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
      }
      temporaryPath = null;
    }
    for (WriterColumnBuilder column : columns) column.clear();
    for (WriterColumnBuffer column : stagedRow) column.clear();
    pendingRowCount = 0;
  }

  /**
   * Add a row to the Parquet file. Rows are buffered into the current row group, which is
   * flushed when adding another row would push the row group's projected encoded size past
   * the {@code rowGroupSize} byte target (and on {@link #close()} for any remainder).
   * Within a row group, each column is split into data pages whenever the next row would
   * push the current page past the {@code pageSize} byte target; flushing is byte-target
   * based, not row-count based.
   *
   * @param row Row data to add to the file
   * @throws IllegalStateException if writer is closed
   * @throws IllegalArgumentException if row schema doesn't match writer schema
   * @throws ParquetException if writing fails
   */
  @Override
  public void addRow(RowColumnGroup row) {
    if (state == State.CLOSED || state == State.FAILED) {
      throw new IllegalStateException("Writer cannot add rows in state " + state);
    }
    try {
      WriterSchema.validateRow(schema, row);
      for (WriterColumnBuffer column : stagedRow) column.clear();
      if (schema.hasLogicalColumns()) {
        int physical = 0;
        for (int logical = 0; logical < schema.getNumLogicalColumns(); logical++) {
          LogicalColumnDescriptor column = schema.getLogicalColumn(logical);
          Object value = row.getColumnValue(logical);
          if (column.isPrimitive()) {
            stagePrimitive(stagedRow[physical++], value);
          } else {
            MapRowStaging.appendRow(value, column.getMapMetadata(),
                stagedRow[physical], stagedRow[physical + 1]);
            physical += 2;
          }
        }
      } else {
        for (int i = 0; i < stagedRow.length; i++) stagePrimitive(stagedRow[i], row.getColumnValue(i));
      }
      // Only a fully validated, detached, encoded row can reach the transaction/group buffers.
      long projectedGroupSize = 0;
      for (int i = 0; i < columns.length; i++) {
        projectedGroupSize = Math.addExact(projectedGroupSize, columns[i].projectedPayloadSize(stagedRow[i]));
      }
      if (pendingRowCount > 0 && projectedGroupSize > rowGroupSize) flushRowGroup();
      if (state == State.NEW) start();
      for (int i = 0; i < columns.length; i++) columns[i].appendRow(stagedRow[i]);
      pendingRowCount = Math.addExact(pendingRowCount, 1);
      for (WriterColumnBuffer column : stagedRow) column.clear();

    } catch (IOException failure) {
      fail(failure);
      throw new ParquetException("Failed to write row group", failure);
    } catch (RuntimeException | Error failure) {
      fail(failure);
      throw failure;
    }
  }

  private void stagePrimitive(WriterColumnBuffer column, Object value) {
    int maximum = column.descriptor().maxDefinitionLevel();
    column.add(value, value == null ? 0 : maximum, 0, maximum > 0);
  }

  private void flushRowGroup() throws IOException {
    if (pendingRowCount == 0) return;
    List<ColumnChunk> chunks = new ArrayList<>(columns.length);
    long uncompressed = 0;
    long compressed = 0;
    for (WriterColumnBuilder column : columns) {
      ColumnChunk chunk = writeColumnChunk(column);
      chunks.add(chunk);
      uncompressed = Math.addExact(uncompressed, chunk.getMeta_data().getTotal_uncompressed_size());
      compressed = Math.addExact(compressed, chunk.getMeta_data().getTotal_compressed_size());
    }
    RowGroup group = new RowGroup();
    group.setColumns(chunks);
    group.setTotal_byte_size(uncompressed);
    group.setTotal_compressed_size(compressed);
    group.setNum_rows(pendingRowCount);
    rowGroups.add(group);
    totalRowCount = Math.addExact(totalRowCount, pendingRowCount);
    pendingRowCount = 0;
    for (WriterColumnBuilder column : columns) column.clear();
  }

  private ColumnChunk writeColumnChunk(WriterColumnBuilder column) throws IOException {
    long start = currentPosition;
    long uncompressed = 0;
    long compressed = 0;
    boolean retainedCompression = false;
    List<WriterPage> pages = column.finishAndGetPages();
    boolean dictionaryEncoded = false;
    for (WriterPage page : pages) {
      dictionaryEncoded |= page.encoding() == Encoding.RLE_DICTIONARY;
    }
    long dictionaryOffset = -1;
    if (dictionaryEncoded) {
      // One dictionary page per chunk, always the chunk's first page.
      PageInfo dictionary = writeDictionaryPage(column.dictionary());
      dictionaryOffset = start;
      uncompressed = Math.addExact(uncompressed, dictionary.total_uncompressed_size);
      compressed = Math.addExact(compressed, dictionary.total_compressed_size);
      retainedCompression |= dictionary.effectiveCodec != CompressionCodec.UNCOMPRESSED;
    }
    long dataPageOffset = currentPosition;
    for (WriterPage data : pages) {
      PageInfo page = writeDataPage(data);
      uncompressed = Math.addExact(uncompressed, page.total_uncompressed_size);
      compressed = Math.addExact(compressed, page.total_compressed_size);
      retainedCompression |= page.effectiveCodec != CompressionCodec.UNCOMPRESSED;
    }
    ColumnDescriptor descriptor = column.descriptor();
    ColumnMetaData metadata = new ColumnMetaData();
    metadata.setType(WriterFileSchema.convertType(descriptor.physicalType()));
    List<org.apache.parquet.format.Encoding> encodings = new ArrayList<>(3);
    encodings.add(org.apache.parquet.format.Encoding.RLE);
    // PLAIN covers the dictionary page body and any fallback data pages; it is
    // listed whenever a dictionary page exists because its entries are PLAIN.
    encodings.add(org.apache.parquet.format.Encoding.PLAIN);
    if (dictionaryEncoded) {
      encodings.add(org.apache.parquet.format.Encoding.RLE_DICTIONARY);
    }
    metadata.setEncodings(encodings);
    metadata.setPath_in_schema(Arrays.asList(descriptor.path()));
    metadata.setCodec(WriterFileSchema.convertCompressionCodec(retainedCompression ? compressionCodec : CompressionCodec.UNCOMPRESSED));
    metadata.setNum_values(column.numValues());
    metadata.setTotal_uncompressed_size(uncompressed);
    metadata.setTotal_compressed_size(compressed);
    metadata.setData_page_offset(dataPageOffset);
    if (dictionaryOffset >= 0) {
      metadata.setDictionary_page_offset(dictionaryOffset);
    }
    metadata.setStatistics(column.statistics().toParquet());
    ColumnChunk chunk = new ColumnChunk();
    chunk.setFile_offset(start);
    chunk.setMeta_data(metadata);
    return chunk;
  }

  /**
   * Helper class to return page size information after writing a data page.
   */
  private static class PageInfo {
    final int uncompressed_page_size;
    final int compressed_page_size;
    final int header_size;
    final long total_compressed_size;
    final long total_uncompressed_size;
    final CompressionCodec effectiveCodec;

    PageInfo(int uncompressed_page_size, int compressed_page_size, int header_size,
             CompressionCodec effectiveCodec) {
      this.uncompressed_page_size = uncompressed_page_size;
      this.compressed_page_size = compressed_page_size;
      this.header_size = header_size;
      this.total_compressed_size = (long) header_size + compressed_page_size;
      this.total_uncompressed_size = (long) header_size + uncompressed_page_size;
      this.effectiveCodec = effectiveCodec;
    }
  }

  private PageInfo writeDataPage(WriterPage page) throws IOException {
    byte[] values = page.values();
    byte[] stored = values;
    CompressionCodec effective = CompressionCodec.UNCOMPRESSED;
    if (compressionCodec != CompressionCodec.UNCOMPRESSED && minCompressionRatio > 0
        && values.length != 0) {
      byte[] candidate = compressor.compress(values);
      if ((double) candidate.length / values.length < minCompressionRatio) {
        stored = candidate;
        effective = compressionCodec;
      }
    }
    int levelBytes = Math.toIntExact((long) page.repetitions().length + page.definitions().length);
    int uncompressedSize = Math.toIntExact((long) levelBytes + values.length);
    int compressedSize = Math.toIntExact((long) levelBytes + stored.length);
    DataPageHeaderV2 data = new DataPageHeaderV2();
    data.setNum_values(page.numValues());
    data.setNum_nulls(page.numNulls());
    data.setNum_rows(page.numRows());
    data.setEncoding(WriterFileSchema.convertEncoding(page.encoding()));
    data.setDefinition_levels_byte_length(page.definitions().length);
    data.setRepetition_levels_byte_length(page.repetitions().length);
    data.setIs_compressed(effective != CompressionCodec.UNCOMPRESSED);
    data.setStatistics(page.statistics());
    PageHeader header = new PageHeader();
    header.setType(PageType.DATA_PAGE_V2);
    header.setUncompressed_page_size(uncompressedSize);
    header.setCompressed_page_size(compressedSize);
    header.setData_page_header_v2(data);
    ByteArrayOutputStream encodedHeader = new ByteArrayOutputStream();
    try {
      header.write(new TCompactProtocol(new TIOStreamTransport(encodedHeader)));
    } catch (TException failure) {
      throw new IOException("Failed to write page header", failure);
    }
    byte[] headerBytes = encodedHeader.toByteArray();
    outputStream.write(headerBytes);
    outputStream.write(page.repetitions());
    outputStream.write(page.definitions());
    outputStream.write(stored);
    currentPosition = Math.addExact(currentPosition, (long) headerBytes.length + compressedSize);
    return new PageInfo(uncompressedSize, compressedSize, headerBytes.length, effective);
  }

  /**
   * Writes the column chunk's dictionary page: PLAIN-encoded entries as the body,
   * compressed with the chunk codec. The dictionary page has no per-page
   * {@code is_compressed} flag, so when a codec is configured the body is always
   * compressed and the chunk metadata reports that codec.
   */
  private PageInfo writeDictionaryPage(WriterDictionary dictionary) throws IOException {
    byte[] body = dictionary.pageBody();
    byte[] stored = body;
    CompressionCodec effective = CompressionCodec.UNCOMPRESSED;
    if (compressor != null) {
      stored = compressor.compress(body);
      effective = compressionCodec;
    }
    org.apache.parquet.format.DictionaryPageHeader data =
            new org.apache.parquet.format.DictionaryPageHeader(dictionary.size(),
                    org.apache.parquet.format.Encoding.PLAIN);
    PageHeader header = new PageHeader();
    header.setType(PageType.DICTIONARY_PAGE);
    header.setUncompressed_page_size(body.length);
    header.setCompressed_page_size(stored.length);
    header.setDictionary_page_header(data);
    ByteArrayOutputStream encodedHeader = new ByteArrayOutputStream();
    try {
      header.write(new TCompactProtocol(new TIOStreamTransport(encodedHeader)));
    } catch (TException failure) {
      throw new IOException("Failed to write dictionary page header", failure);
    }
    byte[] headerBytes = encodedHeader.toByteArray();
    outputStream.write(headerBytes);
    outputStream.write(stored);
    currentPosition = Math.addExact(currentPosition, (long) headerBytes.length + stored.length);
    return new PageInfo(body.length, stored.length, headerBytes.length, effective);
  }

  /**
   * Finalize and close the file, writing the footer metadata.
   */
  @Override
  public void close() throws IOException {
    if (state == State.CLOSED || state == State.FAILED) {
      return;
    }

    try {
      // If no rows were added, start the file anyway to write a valid empty parquet file
      if (outputStream == null) {
        start();
      }

      // Flush any remaining rows
      flushRowGroup();

      // Build file metadata
      FileMetaData fileMetaData = new FileMetaData();
      fileMetaData.setVersion(1);
      fileMetaData.setNum_rows(totalRowCount);

      // Build schema
      List<SchemaElement> schema = new ArrayList<>();
      schema.add(WriterFileSchema.buildFileSchema(this.schema));
      schema.addAll(WriterFileSchema.buildColumnSchemas(this.schema));
      fileMetaData.setSchema(schema);

      fileMetaData.setRow_groups(rowGroups);
      List<ColumnOrder> orders = new ArrayList<>(this.schema.getNumColumns());
      for (int i = 0; i < this.schema.getNumColumns(); i++) {
        orders.add(ColumnOrder.TYPE_ORDER(new TypeDefinedOrder()));
      }
      fileMetaData.setColumn_orders(orders);
      fileMetaData.setCreated_by("java-parquet-rs ParquetFileWriter");

      // Serialize metadata using Thrift
      ByteArrayOutputStream metadataBuffer = new ByteArrayOutputStream();
      try {
        fileMetaData.write(new TCompactProtocol(new TIOStreamTransport(metadataBuffer)));
      } catch (TException e) {
        throw new IOException("Failed to write file metadata", e);
      }

      byte[] metadataBytes = metadataBuffer.toByteArray();

      // Write metadata
      outputStream.write(metadataBytes);

      // Write metadata length as 4-byte little-endian integer
      ByteBuffer lengthBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      lengthBuffer.putInt(metadataBytes.length);
      outputStream.write(lengthBuffer.array());

      // Write magic number at the end
      outputStream.write(PARQUET_MAGIC);

      // A footer is not a commit until the sink has closed successfully.
      OutputStream completedSink = outputStream;
      outputStream = null; // Close is attempted exactly once, even if it fails.
      completedSink.close();
      Files.move(temporaryPath, filePath, StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      temporaryPath = null;
      state = State.CLOSED;
    } catch (IOException | RuntimeException | Error failure) {
      fail(failure);
      throw failure;
    }
  }
}
