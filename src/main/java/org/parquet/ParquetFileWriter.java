package org.parquet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.CompressionCodec;
import org.parquet.model.LogicalColumnDescriptor;
import org.parquet.model.MapMetadata;
import org.parquet.model.ParquetException;
import org.parquet.model.RowColumnGroup;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.Type;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

/**
 * A simple Parquet file writer that implements the ParquetWriter interface.
 * This writer supports basic data types and PLAIN encoding with optional compression.
 */
public class ParquetFileWriter implements ParquetWriter {
  private static final byte[] PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
  private static final int DEFAULT_PAGE_SIZE = 1024 * 1024; // 1MB
  private static final int DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024; // 128MB

  private final Path filePath;
  private final SchemaDescriptor schema;
  private final org.parquet.model.CompressionCodec compressionCodec;
  private final int pageSize;
  private final int rowGroupSize;

  private OutputStream outputStream;
  private long currentPosition;
  private final List<RowGroup> rowGroups;
  private final List<RowColumnGroup> currentRowGroupRows;
  private int totalRowCount;
  private boolean closed;
  private final Compressor compressor;

  /**
   * Create a new ParquetFileWriter with default settings.
   *
   * @param filePath Path to the output Parquet file
   * @param schema   Schema descriptor for the file
   */
  public ParquetFileWriter(Path filePath, SchemaDescriptor schema) {
    this(filePath, schema, org.parquet.model.CompressionCodec.UNCOMPRESSED, DEFAULT_PAGE_SIZE,
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
                           org.parquet.model.CompressionCodec compressionCodec, int pageSize,
                           int rowGroupSize) {
    this.filePath = filePath;
    this.schema = schema;
    this.compressionCodec = compressionCodec;
    this.pageSize = pageSize;
    this.rowGroupSize = rowGroupSize;
    this.rowGroups = new ArrayList<>();
    this.currentRowGroupRows = new ArrayList<>();
    this.currentPosition = 0;
    this.totalRowCount = 0;
    this.closed = false;
    this.compressor = Compressor.create(compressionCodec);
  }

  /**
   * Initialize the writer and write the file header.
   */
  public void start() throws IOException {
    if (outputStream != null) {
      throw new IllegalStateException("Writer already started");
    }

    outputStream = Files.newOutputStream(filePath,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE);

    // Write magic number at the beginning
    outputStream.write(PARQUET_MAGIC);
    currentPosition += PARQUET_MAGIC.length;
  }

  @Override
  public void addRow(RowColumnGroup row) {
    if (closed) {
      throw new IllegalStateException("Writer is closed");
    }

    if (outputStream == null) {
      try {
        start();
      } catch (IOException e) {
        throw new ParquetException("Failed to start writer", e);
      }
    }

    // Validate schema matches
    if (!row.getSchema().name().equals(schema.name())) {
      throw new IllegalArgumentException("Row schema does not match writer schema");
    }

    currentRowGroupRows.add(row);

    // Check if we should flush the row group (simple size check based on row count)
    // In a production implementation, this should track actual byte size
    if (currentRowGroupRows.size() >= 1000) {
      try {
        flushRowGroup();
      } catch (IOException e) {
        throw new ParquetException("Failed to flush row group", e);
      }
    }
  }

  /**
   * Flush the current row group to disk.
   */
  private void flushRowGroup() throws IOException {
    if (currentRowGroupRows.isEmpty()) {
      return;
    }

    RowGroup rowGroup = new RowGroup();
    List<ColumnChunk> columnChunks = new ArrayList<>();

    long rowGroupStartPos = currentPosition;

    // Check if we have logical columns (for maps, structs, etc.)
    if (schema.hasLogicalColumns()) {
      // Write logical columns, which may map to multiple physical columns
      for (int logicalIndex = 0; logicalIndex < schema.getNumLogicalColumns(); logicalIndex++) {
        LogicalColumnDescriptor logicalCol = schema.getLogicalColumn(logicalIndex);

        if (logicalCol.isPrimitive()) {
          // Write primitive column normally
          ColumnDescriptor physicalCol = logicalCol.getPhysicalDescriptor();
          int physicalIndex = schema.columns().indexOf(physicalCol);
          ColumnChunk columnChunk =
              writeColumnChunk(physicalCol, physicalIndex, currentRowGroupRows);
          columnChunks.add(columnChunk);
        } else if (logicalCol.isMap()) {
          // Write map columns (produces 2 physical column chunks)
          List<ColumnChunk> mapChunks = writeMapColumnChunks(logicalCol, currentRowGroupRows);
          columnChunks.addAll(mapChunks);
        }
      }
    } else {
      // Write each physical column chunk (legacy path for simple schemas)
      for (int colIndex = 0; colIndex < schema.getNumColumns(); colIndex++) {
        ColumnDescriptor columnDesc = schema.getColumn(colIndex);
        ColumnChunk columnChunk = writeColumnChunk(columnDesc, colIndex, currentRowGroupRows);
        columnChunks.add(columnChunk);
      }
    }

    rowGroup.setColumns(columnChunks);
    rowGroup.setTotal_byte_size(currentPosition - rowGroupStartPos);
    rowGroup.setNum_rows(currentRowGroupRows.size());

    rowGroups.add(rowGroup);
    totalRowCount += currentRowGroupRows.size();
    currentRowGroupRows.clear();
  }

  /**
   * Write a single column chunk for a row group.
   */
  private ColumnChunk writeColumnChunk(ColumnDescriptor columnDesc,
                                       int columnIndex,
                                       List<RowColumnGroup> rows) throws IOException {
    long columnChunkStartPos = currentPosition;

    // Collect all values for this column
    List<Object> values = new ArrayList<>();
    List<Integer> definitionLevels = new ArrayList<>();
    List<Integer> repetitionLevels = new ArrayList<>();

    for (RowColumnGroup row : rows) {
      Object value = null;

      // Find the value for this physical column
      if (columnIndex < row.getColumnCount()) {
        // Try to get by index first (for simple schemas)
        try {
          value = row.getColumnValue(columnIndex);
        } catch (Exception e) {
          // Fallback to finding by path
          value = findValueByPath(row, columnDesc.path());
        }
      }

      values.add(value);

      // Definition levels: 0 for null, max for non-null
      definitionLevels.add(value == null ? 0 : columnDesc.maxDefinitionLevel());

      // Repetition levels: 0 for non-repeated
      repetitionLevels.add(0);
    }

    // Calculate statistics
    ColumnStatistics stats = calculateStatistics(values, columnDesc.physicalType());

    // Write the data page
    PageInfo pageInfo = writeDataPage(
        columnDesc,
        values,
        definitionLevels,
        repetitionLevels
    );

    // Create column metadata
    ColumnMetaData columnMetaData = new ColumnMetaData();
    columnMetaData.setType(convertType(columnDesc.physicalType()));
    columnMetaData.setEncodings(Arrays.asList(
        org.apache.parquet.format.Encoding.RLE,  // For levels
        org.apache.parquet.format.Encoding.PLAIN  // For values
    ));
    columnMetaData.setPath_in_schema(Arrays.asList(columnDesc.path()));
    columnMetaData.setCodec(convertCompressionCodec(compressionCodec));
    columnMetaData.setNum_values(values.size());
    // Note: total sizes should NOT include page headers, only the page data itself
    columnMetaData.setTotal_uncompressed_size(pageInfo.uncompressed_page_size);
    columnMetaData.setTotal_compressed_size(pageInfo.compressed_page_size);
    columnMetaData.setData_page_offset(columnChunkStartPos);

    // Add statistics
    Statistics parquetStats = toParquetStatistics(stats, columnDesc.physicalType());
    if (parquetStats != null) {
      columnMetaData.setStatistics(parquetStats);
    }

    // Create column chunk
    ColumnChunk columnChunk = new ColumnChunk();
    columnChunk.setFile_offset(columnChunkStartPos);
    columnChunk.setMeta_data(columnMetaData);

    return columnChunk;
  }

  /**
   * Write map column chunks (key and value columns) for a logical MAP column.
   */
  private List<ColumnChunk> writeMapColumnChunks(LogicalColumnDescriptor logicalCol,
                                                 List<RowColumnGroup> rows) throws IOException {
    MapMetadata mapMeta = logicalCol.getMapMetadata();
    MapColumnWriter mapWriter = new MapColumnWriter(mapMeta.keyType(), mapMeta.valueType());

    // Extract map values from rows
    List<Map<?, ?>> maps = new ArrayList<>();
    for (RowColumnGroup row : rows) {
      Object value = null;
      try {
        value = row.getColumnValue(logicalCol.getName());
      } catch (Exception e) {
        // Column not found or error, treat as null
      }

      if (value instanceof Map) {
        maps.add((Map<?, ?>) value);
      } else {
        maps.add(null);  // NULL map
      }
    }

    // Extract keys, values, and levels using MapColumnWriter
    List<Object> keys = mapWriter.extractKeys(maps);
    List<Object> values = mapWriter.extractValues(maps);
    List<Integer> repLevels = mapWriter.calculateRepetitionLevels(maps);
    List<Integer> keyDefLevels = mapWriter.calculateKeyDefinitionLevels(maps);
    List<Integer> valueDefLevels = mapWriter.calculateValueDefinitionLevels(maps);

    // Number of values for each column is the total entry count
    int numValues = mapWriter.countTotalEntries(maps);

    List<ColumnChunk> chunks = new ArrayList<>();

    // Write key column chunk
    chunks.add(writeColumnChunkWithLevels(
        mapMeta.keyDescriptor(),
        keys,
        keyDefLevels,
        repLevels,
        numValues
    ));

    // Write value column chunk
    chunks.add(writeColumnChunkWithLevels(
        mapMeta.valueDescriptor(),
        values,
        valueDefLevels,
        repLevels,
        numValues
    ));

    return chunks;
  }

  /**
   * Write a column chunk with pre-calculated definition and repetition levels.
   * This is used for map columns where levels are calculated by MapColumnWriter.
   */
  private ColumnChunk writeColumnChunkWithLevels(
      ColumnDescriptor columnDesc,
      List<Object> values,
      List<Integer> definitionLevels,
      List<Integer> repetitionLevels,
      int numValues) throws IOException {

    long columnChunkStartPos = currentPosition;

    // Calculate statistics
    ColumnStatistics stats = calculateStatistics(values, columnDesc.physicalType());

    // Write the data page with provided levels
    PageInfo pageInfo = writeDataPageWithLevels(
        columnDesc,
        values,
        definitionLevels,
        repetitionLevels,
        numValues
    );

    // Create column metadata
    ColumnMetaData columnMetaData = new ColumnMetaData();
    columnMetaData.setType(convertType(columnDesc.physicalType()));
    columnMetaData.setEncodings(Arrays.asList(
        org.apache.parquet.format.Encoding.RLE,  // For levels
        org.apache.parquet.format.Encoding.PLAIN  // For values
    ));
    columnMetaData.setPath_in_schema(Arrays.asList(columnDesc.path()));
    columnMetaData.setCodec(convertCompressionCodec(compressionCodec));
    columnMetaData.setNum_values(numValues);
    columnMetaData.setTotal_uncompressed_size(pageInfo.uncompressed_page_size);
    columnMetaData.setTotal_compressed_size(pageInfo.compressed_page_size);
    columnMetaData.setData_page_offset(columnChunkStartPos);

    // Add statistics
    Statistics parquetStats = toParquetStatistics(stats, columnDesc.physicalType());
    if (parquetStats != null) {
      columnMetaData.setStatistics(parquetStats);
    }

    // Create column chunk
    ColumnChunk columnChunk = new ColumnChunk();
    columnChunk.setFile_offset(columnChunkStartPos);
    columnChunk.setMeta_data(columnMetaData);

    return columnChunk;
  }

  /**
   * Find a value by column path in a row.
   */
  private Object findValueByPath(RowColumnGroup row, String[] path) {
    String pathString = String.join(".", path);
    try {
      return row.getColumnValue(pathString);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Calculate statistics for a list of values.
   */
  private ColumnStatistics calculateStatistics(List<Object> values, org.parquet.model.Type type) {
    ColumnStatistics stats = new ColumnStatistics();
    Set<Object> distinctValues = new HashSet<>();

    for (Object value : values) {
      if (value == null) {
        stats.nullCount++;
        continue;
      }

      distinctValues.add(value);

      // Update min/max based on type
      if (stats.min == null) {
        stats.min = value;
        stats.max = value;
      } else {
        stats.min = minValue(stats.min, value, type);
        stats.max = maxValue(stats.max, value, type);
      }
    }

    stats.distinctCount = distinctValues.size();
    return stats;
  }

  /**
   * Compare two values and return the minimum based on type.
   */
  @SuppressWarnings("unchecked")
  private Object minValue(Object a, Object b, org.parquet.model.Type type) {
    if (a == null) return b;
    if (b == null) return a;

    return switch (type) {
      case BOOLEAN -> ((Boolean) a && !(Boolean) b) ? a : b;
      case INT32 -> ((Number) a).intValue() < ((Number) b).intValue() ? a : b;
      case INT64 -> ((Number) a).longValue() < ((Number) b).longValue() ? a : b;
      case FLOAT -> ((Number) a).floatValue() < ((Number) b).floatValue() ? a : b;
      case DOUBLE -> ((Number) a).doubleValue() < ((Number) b).doubleValue() ? a : b;
      case BYTE_ARRAY -> {
        byte[] bytesA = getByteArray(a);
        byte[] bytesB = getByteArray(b);
        yield compareByteArrays(bytesA, bytesB) < 0 ? a : b;
      }
      case FIXED_LEN_BYTE_ARRAY -> {
        byte[] bytesA = getByteArray(a);
        byte[] bytesB = getByteArray(b);
        yield compareByteArrays(bytesA, bytesB) < 0 ? a : b;
      }
      default -> a;
    };
  }

  /**
   * Compare two values and return the maximum based on type.
   */
  @SuppressWarnings("unchecked")
  private Object maxValue(Object a, Object b, org.parquet.model.Type type) {
    if (a == null) return b;
    if (b == null) return a;

    return switch (type) {
      case BOOLEAN -> ((Boolean) a || (Boolean) b) ? a : b;
      case INT32 -> ((Number) a).intValue() > ((Number) b).intValue() ? a : b;
      case INT64 -> ((Number) a).longValue() > ((Number) b).longValue() ? a : b;
      case FLOAT -> ((Number) a).floatValue() > ((Number) b).floatValue() ? a : b;
      case DOUBLE -> ((Number) a).doubleValue() > ((Number) b).doubleValue() ? a : b;
      case BYTE_ARRAY -> {
        byte[] bytesA = getByteArray(a);
        byte[] bytesB = getByteArray(b);
        yield compareByteArrays(bytesA, bytesB) > 0 ? a : b;
      }
      case FIXED_LEN_BYTE_ARRAY -> {
        byte[] bytesA = getByteArray(a);
        byte[] bytesB = getByteArray(b);
        yield compareByteArrays(bytesA, bytesB) > 0 ? a : b;
      }
      default -> a;
    };
  }

  /**
   * Compare two byte arrays lexicographically.
   */
  private int compareByteArrays(byte[] a, byte[] b) {
    int minLength = Math.min(a.length, b.length);
    for (int i = 0; i < minLength; i++) {
      int cmp = Byte.compareUnsigned(a[i], b[i]);
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(a.length, b.length);
  }

  /**
   * Convert column statistics to Parquet Statistics format.
   */
  private Statistics toParquetStatistics(ColumnStatistics stats, org.parquet.model.Type type)
      throws IOException {
    if (stats.min == null && stats.max == null) {
      return null;
    }

    Statistics parquetStats = new Statistics();
    parquetStats.setNull_count(stats.nullCount);
    parquetStats.setDistinct_count(stats.distinctCount);

    if (stats.min != null) {
      parquetStats.setMin(encodeStatValue(stats.min, type));
      parquetStats.setMin_value(encodeStatValue(stats.min, type));
    }

    if (stats.max != null) {
      parquetStats.setMax(encodeStatValue(stats.max, type));
      parquetStats.setMax_value(encodeStatValue(stats.max, type));
    }

    return parquetStats;
  }

  /**
   * Encode a statistic value to byte array for Parquet format.
   */
  private byte[] encodeStatValue(Object value, org.parquet.model.Type type) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    switch (type) {
      case BOOLEAN:
        buffer.write(((Boolean) value) ? 1 : 0);
        break;

      case INT32:
        writeInt32(buffer, ((Number) value).intValue());
        break;

      case INT64:
        writeInt64(buffer, ((Number) value).longValue());
        break;

      case FLOAT:
        writeFloat(buffer, ((Number) value).floatValue());
        break;

      case DOUBLE:
        writeDouble(buffer, ((Number) value).doubleValue());
        break;

      case BYTE_ARRAY:
      case FIXED_LEN_BYTE_ARRAY:
        return getByteArray(value);

      default:
        throw new UnsupportedOperationException("Unsupported type for statistics: " + type);
    }

    return buffer.toByteArray();
  }

  /**
   * Helper class to track column statistics.
   */
  private static class ColumnStatistics {
    Object min;
    Object max;
    long nullCount;
    long distinctCount;

    ColumnStatistics() {
      this.nullCount = 0;
      this.distinctCount = 0;
    }
  }

  /**
   * Helper class to return page information.
   */
  private static class PageInfo {
    final int uncompressed_page_size;
    final int compressed_page_size;
    final int header_size;
    final int total_compressed_size;  // header + compressed data
    final int total_uncompressed_size;  // header + uncompressed data

    PageInfo(int uncompressed_page_size, int compressed_page_size, int header_size) {
      this.uncompressed_page_size = uncompressed_page_size;
      this.compressed_page_size = compressed_page_size;
      this.header_size = header_size;
      this.total_compressed_size = header_size + compressed_page_size;
      this.total_uncompressed_size = header_size + uncompressed_page_size;
    }
  }

  /**
   * Write a data page and return its size information.
   */
  private PageInfo writeDataPage(ColumnDescriptor columnDesc,
                                 List<Object> values,
                                 List<Integer> definitionLevels,
                                 List<Integer> repetitionLevels) throws IOException {
    return writeDataPageWithLevels(columnDesc, values, definitionLevels, repetitionLevels,
        values.size());
  }

  /**
   * Write a data page with explicit numValues (for map columns where numValues != values.size()).
   */
  private PageInfo writeDataPageWithLevels(ColumnDescriptor columnDesc,
                                           List<Object> values,
                                           List<Integer> definitionLevels,
                                           List<Integer> repetitionLevels,
                                           int numValues) throws IOException {

    ByteArrayOutputStream pageBuffer = new ByteArrayOutputStream();

    // Write repetition levels (if needed)
    if (columnDesc.maxRepetitionLevel() > 0) {
      byte[] repetitionLevelData = encodeRLE(repetitionLevels, columnDesc.maxRepetitionLevel());
      pageBuffer.write(repetitionLevelData);
    }

    // Write definition levels (if needed)
    if (columnDesc.maxDefinitionLevel() > 0) {
      byte[] definitionLevelData = encodeRLE(definitionLevels, columnDesc.maxDefinitionLevel());
      pageBuffer.write(definitionLevelData);
    }

    // Write values using PLAIN encoding
    byte[] valueData = encodeValuesPlain(values, columnDesc.physicalType());
    pageBuffer.write(valueData);

    byte[] uncompressedPageData = pageBuffer.toByteArray();
    byte[] compressedPageData = compress(uncompressedPageData);

    // Create data page header
    DataPageHeader dataPageHeader = new DataPageHeader();
    dataPageHeader.setNum_values(numValues);
    dataPageHeader.setEncoding(org.apache.parquet.format.Encoding.PLAIN);
    dataPageHeader.setDefinition_level_encoding(org.apache.parquet.format.Encoding.RLE);
    dataPageHeader.setRepetition_level_encoding(org.apache.parquet.format.Encoding.RLE);

    // Write page header
    PageHeader pageHeader = new PageHeader();
    pageHeader.setType(PageType.DATA_PAGE);
    pageHeader.setUncompressed_page_size(uncompressedPageData.length);
    pageHeader.setCompressed_page_size(compressedPageData.length);
    pageHeader.setData_page_header(dataPageHeader);

    // Serialize page header using Thrift
    ByteArrayOutputStream headerBuffer = new ByteArrayOutputStream();
    try {
      pageHeader.write(new TCompactProtocol(new TIOStreamTransport(headerBuffer)));
    } catch (TException e) {
      throw new IOException("Failed to write page header", e);
    }

    byte[] headerBytes = headerBuffer.toByteArray();

    // Write to output stream
    outputStream.write(headerBytes);
    outputStream.write(compressedPageData);

    int totalBytesWritten = headerBytes.length + compressedPageData.length;
    currentPosition += totalBytesWritten;

    return new PageInfo(uncompressedPageData.length, compressedPageData.length, headerBytes.length);
  }

  /**
   * Encode definition/repetition levels using proper RLE/Bit-Packing Hybrid encoding.
   */
  private byte[] encodeRLE(List<Integer> levels, int maxLevel) throws IOException {
    // Calculate bit width needed for the max level value
    int bitWidth = RleEncoder.bitWidth(maxLevel);

    // Use the RleEncoder for proper encoding
    RleEncoder encoder = new RleEncoder(bitWidth);
    return encoder.encode(levels);
  }

  /**
   * Encode values using PLAIN encoding.
   */
  private byte[] encodeValuesPlain(List<Object> values, org.parquet.model.Type type)
      throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    for (Object value : values) {
      if (value == null) {
        // Nulls are handled by definition levels, skip writing value
        continue;
      }

      switch (type) {
        case BOOLEAN:
          // Booleans are bit-packed, but for simplicity we'll write bytes
          buffer.write(((Boolean) value) ? 1 : 0);
          break;

        case INT32:
          writeInt32(buffer, ((Number) value).intValue());
          break;

        case INT64:
          writeInt64(buffer, ((Number) value).longValue());
          break;

        case FLOAT:
          writeFloat(buffer, ((Number) value).floatValue());
          break;

        case DOUBLE:
          writeDouble(buffer, ((Number) value).doubleValue());
          break;

        case BYTE_ARRAY:
          writeByteArray(buffer, getByteArray(value));
          break;

        case FIXED_LEN_BYTE_ARRAY:
          writeFixedByteArray(buffer, getByteArray(value));
          break;

        default:
          throw new UnsupportedOperationException("Unsupported type: " + type);
      }
    }

    return buffer.toByteArray();
  }

  private byte[] getByteArray(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    } else if (value instanceof String) {
      return ((String) value).getBytes(StandardCharsets.UTF_8);
    } else if (value instanceof ByteBuffer bb) {
      byte[] result = new byte[bb.remaining()];
      bb.duplicate().get(result);
      return result;
    } else {
      return value.toString().getBytes(StandardCharsets.UTF_8);
    }
  }

  private void writeInt32(ByteArrayOutputStream buffer, int value) {
    ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(value);
    buffer.write(bb.array(), 0, 4);
  }

  private void writeInt64(ByteArrayOutputStream buffer, long value) {
    ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    bb.putLong(value);
    buffer.write(bb.array(), 0, 8);
  }

  private void writeFloat(ByteArrayOutputStream buffer, float value) {
    ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    bb.putFloat(value);
    buffer.write(bb.array(), 0, 4);
  }

  private void writeDouble(ByteArrayOutputStream buffer, double value) {
    ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    bb.putDouble(value);
    buffer.write(bb.array(), 0, 8);
  }

  private void writeByteArray(ByteArrayOutputStream buffer, byte[] value) throws IOException {
    // Write length as 4-byte little-endian integer
    writeInt32(buffer, value.length);
    buffer.write(value);
  }

  private void writeFixedByteArray(ByteArrayOutputStream buffer, byte[] value) throws IOException {
    buffer.write(value);
  }

  /**
   * Compress data using the configured compression codec.
   */
  private byte[] compress(byte[] data) throws IOException {
    return compressor.compress(data);
  }

  /**
   * Convert internal Type to Parquet format Type.
   */
  private org.apache.parquet.format.Type convertType(org.parquet.model.Type type) {
    return switch (type) {
      case BOOLEAN -> org.apache.parquet.format.Type.BOOLEAN;
      case INT32 -> org.apache.parquet.format.Type.INT32;
      case INT64 -> org.apache.parquet.format.Type.INT64;
      case INT96 -> org.apache.parquet.format.Type.INT96;
      case FLOAT -> org.apache.parquet.format.Type.FLOAT;
      case DOUBLE -> org.apache.parquet.format.Type.DOUBLE;
      case BYTE_ARRAY -> org.apache.parquet.format.Type.BYTE_ARRAY;
      case FIXED_LEN_BYTE_ARRAY -> org.apache.parquet.format.Type.FIXED_LEN_BYTE_ARRAY;
    };
  }

  /**
   * Convert internal CompressionCodec to Parquet format CompressionCodec.
   */
  private org.apache.parquet.format.CompressionCodec convertCompressionCodec(
      CompressionCodec codec) {
    return switch (codec) {
      case UNCOMPRESSED -> org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
      case SNAPPY -> org.apache.parquet.format.CompressionCodec.SNAPPY;
      case GZIP -> org.apache.parquet.format.CompressionCodec.GZIP;
      case LZO -> org.apache.parquet.format.CompressionCodec.LZO;
      case BROTLI -> org.apache.parquet.format.CompressionCodec.BROTLI;
      case LZ4 -> org.apache.parquet.format.CompressionCodec.LZ4;
      case ZSTD -> org.apache.parquet.format.CompressionCodec.ZSTD;
      case LZ4_RAW -> org.apache.parquet.format.CompressionCodec.LZ4_RAW;
    };
  }

  /**
   * Build the file schema from the schema descriptor.
   */
  private SchemaElement buildFileSchema() {
    SchemaElement root = new SchemaElement();
    root.setName(schema.name());

    // Count children: logical columns if present, otherwise physical columns
    int numChildren = schema.hasLogicalColumns()
        ? schema.getNumLogicalColumns()
        : schema.getNumColumns();
    root.setNum_children(numChildren);

    // Root has no type
    return root;
  }

  /**
   * Build schema elements for columns, supporting hierarchical MAP structures.
   */
  private List<SchemaElement> buildColumnSchemas() {
    List<SchemaElement> elements = new ArrayList<>();

    if (schema.hasLogicalColumns()) {
      // Build hierarchical schema for logical columns
      for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
        LogicalColumnDescriptor logicalCol = schema.getLogicalColumn(i);

        if (logicalCol.isPrimitive()) {
          // Add primitive column
          elements.add(buildPrimitiveSchemaElement(logicalCol.getPhysicalDescriptor()));
        } else if (logicalCol.isMap()) {
          // Add MAP group with nested key_value group
          elements.addAll(buildMapSchemaElements(logicalCol));
        }
      }
    } else {
      // Legacy: build flat schema from physical columns
      for (int i = 0; i < schema.getNumColumns(); i++) {
        elements.add(buildPrimitiveSchemaElement(schema.getColumn(i)));
      }
    }

    return elements;
  }

  /**
   * Build a schema element for a primitive column.
   */
  private SchemaElement buildPrimitiveSchemaElement(ColumnDescriptor col) {
    SchemaElement element = new SchemaElement();

    // Use the last part of the path as the name
    String[] path = col.path();
    element.setName(path[path.length - 1]);
    element.setType(convertType(col.physicalType()));

    // Set repetition type
    if (col.maxRepetitionLevel() > 0) {
      element.setRepetition_type(FieldRepetitionType.REPEATED);
    } else if (col.maxDefinitionLevel() > 0) {
      element.setRepetition_type(FieldRepetitionType.OPTIONAL);
    } else {
      element.setRepetition_type(FieldRepetitionType.REQUIRED);
    }

    if (col.physicalType() == org.parquet.model.Type.FIXED_LEN_BYTE_ARRAY) {
      element.setType_length(col.typeLength());
    }

    return element;
  }

  /**
   * Build schema elements for a MAP column.
   * Returns 3 elements: map group, key_value group, key, and value.
   */
  private List<SchemaElement> buildMapSchemaElements(LogicalColumnDescriptor logicalCol) {
    List<SchemaElement> elements = new ArrayList<>();
    MapMetadata mapMeta = logicalCol.getMapMetadata();

    // 1. Map group (optional group <name> (MAP))
    SchemaElement mapGroup = new SchemaElement();
    mapGroup.setName(logicalCol.getName());
    mapGroup.setRepetition_type(
        mapMeta.keyDescriptor().maxDefinitionLevel() > 1
            ? FieldRepetitionType.OPTIONAL
            : FieldRepetitionType.REQUIRED
    );
    mapGroup.setConverted_type(ConvertedType.MAP);
    mapGroup.setNum_children(1);  // Contains key_value group
    elements.add(mapGroup);

    // 2. key_value group (repeated group key_value)
    SchemaElement keyValueGroup = new SchemaElement();
    keyValueGroup.setName("key_value");
    keyValueGroup.setRepetition_type(FieldRepetitionType.REPEATED);
    keyValueGroup.setNum_children(2);  // Contains key and value
    elements.add(keyValueGroup);

    // 3. Key element (required <type> key)
    SchemaElement keyElement = new SchemaElement();
    keyElement.setName("key");
    keyElement.setType(convertType(mapMeta.keyType()));
    keyElement.setRepetition_type(FieldRepetitionType.REQUIRED);
    if (mapMeta.keyType() == org.parquet.model.Type.BYTE_ARRAY) {
      keyElement.setConverted_type(ConvertedType.UTF8);
    }
    elements.add(keyElement);

    // 4. Value element (optional/required <type> value)
    SchemaElement valueElement = new SchemaElement();
    valueElement.setName("value");
    valueElement.setType(convertType(mapMeta.valueType()));

    // Value is optional if maxDefLevel indicates it can be null
    int valueMaxDef = mapMeta.valueDescriptor().maxDefinitionLevel();
    int keyMaxDef = mapMeta.keyDescriptor().maxDefinitionLevel();
    boolean valueOptional = valueMaxDef > keyMaxDef;

    valueElement.setRepetition_type(
        valueOptional ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED
    );
    if (mapMeta.valueType() == Type.BYTE_ARRAY) {
      valueElement.setConverted_type(ConvertedType.UTF8);
    }
    elements.add(valueElement);

    return elements;
  }

  /**
   * Finalize and close the file, writing the footer metadata.
   */
  @Override
  public void close() throws IOException {
    if (closed) {
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
      schema.add(buildFileSchema());
      schema.addAll(buildColumnSchemas());
      fileMetaData.setSchema(schema);

      fileMetaData.setRow_groups(rowGroups);
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

    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
      closed = true;
    }
  }
}
