package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.MapMetadata;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

/**
 * Reads Parquet file metadata from the footer.
 *
 * <p>This class provides utilities to read and parse Parquet file metadata stored in the
 * file footer. It handles Thrift deserialization and converts the metadata into our
 * internal representation including schema, row groups, and column statistics.
 */
public class ParquetMetadataReader {

  /**
   * Private constructor to prevent instantiation of this utility class.
   */
  private ParquetMetadataReader() {
    // Utility class
  }

  private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
  private static final int FOOTER_SIZE = 8; // 4 bytes footer length + 4 bytes magic

  /**
   * Reads metadata from a Parquet file.
   *
   * <p>This method reads the Parquet file footer, validates the magic number,
   * and deserializes the metadata using Thrift protocol.
   *
   * @param reader the ChunkReader for reading file contents
   * @return the parsed ParquetMetadata containing schema and row group information
   * @throws IOException if an I/O error occurs while reading the file
   * @throws ParquetException if the file is not a valid Parquet file or metadata is corrupt
   */
  public static ParquetMetadata readMetadata(ChunkReader reader) throws IOException {
    long fileLen = reader.length();
    if (fileLen < FOOTER_SIZE + 4) {
      throw new ParquetException("File too small to be a valid Parquet file");
    }

    // Read the footer (last 8 bytes)
    ByteBuffer footer = reader.readBytes(fileLen - FOOTER_SIZE, FOOTER_SIZE);
    footer.order(ByteOrder.LITTLE_ENDIAN);

    // Check magic number
    byte[] magic = new byte[4];
    footer.position(4);
    footer.get(magic);
    if (!java.util.Arrays.equals(magic, MAGIC)) {
      throw new ParquetException("Not a valid Parquet file - invalid magic number");
    }

    // Read footer length
    footer.position(0);
    int footerLen = footer.getInt();
    if (footerLen <= 0 || footerLen > fileLen - FOOTER_SIZE) {
      throw new ParquetException("Invalid footer length: " + footerLen);
    }

    // Read the file metadata
    long metadataStart = fileLen - FOOTER_SIZE - footerLen;
    ByteBuffer metadataBytes = reader.readBytes(metadataStart, footerLen);

    return parseMetadata(metadataBytes);
  }

  /**
   * Parses metadata from Thrift-encoded bytes.
   *
   * <p>Uses Apache Thrift's TCompactProtocol to deserialize the FileMetaData structure
   * from the byte buffer.
   *
   * @param buffer the ByteBuffer containing Thrift-encoded metadata
   * @return the parsed ParquetMetadata
   * @throws IOException if an I/O error occurs during parsing
   * @throws ParquetException if the Thrift deserialization fails
   */
  private static ParquetMetadata parseMetadata(ByteBuffer buffer) throws IOException {
    try {
      // Create Thrift protocol to deserialize
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      TIOStreamTransport transport = new TIOStreamTransport(new ByteArrayInputStream(bytes));
      TCompactProtocol protocol = new TCompactProtocol(transport);

      // Read FileMetaData
      FileMetaData thriftMetadata = new FileMetaData();
      thriftMetadata.read(protocol);

      return convertFromThrift(thriftMetadata);
    } catch (TException e) {
      throw new ParquetException("Failed to parse Parquet metadata", e);
    }
  }

  /**
   * Converts Thrift metadata to our internal metadata classes.
   *
   * <p>This method performs the following conversions:
   * <ul>
   *   <li>Schema elements to ColumnDescriptors and LogicalColumnDescriptors</li>
   *   <li>Key-value metadata to a Map</li>
   *   <li>Row groups and column chunks with statistics</li>
   * </ul>
   *
   * @param thriftMetadata the FileMetaData from Thrift deserialization
   * @return the converted ParquetMetadata with our internal representation
   */
  private static ParquetMetadata convertFromThrift(FileMetaData thriftMetadata) {
    // Convert schema
    SchemaElement rootSchema = thriftMetadata.getSchema().get(0);

    // Build the annotated schema tree; physical leaf descriptors (with their
    // PrimitiveLogicalType annotation) and logical columns derive from it centrally.
    List<SchemaElement> schemaElements = thriftMetadata.getSchema();
    SchemaDescriptor schema = SchemaDescriptor.fromSchemaElements(
        rootSchema.getName(), schemaElements);
    List<ColumnDescriptor> columns = schema.columns();
    List<LogicalColumnDescriptor> logicalColumns = schema.logicalColumns();

    // Convert key-value metadata
    Map<String, String> kvMetadata = new HashMap<>();
    if (thriftMetadata.isSetKey_value_metadata()) {
      for (KeyValue kv : thriftMetadata.getKey_value_metadata()) {
        kvMetadata.put(kv.getKey(), kv.getValue());
      }
    }

    ParquetMetadata.FileMetadata fileMetadata = new ParquetMetadata.FileMetadata(
        thriftMetadata.getVersion(),
        schema,
        thriftMetadata.getNum_rows(),
        kvMetadata
    );

    // Convert row groups
    List<ParquetMetadata.RowGroupMetadata> rowGroups = new ArrayList<>();
    for (RowGroup rg : thriftMetadata.getRow_groups()) {
      List<ParquetMetadata.ColumnChunkMetadata> columnChunks = new ArrayList<>();

      for (int i = 0; i < rg.getColumns().size(); i++) {
        ColumnChunk cc = rg.getColumns().get(i);
        ColumnMetaData meta = cc.getMeta_data();

        // Get column path
        String[] path = meta.getPath_in_schema().toArray(new String[0]);

        // Convert type
        Type type = Type.fromValue(meta.getType().getValue());

        // Convert codec
        CompressionCodec codec = CompressionCodec.fromValue(
            meta.getCodec().getValue());

        long dictionaryPageOffset = meta.isSetDictionary_page_offset()
            ? meta.getDictionary_page_offset() : -1;

        // Extract statistics if available
        ColumnStatistics statistics = null;
        if (meta.isSetStatistics()) {
          org.apache.parquet.format.Statistics stats = meta.getStatistics();
          byte[] min = null;
          byte[] max = null;
          Long nullCount = null;
          Long distinctCount = null;

          // Use min_value/max_value if available, otherwise fall back to min/max
          if (stats.isSetMin_value()) {
            min = stats.getMin_value();
          } else if (stats.isSetMin()) {
            min = stats.getMin();
          }

          if (stats.isSetMax_value()) {
            max = stats.getMax_value();
          } else if (stats.isSetMax()) {
            max = stats.getMax();
          }

          if (stats.isSetNull_count()) {
            nullCount = stats.getNull_count();
          }

          if (stats.isSetDistinct_count()) {
            distinctCount = stats.getDistinct_count();
          }

          statistics = new ColumnStatistics(min, max, nullCount, distinctCount);
        }

        ParquetMetadata.ColumnChunkMetadata colMeta =
            new ParquetMetadata.ColumnChunkMetadata(
                type,
                path,
                codec,
                meta.getData_page_offset(),
                dictionaryPageOffset,
                meta.getTotal_compressed_size(),
                meta.getTotal_uncompressed_size(),
                meta.getNum_values(),
                statistics
            );

        columnChunks.add(colMeta);
      }

      rowGroups.add(new ParquetMetadata.RowGroupMetadata(
          columnChunks,
          rg.getTotal_byte_size(),
          rg.getNum_rows()
      ));
    }

    return new ParquetMetadata(fileMetadata, rowGroups);
  }

  /**
   * Recursively builds column descriptors from schema elements.
   *
   * @deprecated leaf descriptors and their logical annotations are derived centrally from the
   * reconstructed schema tree by {@link SchemaDescriptor#fromSchemaElements}; this method is
   * retained for source compatibility and delegates to the same tree.
   * @param schemaElements the list of all schema elements from the file
   * @param index the current index in schemaElements to process
   * @param currentPath the path from the root to the current element
   * @param currentDefLevel the current definition level
   * @param currentRepLevel the current repetition level
   * @param columns the list to add discovered ColumnDescriptors to
   * @return the next index to process after this element and its children
   */
  private static int buildColumns(List<SchemaElement> schemaElements, int index,
                                  String[] currentPath, int currentDefLevel,
                                  int currentRepLevel,
                                  List<ColumnDescriptor> columns) {
    // Delegate to the central tree so annotations are applied consistently.
    SchemaDescriptor schema = SchemaDescriptor.fromSchemaElements(
        schemaElements.get(0).getName(), schemaElements);
    columns.clear();
    columns.addAll(schema.columns());
    return schemaElements.size();
  }

  /**
   * Builds logical columns from physical columns by reconstructing the annotated schema tree.
   *
   * <p>Classification is annotation-first (LIST per the 3-level standard plus legacy
   * variants, MAP per the 3-level standard with a required key, STRUCT as the unannotated
   * default); the {@code key_value} name pattern is only a last-resort legacy heuristic, so
   * unannotated structures with MAP-like child names are never classified as MAP. Physical
   * columns supplied by the caller are matched back by full path.
   *
   * @param physicalColumns the list of physical ColumnDescriptors
   * @param schemaElements the schema elements of the file schema
   * @return a list of LogicalColumnDescriptors representing the logical schema
   */
  public static List<LogicalColumnDescriptor> buildLogicalColumns(
      List<ColumnDescriptor> physicalColumns,
      List<SchemaElement> schemaElements) {
    SchemaDescriptor schema = SchemaDescriptor.fromSchemaElements(
        schemaElements.get(0).getName(), schemaElements);
    return schema.logicalColumns();
  }


  /**
   * Reads and verifies the header magic bytes to confirm this is a Parquet file.
   *
   * <p>Valid Parquet files start with the magic bytes "PAR1".
   *
   * @param reader the ChunkReader for reading file contents
   * @return {@code true} if the file has valid Parquet magic bytes, {@code false} otherwise
   * @throws IOException if an I/O error occurs while reading the file
   */
  public static boolean verifyMagic(ChunkReader reader) throws IOException {
    if (reader.length() < 4) {
      return false;
    }

    ByteBuffer header = reader.readBytes(0, 4);
    byte[] magic = new byte[4];
    header.get(magic);

    return java.util.Arrays.equals(magic, MAGIC);
  }
}
