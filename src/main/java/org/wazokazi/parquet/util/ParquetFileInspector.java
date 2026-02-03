package org.wazokazi.parquet.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.wazokazi.parquet.RowColumnGroupIterator;
import org.wazokazi.parquet.SerializedFileReader;
import org.wazokazi.parquet.model.ColumnDescriptor;
import org.wazokazi.parquet.model.LogicalColumnDescriptor;
import org.wazokazi.parquet.model.MapMetadata;
import org.wazokazi.parquet.model.ParquetMetadata;
import org.wazokazi.parquet.model.RowColumnGroup;
import org.wazokazi.parquet.model.SchemaDescriptor;
import org.wazokazi.parquet.model.Type;

/**
 * Helper class to inspect and describe Parquet files in detail.
 * <p>
 * Provides comprehensive information about:
 * - File metadata (version, row count, size)
 * - Schema structure (columns, types, logical types)
 * - Row group details (count, size, location)
 * - Column chunk details (encoding, compression, statistics)
 * - Data sampling with row group and index information
 */
public class ParquetFileInspector implements AutoCloseable {

  private final SerializedFileReader reader;
  private final ParquetMetadata metadata;
  private final SchemaDescriptor schema;
  private final Path filePath;

  /**
   * Create an inspector for a Parquet file.
   *
   * @param filePath the path to the Parquet file to inspect
   * @throws IOException if the file cannot be read or is not a valid Parquet file
   */
  public ParquetFileInspector(String filePath) throws IOException {
    this(Paths.get(filePath));
  }

  /**
   * Create an inspector for a Parquet file.
   *
   * @param filePath the path to the Parquet file to inspect
   * @throws IOException if the file cannot be read or is not a valid Parquet file
   */
  public ParquetFileInspector(Path filePath) throws IOException {
    this.filePath = filePath;
    this.reader = new SerializedFileReader(filePath);
    this.metadata = reader.getMetadata();
    this.schema = reader.getSchema();
  }

  /**
   * Generate a comprehensive description of the Parquet file.
   * <p>
   * The output includes:
   * <ul>
   *   <li>File information (path, size, version, row count)</li>
   *   <li>Schema structure (logical and physical columns)</li>
   *   <li>Row group details (count, size, compression)</li>
   *   <li>Column chunk details (encoding, compression, statistics)</li>
   *   <li>Data sampling (first few rows with location information)</li>
   * </ul>
   *
   * @return a formatted text report of the Parquet file structure and contents
   */
  public String inspect() {
    StringBuilder sb = new StringBuilder();

    sb.append("=".repeat(80)).append("\n");
    sb.append("PARQUET FILE INSPECTION\n");
    sb.append("=".repeat(80)).append("\n\n");

    // File information
    inspectFileInfo(sb);
    sb.append("\n");

    // Schema information
    inspectSchema(sb);
    sb.append("\n");

    // Row group information
    inspectRowGroups(sb);
    sb.append("\n");

    // Column chunk details
    inspectColumnChunks(sb);
    sb.append("\n");

    // Data sampling
    inspectDataSampling(sb);

    return sb.toString();
  }

  /**
   * Generate a comprehensive JSON description of the Parquet file.
   *
   * @return JsonObject containing all file details
   */
  public JsonObject inspectAsJson() {
    JsonObject root = new JsonObject();

    // File information
    root.add("fileInfo", buildFileInfoJson());

    // Schema information
    root.add("schema", buildSchemaJson());

    // Row groups
    root.add("rowGroups", buildRowGroupsJson());

    // Column chunks (first row group)
    if (!metadata.rowGroups().isEmpty()) {
      root.add("columnChunks", buildColumnChunksJson());
    }

    // Data sampling
    root.add("dataSampling", buildDataSamplingJson());

    // Statistics
    root.add("statistics", buildStatisticsJson());

    return root;
  }

  /**
   * Get JSON inspection as formatted string.
   *
   * @return Pretty-printed JSON string
   */
  public String inspectAsJsonString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(inspectAsJson());
  }

  /**
   * Build file information JSON.
   * <p>
   * Includes file path, size, format version, row count, row group count,
   * created-by metadata, and column counts.
   *
   * @return a JsonObject containing file-level metadata
   */
  private JsonObject buildFileInfoJson() {
    JsonObject fileInfo = new JsonObject();

    fileInfo.addProperty("filePath", filePath.toAbsolutePath().toString());

    if (filePath.toFile().exists()) {
      long fileSize = filePath.toFile().length();
      fileInfo.addProperty("fileSizeBytes", fileSize);
      fileInfo.addProperty("fileSizeMB", fileSize / (1024.0 * 1024.0));
    }

    ParquetMetadata.FileMetadata fileMeta = metadata.fileMetadata();
    fileInfo.addProperty("formatVersion", fileMeta.version());
    fileInfo.addProperty("totalRows", fileMeta.numRows());
    fileInfo.addProperty("rowGroups", metadata.rowGroups().size());

    if (fileMeta.keyValueMetadata() != null &&
        fileMeta.keyValueMetadata().containsKey("created_by")) {
      fileInfo.addProperty("createdBy", fileMeta.keyValueMetadata().get("created_by"));
    }

    fileInfo.addProperty("physicalColumns", schema.getNumColumns());
    if (schema.hasLogicalColumns()) {
      fileInfo.addProperty("logicalColumns", schema.getNumLogicalColumns());
    }

    return fileInfo;
  }

  /**
   * Build schema JSON.
   * <p>
   * Includes both logical columns (user-facing view) and physical columns
   * (storage view) with their types, paths, and repetition information.
   *
   * @return a JsonObject containing schema structure information
   */
  private JsonObject buildSchemaJson() {
    JsonObject schemaObj = new JsonObject();

    // Logical columns
    if (schema.hasLogicalColumns()) {
      JsonArray logicalCols = new JsonArray();
      for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
        LogicalColumnDescriptor col = schema.getLogicalColumn(i);
        JsonObject colObj = new JsonObject();
        colObj.addProperty("index", i);
        colObj.addProperty("name", col.getName());
        colObj.addProperty("logicalType", col.getLogicalType().toString());

        if (col.isPrimitive()) {
          colObj.addProperty("physicalType", col.getPhysicalType().toString());
        } else if (col.isMap()) {
          MapMetadata mapMeta = col.getMapMetadata();
          colObj.addProperty("keyType", mapMeta.keyType().toString());
          colObj.addProperty("valueType", mapMeta.valueType().toString());
        }

        logicalCols.add(colObj);
      }
      schemaObj.add("logicalColumns", logicalCols);
    }

    // Physical columns
    JsonArray physicalCols = new JsonArray();
    for (int i = 0; i < schema.getNumColumns(); i++) {
      ColumnDescriptor col = schema.getColumn(i);
      JsonObject colObj = new JsonObject();
      colObj.addProperty("index", i);
      colObj.addProperty("path", col.getPathString());
      colObj.addProperty("physicalType", col.physicalType().toString());
      colObj.addProperty("maxDefinitionLevel", col.maxDefinitionLevel());
      colObj.addProperty("maxRepetitionLevel", col.maxRepetitionLevel());

      String repetitionType = col.maxRepetitionLevel() > 0 ? "REPEATED" :
          (col.maxDefinitionLevel() > 0 ? "OPTIONAL" : "REQUIRED");
      colObj.addProperty("repetitionType", repetitionType);

      physicalCols.add(colObj);
    }
    schemaObj.add("physicalColumns", physicalCols);

    return schemaObj;
  }

  /**
   * Build row groups JSON.
   * <p>
   * Includes information about each row group: row count, size, compression
   * ratio, and column count.
   *
   * @return a JsonObject containing row group metadata for all row groups
   */
  private JsonObject buildRowGroupsJson() {
    JsonObject rowGroupsObj = new JsonObject();
    JsonArray groups = new JsonArray();

    for (int i = 0; i < metadata.rowGroups().size(); i++) {
      ParquetMetadata.RowGroupMetadata rg = metadata.rowGroups().get(i);
      JsonObject rgObj = new JsonObject();

      rgObj.addProperty("index", i);
      rgObj.addProperty("numRows", rg.numRows());
      rgObj.addProperty("totalByteSize", rg.totalByteSize());
      rgObj.addProperty("numColumns", rg.columns().size());

      // Calculate compression ratio
      long totalUncompressed = 0;
      long totalCompressed = 0;
      for (ParquetMetadata.ColumnChunkMetadata col : rg.columns()) {
        totalUncompressed += col.totalUncompressedSize();
        totalCompressed += col.totalCompressedSize();
      }

      if (totalCompressed > 0) {
        double ratio = (double) totalUncompressed / totalCompressed;
        rgObj.addProperty("compressionRatio", ratio);
        rgObj.addProperty("uncompressedSize", totalUncompressed);
        rgObj.addProperty("compressedSize", totalCompressed);
      }

      groups.add(rgObj);
    }

    rowGroupsObj.add("groups", groups);
    rowGroupsObj.addProperty("count", metadata.rowGroups().size());

    return rowGroupsObj;
  }

  /**
   * Build column chunks JSON (first row group).
   * <p>
   * Provides detailed information about each column chunk in the first row group,
   * including type, codec, compression ratio, and statistics (min, max, null count,
   * distinct count).
   *
   * @return a JsonObject containing column chunk details for the first row group
   */
  private JsonObject buildColumnChunksJson() {
    JsonObject columnsObj = new JsonObject();
    JsonArray chunks = new JsonArray();

    ParquetMetadata.RowGroupMetadata firstRowGroup = metadata.rowGroups().get(0);

    for (int i = 0; i < firstRowGroup.columns().size(); i++) {
      ParquetMetadata.ColumnChunkMetadata col = firstRowGroup.columns().get(i);
      JsonObject chunkObj = new JsonObject();

      chunkObj.addProperty("index", i);
      chunkObj.addProperty("path", String.join(".", col.path()));
      chunkObj.addProperty("type", col.type().toString());
      chunkObj.addProperty("codec", col.codec().toString());
      chunkObj.addProperty("numValues", col.numValues());
      chunkObj.addProperty("totalUncompressedSize", col.totalUncompressedSize());
      chunkObj.addProperty("totalCompressedSize", col.totalCompressedSize());
      chunkObj.addProperty("dataPageOffset", col.dataPageOffset());

      if (col.totalUncompressedSize() > 0) {
        double ratio = (double) col.totalUncompressedSize() / col.totalCompressedSize();
        chunkObj.addProperty("compressionRatio", ratio);
      }

      // Add statistics if available
      if (col.statistics() != null) {
        JsonObject statsObj = new JsonObject();
        ParquetMetadata.ColumnStatistics stats = col.statistics();

        if (stats.hasNullCount()) {
          statsObj.addProperty("nullCount", stats.nullCount());
        }

        if (stats.hasDistinctCount()) {
          statsObj.addProperty("distinctCount", stats.distinctCount());
        }

        if (stats.hasMin()) {
          statsObj.addProperty("min", formatStatisticValue(stats.min(), col.type()));
        }

        if (stats.hasMax()) {
          statsObj.addProperty("max", formatStatisticValue(stats.max(), col.type()));
        }

        chunkObj.add("statistics", statsObj);
      }

      chunks.add(chunkObj);
    }

    columnsObj.add("chunks", chunks);
    columnsObj.addProperty("rowGroupIndex", 0);

    return columnsObj;
  }

  /**
   * Build data sampling JSON.
   * <p>
   * Samples the first few rows from the file with location information
   * (row group index and row index within the group). The number of samples
   * depends on the total row count:
   * <ul>
   *   <li>10 or fewer rows: all rows</li>
   *   <li>11-100 rows: first 10 rows</li>
   *   <li>More than 100 rows: first 5 rows</li>
   * </ul>
   *
   * @return a JsonObject containing sampled row data with location metadata
   */
  private JsonObject buildDataSamplingJson() {
    JsonObject samplingObj = new JsonObject();
    JsonArray samples = new JsonArray();

    try {
      RowColumnGroupIterator iterator = reader.rowIterator();
      long totalRows = metadata.fileMetadata().numRows();

      // Determine sampling strategy
      int samplesToShow;
      if (totalRows <= 10) {
        samplesToShow = (int) totalRows;
      } else if (totalRows <= 100) {
        samplesToShow = 10;
      } else {
        samplesToShow = 5;
      }

      samplingObj.addProperty("totalRows", totalRows);
      samplingObj.addProperty("sampledRows", samplesToShow);

      int currentRow = 0;
      int currentRowGroup = 0;
      int rowInRowGroup = 0;

      while (iterator.hasNext() && currentRow < samplesToShow) {
        RowColumnGroup row = iterator.next();

        // Determine row group and position
        if (metadata.rowGroups().size() > currentRowGroup) {
          ParquetMetadata.RowGroupMetadata rg = metadata.rowGroups().get(currentRowGroup);
          if (rowInRowGroup >= rg.numRows()) {
            currentRowGroup++;
            rowInRowGroup = 0;
          }
        }

        JsonObject rowObj = new JsonObject();
        rowObj.addProperty("rowNumber", currentRow);
        rowObj.addProperty("rowGroup", currentRowGroup);
        rowObj.addProperty("rowIndexInGroup", rowInRowGroup);

        JsonObject values = new JsonObject();

        if (schema.hasLogicalColumns()) {
          for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
            LogicalColumnDescriptor col = schema.getLogicalColumn(i);
            Object value = row.getColumnValue(col.getName());
            addValueToJson(values, col.getName(), value);
          }
        } else {
          for (int i = 0; i < row.getColumnCount(); i++) {
            Object value = row.getColumnValue(i);
            String colName = schema.getColumn(i).getPathString();
            addValueToJson(values, colName, value);
          }
        }

        rowObj.add("values", values);
        samples.add(rowObj);

        currentRow++;
        rowInRowGroup++;
      }

    } catch (Exception e) {
      samplingObj.addProperty("error", e.getMessage());
    }

    samplingObj.add("samples", samples);
    return samplingObj;
  }

  /**
   * Build statistics JSON.
   * <p>
   * Provides file-level statistics including total rows, file size, average
   * row size, and overall compression metrics (uncompressed size, compressed
   * size, compression ratio, bytes saved).
   *
   * @return a JsonObject containing file-level statistics
   */
  private JsonObject buildStatisticsJson() {
    JsonObject stats = new JsonObject();

    long totalRows = metadata.fileMetadata().numRows();
    long totalBytes = filePath.toFile().length();

    stats.addProperty("totalRows", totalRows);
    stats.addProperty("totalFileSize", totalBytes);
    stats.addProperty("averageRowSize", totalRows > 0 ? (double) totalBytes / totalRows : 0);

    // Overall compression
    long totalUncompressed = 0;
    long totalCompressed = 0;

    for (ParquetMetadata.RowGroupMetadata rg : metadata.rowGroups()) {
      for (ParquetMetadata.ColumnChunkMetadata col : rg.columns()) {
        totalUncompressed += col.totalUncompressedSize();
        totalCompressed += col.totalCompressedSize();
      }
    }

    if (totalCompressed > 0) {
      stats.addProperty("totalUncompressedSize", totalUncompressed);
      stats.addProperty("totalCompressedSize", totalCompressed);
      stats.addProperty("overallCompressionRatio", (double) totalUncompressed / totalCompressed);
      stats.addProperty("bytesSaved", totalUncompressed - totalCompressed);
    }

    return stats;
  }

  /**
   * Add a value to JSON object, handling different types.
   * <p>
   * Handles primitives (String, Number, Boolean), byte arrays (converted to UTF-8),
   * Maps (recursively converted to JsonObject), and Lists (converted to JsonArray).
   *
   * @param obj the JsonObject to add the value to
   * @param name the property name
   * @param value the value to add (can be null)
   */
  private void addValueToJson(JsonObject obj, String name, Object value) {
    if (value == null) {
      obj.add(name, null);
      return;
    }

    if (value instanceof String) {
      obj.addProperty(name, (String) value);
    } else if (value instanceof Number) {
      obj.addProperty(name, (Number) value);
    } else if (value instanceof Boolean) {
      obj.addProperty(name, (Boolean) value);
    } else if (value instanceof byte[]) {
      String str = new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8);
      obj.addProperty(name, str);
    } else if (value instanceof Map<?, ?> map) {
      JsonObject mapObj = new JsonObject();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        String key = entry.getKey().toString();
        addValueToJson(mapObj, key, entry.getValue());
      }
      obj.add(name, mapObj);
    } else if (value instanceof List) {
      JsonArray array = new JsonArray();
      for (Object item : (List<?>) value) {
        if (item == null) {
          array.add((String) null);
        } else if (item instanceof String) {
          array.add((String) item);
        } else if (item instanceof Number) {
          array.add((Number) item);
        } else if (item instanceof Boolean) {
          array.add((Boolean) item);
        } else {
          array.add(item.toString());
        }
      }
      obj.add(name, array);
    } else {
      obj.addProperty(name, value.toString());
    }
  }

  /**
   * Inspect basic file information.
   * <p>
   * Appends file path, size, format version, row count, row group count,
   * and column counts to the StringBuilder.
   *
   * @param sb the StringBuilder to append the information to
   */
  private void inspectFileInfo(StringBuilder sb) {
    sb.append("FILE INFORMATION\n");
    sb.append("-".repeat(80)).append("\n");

    // File path and size
    sb.append(String.format("File Path:        %s\n", filePath.toAbsolutePath()));
    if (filePath.toFile().exists()) {
      long fileSize = filePath.toFile().length();
      sb.append(String.format("File Size:        %,d bytes (%.2f MB)\n",
          fileSize, fileSize / (1024.0 * 1024.0)));
    }

    // Metadata
    ParquetMetadata.FileMetadata fileMeta = metadata.fileMetadata();
    sb.append(String.format("Format Version:   %d\n", fileMeta.version()));
    sb.append(String.format("Total Rows:       %,d\n", fileMeta.numRows()));
    sb.append(String.format("Row Groups:       %d\n", metadata.rowGroups().size()));
    // Created by info from key-value metadata if available
    if (fileMeta.keyValueMetadata() != null &&
        fileMeta.keyValueMetadata().containsKey("created_by")) {
      sb.append(
          String.format("Created By:       %s\n", fileMeta.keyValueMetadata().get("created_by")));
    }

    // Physical vs logical columns
    sb.append(String.format("Physical Columns: %d\n", schema.getNumColumns()));
    if (schema.hasLogicalColumns()) {
      sb.append(String.format("Logical Columns:  %d\n", schema.getNumLogicalColumns()));
    }
  }

  /**
   * Inspect schema structure.
   * <p>
   * Appends both logical columns (user-facing view) and physical columns
   * (storage view) with their types and properties.
   *
   * @param sb the StringBuilder to append the schema information to
   */
  private void inspectSchema(StringBuilder sb) {
    sb.append("SCHEMA STRUCTURE\n");
    sb.append("-".repeat(80)).append("\n");

    if (schema.hasLogicalColumns()) {
      // Show logical columns (user-facing view)
      sb.append("Logical Columns (User View):\n");
      for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
        LogicalColumnDescriptor col = schema.getLogicalColumn(i);
        sb.append(String.format("  [%d] %s\n", i, describeLogicalColumn(col)));
      }
      sb.append("\n");
    }

    // Show physical columns (storage view)
    sb.append("Physical Columns (Storage View):\n");
    for (int i = 0; i < schema.getNumColumns(); i++) {
      ColumnDescriptor col = schema.getColumn(i);
      sb.append(String.format("  [%d] %s\n", i, describePhysicalColumn(col)));
    }
  }

  /**
   * Describe a logical column.
   * <p>
   * Formats the column name and type information for display. Handles primitive
   * types, map types, and other logical types.
   *
   * @param col the logical column descriptor
   * @return a formatted string describing the column
   */
  private String describeLogicalColumn(LogicalColumnDescriptor col) {
    if (col.isPrimitive()) {
      return String.format("%s: %s",
          col.getName(),
          col.getPhysicalType());
    } else if (col.isMap()) {
      MapMetadata mapMeta = col.getMapMetadata();
      return String.format("%s: MAP<%s, %s>",
          col.getName(),
          mapMeta.keyType(),
          mapMeta.valueType());
    } else {
      return String.format("%s: %s", col.getName(), col.getLogicalType());
    }
  }

  /**
   * Describe a physical column.
   * <p>
   * Formats the column with repetition type, physical type, path, and
   * definition/repetition levels for display.
   *
   * @param col the column descriptor
   * @return a formatted string describing the physical column
   */
  private String describePhysicalColumn(ColumnDescriptor col) {
    StringBuilder desc = new StringBuilder();

    // Repetition type
    if (col.maxRepetitionLevel() > 0) {
      desc.append("repeated ");
    } else if (col.maxDefinitionLevel() > 0) {
      desc.append("optional ");
    } else {
      desc.append("required ");
    }

    // Type
    desc.append(col.physicalType().name().toLowerCase());

    // Path
    desc.append(" ").append(col.getPathString());

    // Levels
    desc.append(String.format(" (maxDef=%d, maxRep=%d)",
        col.maxDefinitionLevel(),
        col.maxRepetitionLevel()));

    return desc.toString();
  }

  /**
   * Inspect row group information.
   * <p>
   * Appends information about each row group including row count, byte size,
   * column count, and compression ratio.
   *
   * @param sb the StringBuilder to append the row group information to
   */
  private void inspectRowGroups(StringBuilder sb) {
    sb.append("ROW GROUPS\n");
    sb.append("-".repeat(80)).append("\n");

    List<ParquetMetadata.RowGroupMetadata> rowGroups = metadata.rowGroups();

    for (int i = 0; i < rowGroups.size(); i++) {
      ParquetMetadata.RowGroupMetadata rg = rowGroups.get(i);

      sb.append(String.format("Row Group %d:\n", i));
      sb.append(String.format("  Rows:              %,d\n", rg.numRows()));
      sb.append(String.format("  Total Byte Size:   %,d bytes (%.2f MB)\n",
          rg.totalByteSize(),
          rg.totalByteSize() / (1024.0 * 1024.0)));
      sb.append(String.format("  Columns:           %d\n", rg.columns().size()));

      // Calculate compression ratio
      long totalUncompressed = 0;
      long totalCompressed = 0;
      for (ParquetMetadata.ColumnChunkMetadata col : rg.columns()) {
        totalUncompressed += col.totalUncompressedSize();
        totalCompressed += col.totalCompressedSize();
      }

      if (totalUncompressed > 0) {
        double ratio = (double) totalUncompressed / totalCompressed;
        sb.append(String.format("  Compression Ratio: %.2fx (uncompressed: %,d, compressed: %,d)\n",
            ratio, totalUncompressed, totalCompressed));
      }

      sb.append("\n");
    }
  }

  /**
   * Inspect column chunk details for first row group.
   * <p>
   * Appends detailed information about each column chunk in the first row group,
   * including type, compression codec, sizes, compression ratio, data page offset,
   * and statistics (null count, distinct count, min, max).
   *
   * @param sb the StringBuilder to append the column chunk details to
   */
  private void inspectColumnChunks(StringBuilder sb) {
    sb.append("COLUMN CHUNK DETAILS (First Row Group)\n");
    sb.append("-".repeat(80)).append("\n");

    if (metadata.rowGroups().isEmpty()) {
      sb.append("No row groups found.\n");
      return;
    }

    ParquetMetadata.RowGroupMetadata firstRowGroup = metadata.rowGroups().get(0);
    List<ParquetMetadata.ColumnChunkMetadata> columns = firstRowGroup.columns();

    for (int i = 0; i < columns.size(); i++) {
      ParquetMetadata.ColumnChunkMetadata col = columns.get(i);

      sb.append(String.format("Column %d: %s\n", i, String.join(".", col.path())));
      sb.append(String.format("  Type:                %s\n", col.type()));
      sb.append(String.format("  Compression:         %s\n", col.codec()));
      sb.append(String.format("  Values:              %,d\n", col.numValues()));
      sb.append(String.format("  Uncompressed Size:   %,d bytes\n", col.totalUncompressedSize()));
      sb.append(String.format("  Compressed Size:     %,d bytes\n", col.totalCompressedSize()));

      if (col.totalUncompressedSize() > 0) {
        double ratio = (double) col.totalUncompressedSize() / col.totalCompressedSize();
        sb.append(String.format("  Compression Ratio:   %.2fx\n", ratio));
      }

      sb.append(String.format("  Data Page Offset:    %,d\n", col.dataPageOffset()));

      // Display statistics if available
      if (col.statistics() != null) {
        ParquetMetadata.ColumnStatistics stats = col.statistics();
        sb.append("  Statistics:\n");

        if (stats.hasNullCount()) {
          sb.append(String.format("    Null Count:        %,d\n", stats.nullCount()));
        }

        if (stats.hasDistinctCount()) {
          sb.append(String.format("    Distinct Count:    %,d\n", stats.distinctCount()));
        }

        if (stats.hasMin()) {
          String minStr = formatStatisticValue(stats.min(), col.type());
          sb.append(String.format("    Min Value:         %s\n", minStr));
        }

        if (stats.hasMax()) {
          String maxStr = formatStatisticValue(stats.max(), col.type());
          sb.append(String.format("    Max Value:         %s\n", maxStr));
        }
      }

      sb.append("\n");
    }
  }

  /**
   * Sample data from the file with location information.
   * <p>
   * Appends sample rows from the file, including row group index and row index
   * within the group. The number of samples depends on total row count.
   *
   * @param sb the StringBuilder to append the sample data to
   */
  private void inspectDataSampling(StringBuilder sb) {
    sb.append("DATA SAMPLING\n");
    sb.append("-".repeat(80)).append("\n");

    try {
      RowColumnGroupIterator iterator = reader.rowIterator();
      long totalRows = metadata.fileMetadata().numRows();

      // Determine sampling strategy
      int samplesToShow;
      if (totalRows <= 10) {
        samplesToShow = (int) totalRows;
        sb.append(String.format("Showing all %d rows:\n\n", totalRows));
      } else if (totalRows <= 100) {
        samplesToShow = 10;
        sb.append(String.format("Showing first 10 rows (of %,d total):\n\n", totalRows));
      } else {
        samplesToShow = 5;
        sb.append(String.format("Showing first 5 rows (of %,d total):\n\n", totalRows));
      }

      int currentRow = 0;
      int currentRowGroup = 0;
      int rowInRowGroup = 0;

      while (iterator.hasNext() && currentRow < samplesToShow) {
        RowColumnGroup row = iterator.next();

        // Determine row group and position
        if (metadata.rowGroups().size() > currentRowGroup) {
          ParquetMetadata.RowGroupMetadata rg = metadata.rowGroups().get(currentRowGroup);
          if (rowInRowGroup >= rg.numRows()) {
            currentRowGroup++;
            rowInRowGroup = 0;
          }
        }

        sb.append(String.format("Row %d (Row Group %d, Row Index %d):\n",
            currentRow, currentRowGroup, rowInRowGroup));

        // Show column values
        if (schema.hasLogicalColumns()) {
          // Show logical columns
          for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
            LogicalColumnDescriptor col = schema.getLogicalColumn(i);
            Object value = row.getColumnValue(col.getName());
            sb.append(String.format("  %-20s = %s\n",
                col.getName(), formatValue(value)));
          }
        } else {
          // Show physical columns
          for (int i = 0; i < row.getColumnCount(); i++) {
            Object value = row.getColumnValue(i);
            String colName = schema.getColumn(i).getPathString();
            sb.append(String.format("  %-20s = %s\n",
                colName, formatValue(value)));
          }
        }

        sb.append("\n");
        currentRow++;
        rowInRowGroup++;
      }

      if (totalRows > samplesToShow) {
        sb.append(String.format("... (%,d more rows)\n", totalRows - samplesToShow));
      }

    } catch (Exception e) {
      sb.append(String.format("Error reading data: %s\n", e.getMessage()));
    }
  }

  /**
   * Format a statistic value based on its type.
   * <p>
   * Converts byte array statistics to human-readable strings based on the
   * physical type. Handles numeric types, booleans, strings, and binary data.
   *
   * @param value the statistic value as a byte array
   * @param type the physical type of the column
   * @return a formatted string representation of the statistic value
   */
  private String formatStatisticValue(byte[] value, Type type) {
    if (value == null || value.length == 0) {
      return "N/A";
    }

    try {
      java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(value);
      buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

      switch (type) {
        case INT32:
          if (value.length >= 4) {
            return String.valueOf(buffer.getInt());
          }
          break;
        case INT64:
          if (value.length >= 8) {
            return String.valueOf(buffer.getLong());
          }
          break;
        case FLOAT:
          if (value.length >= 4) {
            return String.valueOf(buffer.getFloat());
          }
          break;
        case DOUBLE:
          if (value.length >= 8) {
            return String.valueOf(buffer.getDouble());
          }
          break;
        case BOOLEAN:
          if (value.length >= 1) {
            return String.valueOf(value[0] != 0);
          }
          break;
        case BYTE_ARRAY:
        case FIXED_LEN_BYTE_ARRAY:
          // Try to decode as UTF-8 string
          String str = new String(value, java.nio.charset.StandardCharsets.UTF_8);
          // Check if it's printable
          if (str.chars().allMatch(c -> c >= 32 && c < 127 || Character.isWhitespace(c))) {
            if (str.length() > 50) {
              return "\"" + str.substring(0, 50) + "...\"";
            }
            return "\"" + str + "\"";
          } else {
            // Return hex representation for binary data
            return "0x" + bytesToHex(value, 16);
          }
        case INT96:
          // INT96 is typically used for timestamps
          return "0x" + bytesToHex(value, value.length);
      }
    } catch (Exception e) {
      // Fall through to hex representation
    }

    // Default: hex representation
    return "0x" + bytesToHex(value, 16);
  }

  /**
   * Convert bytes to hex string (limited to maxBytes).
   * <p>
   * Converts a byte array to its hexadecimal string representation, limiting
   * the output to a maximum number of bytes and adding "..." if truncated.
   *
   * @param bytes the byte array to convert
   * @param maxBytes the maximum number of bytes to convert
   * @return a hexadecimal string representation
   */
  private String bytesToHex(byte[] bytes, int maxBytes) {
    if (bytes == null || bytes.length == 0) {
      return "";
    }

    int len = Math.min(bytes.length, maxBytes);
    StringBuilder hex = new StringBuilder(len * 2);
    for (int i = 0; i < len; i++) {
      hex.append(String.format("%02x", bytes[i]));
    }
    if (bytes.length > maxBytes) {
      hex.append("...");
    }
    return hex.toString();
  }

  /**
   * Format a value for display.
   * <p>
   * Converts column values to human-readable strings, handling nulls, strings,
   * byte arrays, maps, and lists. Long strings and large collections are
   * truncated with ellipsis.
   *
   * @param value the value to format (can be null)
   * @return a formatted string representation of the value
   */
  private String formatValue(Object value) {
    if (value == null) {
      return "NULL";
    }

    if (value instanceof String str) {
      if (str.length() > 100) {
        return "\"" + str.substring(0, 100) + "...\"";
      }
      return "\"" + str + "\"";
    }

    if (value instanceof byte[] bytes) {
      String str = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
      if (str.length() > 100) {
        return "\"" + str.substring(0, 100) + "...\"";
      }
      return "\"" + str + "\"";
    }

    if (value instanceof Map<?, ?> map) {
      if (map.isEmpty()) {
        return "{}";
      }

      StringBuilder mapStr = new StringBuilder("{");
      int count = 0;
      int maxEntries = 5;

      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (count > 0) {
          mapStr.append(", ");
        }
        if (count >= maxEntries) {
          mapStr.append(String.format("... (%d more)", map.size() - maxEntries));
          break;
        }
        mapStr.append(String.format("%s: %s",
            formatValue(entry.getKey()),
            formatValue(entry.getValue())));
        count++;
      }
      mapStr.append("}");
      return mapStr.toString();
    }

    if (value instanceof List<?> list) {
      if (list.isEmpty()) {
        return "[]";
      }

      StringBuilder listStr = new StringBuilder("[");
      int maxItems = 10;
      for (int i = 0; i < Math.min(list.size(), maxItems); i++) {
        if (i > 0) {
          listStr.append(", ");
        }
        listStr.append(formatValue(list.get(i)));
      }
      if (list.size() > maxItems) {
        listStr.append(String.format(", ... (%d more)", list.size() - maxItems));
      }
      listStr.append("]");
      return listStr.toString();
    }

    return value.toString();
  }

  /**
   * Get a summary statistics report.
   * <p>
   * Provides a concise summary of file statistics including total rows,
   * file size, average row size, and overall compression metrics.
   *
   * @return a formatted text summary of file statistics
   */
  public String getStatisticsSummary() {
    StringBuilder sb = new StringBuilder();

    sb.append("STATISTICS SUMMARY\n");
    sb.append("-".repeat(80)).append("\n");

    long totalRows = metadata.fileMetadata().numRows();
    long totalBytes = filePath.toFile().length();
    double avgRowSize = totalRows > 0 ? (double) totalBytes / totalRows : 0;

    sb.append(String.format("Total Rows:           %,d\n", totalRows));
    sb.append(String.format("Total File Size:      %,d bytes (%.2f MB)\n",
        totalBytes, totalBytes / (1024.0 * 1024.0)));
    sb.append(String.format("Average Row Size:     %.2f bytes\n", avgRowSize));

    // Calculate total uncompressed vs compressed across all row groups
    long totalUncompressed = 0;
    long totalCompressed = 0;

    for (ParquetMetadata.RowGroupMetadata rg : metadata.rowGroups()) {
      for (ParquetMetadata.ColumnChunkMetadata col : rg.columns()) {
        totalUncompressed += col.totalUncompressedSize();
        totalCompressed += col.totalCompressedSize();
      }
    }

    if (totalCompressed > 0) {
      double overallRatio = (double) totalUncompressed / totalCompressed;
      sb.append(String.format("Overall Compression:  %.2fx (saved %,d bytes)\n",
          overallRatio, totalUncompressed - totalCompressed));
    }

    return sb.toString();
  }

  /**
   * Close the inspector and underlying reader.
   * <p>
   * Releases resources associated with the Parquet file reader. This method
   * should be called when the inspector is no longer needed, preferably using
   * try-with-resources.
   *
   * @throws IOException if an I/O error occurs while closing the reader
   */
  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  /**
   * Main method for command-line usage.
   * <p>
   * Provides a command-line interface to inspect Parquet files. Supports
   * multiple output formats (text, JSON) and modes (full report, statistics only).
   * <p>
   * Usage: java ParquetFileInspector &lt;parquet-file&gt; [options]
   * <p>
   * Options:
   * <ul>
   *   <li>--stats-only: Show only statistics summary (text format)</li>
   *   <li>--json: Output in JSON format</li>
   *   <li>--json-pretty: Output in pretty-printed JSON format</li>
   * </ul>
   *
   * @param args command-line arguments
   */
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: java ParquetFileInspector <parquet-file> [options]");
      System.err.println("\nOptions:");
      System.err.println("  --stats-only    Show only statistics summary (text format)");
      System.err.println("  --json          Output in JSON format");
      System.err.println(
          "  --json-pretty   Output in pretty-printed JSON format (default for JSON)");
      System.err.println("\nExamples:");
      System.err.println("  java ParquetFileInspector data.parquet");
      System.err.println("  java ParquetFileInspector data.parquet --stats-only");
      System.err.println("  java ParquetFileInspector data.parquet --json");
      System.exit(1);
    }

    String filePath = args[0];
    boolean statsOnly = false;
    boolean jsonOutput = false;

    // Parse options
    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "--stats-only":
          statsOnly = true;
          break;
        case "--json":
        case "--json-pretty":
          jsonOutput = true;
          break;
      }
    }

    try (ParquetFileInspector inspector = new ParquetFileInspector(filePath)) {
      if (jsonOutput) {
        // JSON output
        System.out.println(inspector.inspectAsJsonString());
      } else if (statsOnly) {
        // Text statistics only
        System.out.println(inspector.getStatisticsSummary());
      } else {
        // Full text report
        System.out.println(inspector.inspect());
        System.out.println();
        System.out.println(inspector.getStatisticsSummary());
      }
    } catch (IOException e) {
      System.err.println("Error inspecting file: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}
