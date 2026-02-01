package org.parquet.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.parquet.ParquetRowIterator;
import org.parquet.SerializedFileReader;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.LogicalColumnDescriptor;
import org.parquet.model.ParquetMetadata;
import org.parquet.model.RowColumnGroup;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.Type;

/**
 * Converts a Parquet file to JSON format with metadata and row data.
 * <p>
 * The output JSON has two main properties:
 * - "metadata": contains column descriptions, statistics, and file information
 * - "rows": contains the actual data from the parquet file
 */
public class ParquetToJsonConverter {

  private final Gson gson;

  public ParquetToJsonConverter() {
    this.gson = new GsonBuilder().setPrettyPrinting().create();
  }

  /**
   * Convert a Parquet file to JSON and write to a file
   *
   * @param parquetFilePath Path to the input Parquet file
   * @param writer          writer to output JSON file
   * @throws IOException if file operations fail
   */
  public void convert(String parquetFilePath, Writer writer) throws IOException {
    JsonObject result = convertToJson(parquetFilePath);
    gson.toJson(result, writer);
  }

  /**
   * Convert a Parquet file to a JSON object
   *
   * @param parquetFilePath Path to the input Parquet file
   * @return JsonObject containing metadata and rows
   * @throws IOException if file operations fail
   */
  public JsonObject convertToJson(String parquetFilePath) throws IOException {
    JsonObject result = new JsonObject();

    try (SerializedFileReader reader = new SerializedFileReader(parquetFilePath)) {
      // Extract metadata
      result.add("metadata", extractMetadata(reader));

      // Extract rows
      result.add("rows", extractRows(reader));
    }

    return result;
  }

  /**
   * Extract metadata from the Parquet file
   */
  private JsonObject extractMetadata(SerializedFileReader reader) {
    JsonObject metadata = new JsonObject();
    ParquetMetadata parquetMetadata = reader.getMetadata();
    SchemaDescriptor schema = reader.getSchema();

    // File-level information
    metadata.addProperty("version", parquetMetadata.fileMetadata().version());
    metadata.addProperty("numRows", parquetMetadata.fileMetadata().numRows());
    metadata.addProperty("numRowGroups", parquetMetadata.getNumRowGroups());
    metadata.addProperty("numColumns", schema.getNumLogicalColumns());

    // Key-value metadata
    JsonObject kvMetadata = new JsonObject();
    for (Map.Entry<String, String> entry : parquetMetadata.fileMetadata().keyValueMetadata()
        .entrySet()) {
      kvMetadata.addProperty(entry.getKey(), entry.getValue());
    }
    metadata.add("keyValueMetadata", kvMetadata);

    // Column descriptions
    JsonArray columns = new JsonArray();
    for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
      LogicalColumnDescriptor logicalCol = schema.getLogicalColumn(i);
      JsonObject columnInfo = new JsonObject();

      columnInfo.addProperty("name", logicalCol.getName());
      columnInfo.addProperty("isMap", logicalCol.isMap());

      if (!logicalCol.isMap()) {
        ColumnDescriptor physicalCol = logicalCol.getPhysicalDescriptor();
        columnInfo.addProperty("type", physicalCol.physicalType().toString());
        columnInfo.addProperty("maxDefinitionLevel", physicalCol.maxDefinitionLevel());
        columnInfo.addProperty("maxRepetitionLevel", physicalCol.maxRepetitionLevel());
      } else {
        columnInfo.addProperty("type", "MAP");
        columnInfo.addProperty("keyType", logicalCol.getMapMetadata().keyType().toString());
        columnInfo.addProperty("valueType", logicalCol.getMapMetadata().valueType().toString());
      }

      // Add statistics from all row groups for this column
      JsonObject stats = extractColumnStatistics(parquetMetadata, logicalCol, schema);
      if (stats.size() > 0) {
        columnInfo.add("statistics", stats);
      }

      columns.add(columnInfo);
    }
    metadata.add("columns", columns);

    // Row group information
    JsonArray rowGroups = new JsonArray();
    for (int i = 0; i < parquetMetadata.getNumRowGroups(); i++) {
      ParquetMetadata.RowGroupMetadata rgMetadata = parquetMetadata.rowGroups().get(i);
      JsonObject rgInfo = new JsonObject();

      rgInfo.addProperty("index", i);
      rgInfo.addProperty("numRows", rgMetadata.numRows());
      rgInfo.addProperty("totalByteSize", rgMetadata.totalByteSize());

      rowGroups.add(rgInfo);
    }
    metadata.add("rowGroups", rowGroups);

    return metadata;
  }

  /**
   * Extract statistics for a logical column across all row groups
   */
  private JsonObject extractColumnStatistics(ParquetMetadata metadata,
                                             LogicalColumnDescriptor logicalCol,
                                             SchemaDescriptor schema) {
    JsonObject stats = new JsonObject();

    if (logicalCol.isMap()) {
      // For maps, we don't have straightforward statistics
      return stats;
    }

    // Find the physical column in the metadata
    ColumnDescriptor physicalCol = logicalCol.getPhysicalDescriptor();
    String columnPath = physicalCol.getPathString();

    long totalValues = 0;
    long totalNulls = 0;
    boolean hasNullCount = false;

    // Aggregate statistics across all row groups
    for (ParquetMetadata.RowGroupMetadata rowGroup : metadata.rowGroups()) {
      for (ParquetMetadata.ColumnChunkMetadata columnChunk : rowGroup.columns()) {
        if (columnChunk.path().length > 0 &&
            String.join(".", columnChunk.path()).equals(columnPath)) {

          totalValues += columnChunk.numValues();

          if (columnChunk.statistics() != null) {
            ParquetMetadata.ColumnStatistics colStats = columnChunk.statistics();

            if (colStats.hasNullCount()) {
              totalNulls += colStats.nullCount();
              hasNullCount = true;
            }

            // Add min/max from first row group (as examples)
            if (!stats.has("min") && colStats.hasMin()) {
              stats.addProperty("min", decodeStatValue(colStats.min(), physicalCol.physicalType()));
            }
            if (!stats.has("max") && colStats.hasMax()) {
              stats.addProperty("max", decodeStatValue(colStats.max(), physicalCol.physicalType()));
            }
          }
        }
      }
    }

    stats.addProperty("totalValues", totalValues);
    if (hasNullCount) {
      stats.addProperty("nullCount", totalNulls);
      stats.addProperty("nonNullCount", totalValues - totalNulls);
    }

    return stats;
  }

  /**
   * Decode a statistics value based on its type
   */
  private String decodeStatValue(byte[] value, Type type) {
    if (value == null || value.length == 0) {
      return null;
    }

    try {
      switch (type) {
        case INT32:
          if (value.length >= 4) {
            int intVal = littleEndianBuffer(value).getInt();
            return String.valueOf(intVal);
          }
          break;
        case INT64:
          if (value.length >= 8) {
            long longVal = littleEndianBuffer(value).getLong();
            return String.valueOf(longVal);
          }
          break;
        case FLOAT:
          if (value.length >= 4) {
            float floatVal = littleEndianBuffer(value).getFloat();
            return String.valueOf(floatVal);
          }
          break;
        case DOUBLE:
          if (value.length >= 8) {
            double doubleVal = littleEndianBuffer(value).getDouble();
            return String.valueOf(doubleVal);
          }
          break;
        case BYTE_ARRAY:
          return new String(value, StandardCharsets.UTF_8);
        case BOOLEAN:
          if (value.length > 0) {
            return String.valueOf(value[0] != 0);
          }
          break;
      }
    } catch (Exception e) {
      // If decoding fails, return hex representation
      return bytesToHex(value);
    }

    return bytesToHex(value);
  }

  private static ByteBuffer littleEndianBuffer(byte[] value) {
    return ByteBuffer.wrap(value).order(java.nio.ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Convert byte array to hex string
   */
  private String bytesToHex(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return "0x" + sb.toString();
  }

  /**
   * Extract rows from the Parquet file
   */
  private JsonArray extractRows(SerializedFileReader reader) throws IOException {
    JsonArray rows = new JsonArray();
    SchemaDescriptor schema = reader.getSchema();

    ParquetRowIterator iterator = new ParquetRowIterator(reader, false);
    try {
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        JsonObject rowObject = new JsonObject();

        for (int i = 0; i < row.getColumnCount(); i++) {
          LogicalColumnDescriptor logicalCol = schema.getLogicalColumn(i);
          String columnName = logicalCol.getName();
          Object value = row.getColumnValue(i);

          addValueToJson(rowObject, columnName, value);
        }

        rows.add(rowObject);
      }
    } finally {
      iterator.close();
    }

    return rows;
  }

  /**
   * Add a value to a JSON object, handling different types appropriately
   */
  private void addValueToJson(JsonObject jsonObject, String key, Object value) {
    if (value == null) {
      jsonObject.add(key, null);
    } else if (value instanceof String) {
      jsonObject.addProperty(key, (String) value);
    } else if (value instanceof Number) {
      jsonObject.addProperty(key, (Number) value);
    } else if (value instanceof Boolean) {
      jsonObject.addProperty(key, (Boolean) value);
    } else if (value instanceof Map) {
      // Handle map columns
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) value;
      JsonObject mapObject = new JsonObject();
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        addValueToJson(mapObject, entry.getKey(), entry.getValue());
      }
      jsonObject.add(key, mapObject);
    } else if (value instanceof byte[]) {
      // Convert byte arrays to strings (assuming UTF-8)
      jsonObject.addProperty(key, new String((byte[]) value, StandardCharsets.UTF_8));
    } else if (value instanceof List) {
      // Handle lists (for repeated fields)
      JsonArray array = new JsonArray();
      for (Object item : (List<?>) value) {
        if (item instanceof Number) {
          array.add((Number) item);
        } else if (item instanceof String) {
          array.add((String) item);
        } else if (item instanceof Boolean) {
          array.add((Boolean) item);
        } else {
          array.add(gson.toJsonTree(item));
        }
      }
      jsonObject.add(key, array);
    } else {
      // For unknown types, use Gson's default serialization
      jsonObject.add(key, gson.toJsonTree(value));
    }
  }

  /**
   * Main method for command-line usage
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: ParquetToJsonConverter <parquet-file> <json-file>");
      System.err.println("  <parquet-file>: Path to input Parquet file");
      System.err.println("  <json-file>: Path to output JSON file");
      System.exit(1);
    }

    String parquetFilePath = args[0];
    String jsonFilePath = args.length > 1 ? args[1] : null;
    Writer jsonWriter;
    if (jsonFilePath != null) {
      jsonWriter = new FileWriter(jsonFilePath);
    } else {
      jsonWriter = new OutputStreamWriter(System.out);
    }
    try {
      ParquetToJsonConverter converter = new ParquetToJsonConverter();
      converter.convert(parquetFilePath, jsonWriter);
      System.out.println("Successfully converted " + parquetFilePath + " to " + jsonFilePath);
    } catch (IOException e) {
      System.err.println("Error converting Parquet file: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }
}
