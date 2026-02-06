package io.github.aloksingh.parquet.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ParquetRowIterator;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Converts a Parquet file to JSON format with metadata and row data.
 * <p>
 * The output JSON has two main properties:
 * - "metadata": contains column descriptions, statistics, and file information
 * - "rows": contains the actual data from the parquet file
 */
public class ParquetToJsonConverter {

  private final Gson gson;

  /**
   * Constructs a new ParquetToJsonConverter with pretty-printing enabled for JSON output.
   */
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

    try (ParquetFileReader reader = new ParquetFileReader(parquetFilePath)) {
      // Extract metadata
      result.add("metadata", extractMetadata(reader));

      // Extract rows
      result.add("rows", extractRows(reader));
    }

    return result;
  }

  /**
   * Extract metadata from the Parquet file.
   * <p>
   * Extracts comprehensive metadata including file-level information (version, row counts),
   * key-value metadata, column descriptions with statistics, and row group information.
   *
   * @param reader the ParquetFileReader for the Parquet file
   * @return JsonObject containing all metadata information
   */
  private JsonObject extractMetadata(ParquetFileReader reader) {
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
   * Extract statistics for a logical column across all row groups.
   * <p>
   * Aggregates statistics such as value counts, null counts, min/max values across
   * all row groups for a given logical column. Map columns are skipped as they don't
   * have straightforward statistics.
   *
   * @param metadata   the ParquetMetadata containing row group and column information
   * @param logicalCol the logical column descriptor to extract statistics for
   * @param schema     the schema descriptor for the Parquet file
   * @return JsonObject containing aggregated statistics (totalValues, nullCount, min, max, etc.)
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
   * Decode a statistics value based on its type.
   * <p>
   * Converts byte array representations of statistics values (min/max) to their
   * string representations based on the Parquet physical type. Falls back to
   * hex representation if decoding fails.
   *
   * @param value the byte array containing the encoded value
   * @param type  the Parquet physical type (INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, BOOLEAN)
   * @return string representation of the decoded value, or hex string if decoding fails
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
   * Convert byte array to hex string.
   * <p>
   * Converts each byte to its two-digit hexadecimal representation and
   * prefixes with "0x".
   *
   * @param bytes the byte array to convert
   * @return hex string representation prefixed with "0x", or empty string if input is null/empty
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
   * Extract rows from the Parquet file.
   * <p>
   * Iterates through all rows in the Parquet file and converts each row to a JSON object,
   * handling various column types including primitives, maps, and lists.
   *
   * @param reader the ParquetFileReader for the Parquet file
   * @return JsonArray containing all rows as JSON objects
   * @throws IOException if an error occurs while reading the file
   */
  private JsonArray extractRows(ParquetFileReader reader) throws IOException {
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
   * Add a value to a JSON object, handling different types appropriately.
   * <p>
   * Recursively handles various Java types and converts them to appropriate JSON representations:
   * primitives (String, Number, Boolean), Maps (as nested JSON objects), Lists (as JSON arrays),
   * and byte arrays (as UTF-8 strings).
   *
   * @param jsonObject the JSON object to add the value to
   * @param key        the key under which to store the value
   * @param value      the value to add (can be null, primitive, Map, List, byte[], etc.)
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
   * Main method for command-line usage.
   * <p>
   * Converts a Parquet file to JSON format. If no output file is specified,
   * outputs to stdout.
   *
   * @param args command-line arguments: [0] input Parquet file path, [1] optional output JSON file path
   * @throws IOException if an error occurs during file conversion
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
