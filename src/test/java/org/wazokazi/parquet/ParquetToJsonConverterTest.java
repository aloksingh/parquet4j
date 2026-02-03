package org.wazokazi.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import org.wazokazi.parquet.util.ParquetToJsonConverter;

/**
 * Test for ParquetToJsonConverter
 */
public class ParquetToJsonConverterTest {

  @Test
  void testConvertParquetToJson() throws IOException {
    // Use a test parquet file
    String testParquetFile = "src/test/data/alltypes_plain.parquet";
    File parquetFile = new File(testParquetFile);

    // Skip test if file doesn't exist
    if (!parquetFile.exists()) {
      System.out.println("Test file not found: " + testParquetFile + " - skipping test");
      return;
    }

    String outputJsonFile = "target/alltypes_plain.json";

    // Convert the file
    ParquetToJsonConverter converter = new ParquetToJsonConverter();
    converter.convert(testParquetFile, new FileWriter(outputJsonFile));

    // Verify output file was created
    File jsonFile = new File(outputJsonFile);
    assertTrue(jsonFile.exists(), "JSON file should be created");
    assertTrue(jsonFile.length() > 0, "JSON file should not be empty");

    System.out.println("Successfully created JSON file: " + outputJsonFile);
    System.out.println("File size: " + jsonFile.length() + " bytes");

    // Clean up
    Files.deleteIfExists(Paths.get(outputJsonFile));
  }

  @Test
  void testConvertToJsonObject() throws IOException {
    // Use a test parquet file
    String testParquetFile = "src/test/data/alltypes_plain.parquet";
    File parquetFile = new File(testParquetFile);

    // Skip test if file doesn't exist
    if (!parquetFile.exists()) {
      System.out.println("Test file not found: " + testParquetFile + " - skipping test");
      return;
    }

    // Convert to JSON object
    ParquetToJsonConverter converter = new ParquetToJsonConverter();
    JsonObject result = converter.convertToJson(testParquetFile);

    // Verify structure
    assertNotNull(result, "Result should not be null");
    assertTrue(result.has("metadata"), "Result should have metadata property");
    assertTrue(result.has("rows"), "Result should have rows property");

    // Verify metadata structure
    JsonObject metadata = result.getAsJsonObject("metadata");
    assertTrue(metadata.has("version"), "Metadata should have version");
    assertTrue(metadata.has("numRows"), "Metadata should have numRows");
    assertTrue(metadata.has("numRowGroups"), "Metadata should have numRowGroups");
    assertTrue(metadata.has("numColumns"), "Metadata should have numColumns");
    assertTrue(metadata.has("columns"), "Metadata should have columns array");
    assertTrue(metadata.has("rowGroups"), "Metadata should have rowGroups array");

    // Verify columns structure
    JsonArray columns = metadata.getAsJsonArray("columns");
    assertTrue(columns.size() > 0, "Should have at least one column");

    for (int i = 0; i < columns.size(); i++) {
      JsonObject column = columns.get(i).getAsJsonObject();
      assertTrue(column.has("name"), "Column should have name");
      assertTrue(column.has("type"), "Column should have type");
    }

    // Verify rows structure
    JsonArray rows = result.getAsJsonArray("rows");
    assertNotNull(rows, "Rows array should not be null");

    // Verify row count matches metadata
    long expectedRows = metadata.get("numRows").getAsLong();
    assertEquals(expectedRows, rows.size(), "Row count should match metadata");

    System.out.println("\n=== Conversion Results ===");
    System.out.println("Parquet Version: " + metadata.get("version").getAsInt());
    System.out.println("Number of Rows: " + metadata.get("numRows").getAsLong());
    System.out.println("Number of Row Groups: " + metadata.get("numRowGroups").getAsInt());
    System.out.println("Number of Columns: " + metadata.get("numColumns").getAsInt());
    System.out.println("\nColumns:");
    for (int i = 0; i < columns.size(); i++) {
      JsonObject column = columns.get(i).getAsJsonObject();
      System.out.println("  - " + column.get("name").getAsString() +
          " (" + column.get("type").getAsString() + ")");
    }

    if (rows.size() > 0) {
      System.out.println("\nFirst row sample:");
      System.out.println(rows.get(0).toString());
    }
  }
}
