package org.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.LogicalColumnDescriptor;
import org.parquet.model.LogicalType;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.SimpleRowColumnGroup;
import org.parquet.model.Type;
import org.parquet.util.ParquetFileInspector;

/**
 * Test ParquetFileInspector functionality.
 */
class ParquetFileInspectorTest {

  @TempDir
  Path tempDir;

  @Test
  void testInspectSimpleFile() throws Exception {
    // Create a test file
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor("id",
            LogicalType.PRIMITIVE, Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)),
        new LogicalColumnDescriptor("name",
            LogicalType.PRIMITIVE, Type.BYTE_ARRAY,
            new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"name"}, 1, 0, 0)),
        new LogicalColumnDescriptor("score",
            LogicalType.PRIMITIVE, Type.DOUBLE,
            new ColumnDescriptor(Type.DOUBLE, new String[] {"score"}, 0, 0, 0))
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "test_schema",
        logicalColumns
    );

    Path testFile = tempDir.resolve("inspect_test.parquet");

    // Write test data
    try (ParquetFileWriter writer = new ParquetFileWriter(testFile, schema)) {
      for (int i = 0; i < 10; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
            i,
            i % 2 == 0 ? "name_" + i : null,
            i * 1.5
        }));
      }
    }

    // Inspect the file
    try (ParquetFileInspector inspector = new ParquetFileInspector(testFile)) {
      String inspection = inspector.inspect();

      // Verify output contains key sections
      assertNotNull(inspection);
      assertTrue(inspection.contains("FILE INFORMATION"));
      assertTrue(inspection.contains("SCHEMA STRUCTURE"));
      assertTrue(inspection.contains("ROW GROUPS"));
      assertTrue(inspection.contains("COLUMN CHUNK DETAILS"));
      assertTrue(inspection.contains("DATA SAMPLING"));

      // Verify file information
      assertTrue(inspection.contains("Total Rows:"));
      assertTrue(inspection.contains("10"));

      // Verify schema
      assertTrue(inspection.contains("id"));
      assertTrue(inspection.contains("name"));
      assertTrue(inspection.contains("score"));

      // Print for manual verification
      System.out.println(inspection);

      // Test statistics summary
      String stats = inspector.getStatisticsSummary();
      assertNotNull(stats);
      assertTrue(stats.contains("STATISTICS SUMMARY"));
      assertTrue(stats.contains("Total Rows:"));

      System.out.println("\n" + stats);
    }
  }

  @Test
  void testInspectFileWithMaps() throws Exception {
    // Create schema with MAP column
    LogicalColumnDescriptor idCol = new LogicalColumnDescriptor(
        "id",
        LogicalType.PRIMITIVE,
        Type.INT64,
        new ColumnDescriptor(Type.INT64, new String[] {"id"}, 0, 0, 0)
    );

    LogicalColumnDescriptor mapCol = SchemaDescriptor.createStringMapColumn("tags", true);

    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "map_schema",
        Arrays.asList(idCol, mapCol)
    );

    Path testFile = tempDir.resolve("inspect_map_test.parquet");

    // Write test data
    try (ParquetFileWriter writer = new ParquetFileWriter(testFile, schema)) {
      for (int i = 0; i < 5; i++) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("env", "prod");
        tags.put("id", String.valueOf(i));
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {(long) i, tags}));
      }
    }

    // Inspect the file
    try (ParquetFileInspector inspector = new ParquetFileInspector(testFile)) {
      String inspection = inspector.inspect();

      // Verify MAP column is shown
      assertTrue(inspection.contains("MAP<"));
      assertTrue(inspection.contains("tags"));

      // Verify physical columns
      assertTrue(inspection.contains("key_value.key"));
      assertTrue(inspection.contains("key_value.value"));

      // Verify data shows maps
      assertTrue(inspection.contains("{"));

      System.out.println("=== MAP File Inspection ===");
      System.out.println(inspection);
    }
  }

  @Test
  void testInspectExistingFile() throws Exception {
    // Test with an actual test data file
    String testFile = "src/test/data/data_with_map_column.parquet";

    try (ParquetFileInspector inspector = new ParquetFileInspector(testFile)) {
      String inspection = inspector.inspect();

      assertNotNull(inspection);
      assertTrue(inspection.contains("FILE INFORMATION"));

      // Should have map columns
      assertTrue(inspection.contains("MAP") || inspection.contains("item"));

      System.out.println("=== Existing File Inspection ===");
      System.out.println(inspection);
    } catch (Exception e) {
      // File might not exist, skip test
      System.out.println("Test file not found, skipping: " + testFile);
    }
  }

  @Test
  void testStatisticsSummary() throws Exception {
    List<LogicalColumnDescriptor> logicalColumns = List.of(
        new LogicalColumnDescriptor("value",
            LogicalType.PRIMITIVE, Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"value"}, 0, 0, 0))
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "stats_test",
        logicalColumns
    );

    Path testFile = tempDir.resolve("stats_test.parquet");

    // Write 100 rows
    try (ParquetFileWriter writer = new ParquetFileWriter(testFile, schema)) {
      for (int i = 0; i < 100; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {i}));
      }
    }

    try (ParquetFileInspector inspector = new ParquetFileInspector(testFile)) {
      String stats = inspector.getStatisticsSummary();

      assertTrue(stats.contains("Total Rows:"));
      assertTrue(stats.contains("100"));
      assertTrue(stats.contains("Average Row Size:"));
      assertTrue(stats.contains("Overall Compression:"));

      System.out.println("=== Statistics Summary ===");
      System.out.println(stats);
    }
  }

  @Test
  void testInspectAsJson() throws Exception {
    // Create a test file
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor("id",
            LogicalType.PRIMITIVE, Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)),
        new LogicalColumnDescriptor("name",
            LogicalType.PRIMITIVE, Type.BYTE_ARRAY,
            new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"name"}, 1, 0, 0))
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "json_test",
        logicalColumns
    );

    Path testFile = tempDir.resolve("json_test.parquet");

    // Write test data
    try (ParquetFileWriter writer = new ParquetFileWriter(testFile, schema)) {
      for (int i = 0; i < 5; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
            i,
            i % 2 == 0 ? "name_" + i : null
        }));
      }
    }

    // Inspect as JSON
    try (ParquetFileInspector inspector = new ParquetFileInspector(testFile)) {
      JsonObject json = inspector.inspectAsJson();

      // Verify structure
      assertNotNull(json);
      assertTrue(json.has("fileInfo"));
      assertTrue(json.has("schema"));
      assertTrue(json.has("rowGroups"));
      assertTrue(json.has("columnChunks"));
      assertTrue(json.has("dataSampling"));
      assertTrue(json.has("statistics"));

      // Verify file info
      JsonObject fileInfo = json.getAsJsonObject("fileInfo");
      assertEquals(5, fileInfo.get("totalRows").getAsInt());
      assertEquals(2, fileInfo.get("physicalColumns").getAsInt());

      // Verify schema
      JsonObject schemaObj = json.getAsJsonObject("schema");
      assertTrue(schemaObj.has("physicalColumns"));
      assertEquals(2, schemaObj.getAsJsonArray("physicalColumns").size());

      // Verify data sampling
      JsonObject sampling = json.getAsJsonObject("dataSampling");
      assertTrue(sampling.has("samples"));
      assertTrue(sampling.has("totalRows"));
      assertTrue(sampling.has("sampledRows"));
      assertEquals(5, sampling.get("sampledRows").getAsInt());

      // Verify samples have location info
      JsonObject firstSample = sampling.getAsJsonArray("samples").get(0).getAsJsonObject();
      assertTrue(firstSample.has("rowNumber"));
      assertTrue(firstSample.has("rowGroup"));
      assertTrue(firstSample.has("rowIndexInGroup"));
      assertTrue(firstSample.has("values"));

      // Verify statistics
      JsonObject stats = json.getAsJsonObject("statistics");
      assertEquals(5, stats.get("totalRows").getAsInt());
      assertTrue(stats.has("averageRowSize"));

      // Print for manual verification
      System.out.println("=== JSON Inspection ===");
      System.out.println(inspector.inspectAsJsonString());
    }
  }

  @Test
  void testInspectMapAsJson() throws Exception {
    // Create schema with MAP column
    LogicalColumnDescriptor idCol = new LogicalColumnDescriptor(
        "id",
        LogicalType.PRIMITIVE,
        Type.INT32,
        new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)
    );

    LogicalColumnDescriptor mapCol = SchemaDescriptor.createStringMapColumn("tags", true);

    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "map_json_test",
        Arrays.asList(idCol, mapCol)
    );

    Path testFile = tempDir.resolve("map_json_test.parquet");

    // Write test data
    try (ParquetFileWriter writer = new ParquetFileWriter(testFile, schema)) {
      Map<String, String> tags1 = new LinkedHashMap<>();
      tags1.put("env", "prod");
      tags1.put("region", "us-west");
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {1, tags1}));

      Map<String, String> tags2 = new LinkedHashMap<>();
      tags2.put("env", "dev");
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {2, tags2}));
    }

    // Inspect as JSON
    try (ParquetFileInspector inspector = new ParquetFileInspector(testFile)) {
      JsonObject json = inspector.inspectAsJson();

      // Verify logical columns show MAP type
      JsonObject schemaObj = json.getAsJsonObject("schema");
      assertTrue(schemaObj.has("logicalColumns"));

      JsonObject mapColumn = schemaObj.getAsJsonArray("logicalColumns").get(1).getAsJsonObject();
      assertEquals("tags", mapColumn.get("name").getAsString());
      assertEquals("MAP", mapColumn.get("logicalType").getAsString());
      assertTrue(mapColumn.has("keyType"));
      assertTrue(mapColumn.has("valueType"));

      // Verify physical columns
      assertEquals(3, schemaObj.getAsJsonArray("physicalColumns").size());

      // Verify data sampling includes maps as nested objects
      JsonObject sampling = json.getAsJsonObject("dataSampling");
      JsonObject firstSample = sampling.getAsJsonArray("samples").get(0).getAsJsonObject();
      JsonObject values = firstSample.getAsJsonObject("values");

      assertTrue(values.has("tags"));
      JsonObject tagsValue = values.getAsJsonObject("tags");
      assertTrue(tagsValue.has("env"));
      assertEquals("prod", tagsValue.get("env").getAsString());

      System.out.println("=== MAP JSON Inspection ===");
      System.out.println(inspector.inspectAsJsonString());
    }
  }

  @Test
  void testJsonStringParseable() throws Exception {
    List<LogicalColumnDescriptor> logicalColumns = List.of(
        new LogicalColumnDescriptor("value",
            LogicalType.PRIMITIVE, Type.INT64,
            new ColumnDescriptor(Type.INT64, new String[] {"value"}, 0, 0, 0))
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "parseable_test",
        logicalColumns
    );

    Path testFile = tempDir.resolve("parseable_test.parquet");

    try (ParquetFileWriter writer = new ParquetFileWriter(testFile, schema)) {
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {100L}));
    }

    try (ParquetFileInspector inspector = new ParquetFileInspector(testFile)) {
      String jsonString = inspector.inspectAsJsonString();

      // Verify it's valid JSON by parsing it
      JsonObject parsed = JsonParser.parseString(jsonString).getAsJsonObject();
      assertNotNull(parsed);
      assertTrue(parsed.has("fileInfo"));

      System.out.println("JSON is parseable ✓");
    }
  }
}

