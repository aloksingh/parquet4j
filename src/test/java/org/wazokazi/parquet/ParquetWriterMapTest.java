package org.wazokazi.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wazokazi.parquet.model.ColumnDescriptor;
import org.wazokazi.parquet.model.LogicalColumnDescriptor;
import org.wazokazi.parquet.model.LogicalType;
import org.wazokazi.parquet.model.ParquetMetadata;
import org.wazokazi.parquet.model.RowColumnGroup;
import org.wazokazi.parquet.model.SchemaDescriptor;
import org.wazokazi.parquet.model.SimpleRowColumnGroup;
import org.wazokazi.parquet.model.Type;

/**
 * Test suite for writing MAP logical type columns with ParquetFileWriter.
 */
class ParquetWriterMapTest {

  @TempDir
  Path tempDir;

  @Test
  void testWriteSimpleStringMap() throws Exception {
    // Create schema with a map column using logical columns only
    LogicalColumnDescriptor idCol = new LogicalColumnDescriptor(
        "id",
        LogicalType.PRIMITIVE,
        Type.INT64,
        new ColumnDescriptor(Type.INT64, new String[] {"id"}, 0, 0, 0)
    );

    LogicalColumnDescriptor mapCol = SchemaDescriptor.createStringMapColumn("item", true);

    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "test_schema",
        Arrays.asList(idCol, mapCol)
    );

    Path outputFile = tempDir.resolve("simple_map.parquet");

    // Write data
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      // Row 0: id=1, item={key1: value1, key2: value2}
      Map<String, String> map1 = new LinkedHashMap<>();
      map1.put("key1", "value1");
      map1.put("key2", "value2");
      writer.addRow(createMapRow(schema, 1L, map1));

      // Row 1: id=2, item={key3: value3}
      Map<String, String> map2 = new LinkedHashMap<>();
      map2.put("key3", "value3");
      writer.addRow(createMapRow(schema, 2L, map2));

      // Row 2: id=3, item={}
      writer.addRow(createMapRow(schema, 3L, new LinkedHashMap<>()));

      // Row 3: id=4, item=null
      writer.addRow(createMapRow(schema, 4L, null));
    }

    // Verify file was created
    assertTrue(outputFile.toFile().exists());
    assertTrue(outputFile.toFile().length() > 100);

    System.out.println("Simple map test successful! File size: " + outputFile.toFile().length());
  }

  @Test
  void testWriteMapRoundtrip() throws Exception {
    // Create schema using logical columns only
    LogicalColumnDescriptor idCol = new LogicalColumnDescriptor(
        "id",
        LogicalType.PRIMITIVE,
        Type.INT64,
        new ColumnDescriptor(Type.INT64, new String[] {"id"}, 0, 0, 0)
    );

    LogicalColumnDescriptor mapCol = SchemaDescriptor.createStringMapColumn("item", true);

    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "roundtrip_schema",
        Arrays.asList(idCol, mapCol)
    );

    Path outputFile = tempDir.resolve("roundtrip_map.parquet");

    // Write test data
    List<Map<String, String>> expectedMaps = new ArrayList<>();

    Map<String, String> map1 = new LinkedHashMap<>();
    map1.put("a", "apple");
    map1.put("b", "banana");
    expectedMaps.add(map1);

    Map<String, String> map2 = new LinkedHashMap<>();
    map2.put("c", "cherry");
    expectedMaps.add(map2);

    expectedMaps.add(new LinkedHashMap<>());  // Empty map
    expectedMaps.add(null);  // NULL map

    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      for (int i = 0; i < expectedMaps.size(); i++) {
        writer.addRow(createMapRow(schema, (long) i, expectedMaps.get(i)));
      }
    }

    // Read data back
    try (SerializedFileReader reader = new SerializedFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();

      // Verify metadata
      assertEquals(4, metadata.fileMetadata().numRows());

      // Verify schema - we expect 3 physical columns (id, key, value)
      SchemaDescriptor readSchema = reader.getSchema();
      assertEquals(3, readSchema.getNumColumns(), "Should have 3 physical columns");

      // Verify we have 2 logical columns (id, item map)
      assertEquals(2, readSchema.getNumLogicalColumns(), "Should have 2 logical columns");

      // Read rows
      RowColumnGroupIterator iterator = reader.rowIterator();
      List<Map<String, String>> actualMaps = new ArrayList<>();

      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        Object mapValue = row.getColumnValue("item");

        if (mapValue == null) {
          actualMaps.add(null);
        } else if (mapValue instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, String> map = (Map<String, String>) mapValue;
          actualMaps.add(map);
        } else {
          fail("Expected Map or null, got: " + mapValue.getClass());
        }
      }

      // Verify maps match
      assertEquals(expectedMaps.size(), actualMaps.size());
      for (int i = 0; i < expectedMaps.size(); i++) {
        Map<String, String> expected = expectedMaps.get(i);
        Map<String, String> actual = actualMaps.get(i);

        if (expected == null) {
          assertNull(actual, "Row " + i + " should be NULL");
        } else if (expected.isEmpty()) {
          assertNotNull(actual, "Row " + i + " should not be NULL");
          assertTrue(actual.isEmpty(), "Row " + i + " should be empty");
        } else {
          assertEquals(expected, actual, "Row " + i + " map mismatch");
        }
      }
    }

    System.out.println("Map round-trip test successful!");
  }

  @Test
  void testWriteMapWithNullValues() throws Exception {
    // Create schema with values that can be NULL using logical columns only
    LogicalColumnDescriptor idCol = new LogicalColumnDescriptor(
        "id",
        LogicalType.PRIMITIVE,
        Type.INT32,
        new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)
    );

    LogicalColumnDescriptor mapCol = SchemaDescriptor.createMapColumn(
        "data",
        Type.BYTE_ARRAY,  // String key
        Type.BYTE_ARRAY,  // String value
        true,   // map itself is optional
        true    // values can be NULL
    );

    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "map_with_nulls",
        Arrays.asList(idCol, mapCol)
    );

    Path outputFile = tempDir.resolve("map_null_values.parquet");

    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      // Map with null value
      Map<String, String> map1 = new LinkedHashMap<>();
      map1.put("key1", "value1");
      map1.put("key2", null);  // NULL value
      map1.put("key3", "value3");

      writer.addRow(createMapRow(schema, 1, map1));
    }

    // Verify file created
    assertTrue(outputFile.toFile().exists());
    System.out.println("Map with NULL values test successful!");
  }

  @Test
  void testWriteLargeMap() throws Exception {
    LogicalColumnDescriptor idCol = new LogicalColumnDescriptor(
        "id",
        LogicalType.PRIMITIVE,
        Type.INT32,
        new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)
    );

    LogicalColumnDescriptor mapCol = SchemaDescriptor.createStringMapColumn("data", false);

    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "large_map",
        Arrays.asList(idCol, mapCol)
    );

    Path outputFile = tempDir.resolve("large_map.parquet");

    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      // Create map with 100 entries
      Map<String, String> largeMap = new LinkedHashMap<>();
      for (int i = 0; i < 100; i++) {
        largeMap.put("key" + i, "value" + i);
      }

      writer.addRow(createMapRow(schema, 1, largeMap));
    }

    assertTrue(outputFile.toFile().exists());
    System.out.println("Large map test successful!");
  }

  @Test
  void testWriteMultipleRowsWithMaps() throws Exception {
    LogicalColumnDescriptor idCol = new LogicalColumnDescriptor(
        "id",
        LogicalType.PRIMITIVE,
        Type.INT32,
        new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)
    );

    LogicalColumnDescriptor mapCol = SchemaDescriptor.createStringMapColumn("tags", true);

    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "multi_row_maps",
        Arrays.asList(idCol, mapCol)
    );

    Path outputFile = tempDir.resolve("multi_row_maps.parquet");

    int numRows = 50;
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      for (int i = 0; i < numRows; i++) {
        Map<String, String> map = new LinkedHashMap<>();

        // Vary the map size
        int mapSize = i % 5;
        for (int j = 0; j < mapSize; j++) {
          map.put("k" + j, "v" + (i * 10 + j));
        }

        // Every 10th row is null
        writer.addRow(createMapRow(schema, i, i % 10 == 0 ? null : map));
      }
    }

    // Read back and verify
    try (SerializedFileReader reader = new SerializedFileReader(outputFile)) {
      assertEquals(numRows, reader.getTotalRowCount());
    }

    System.out.println("Multiple rows with maps test successful!");
  }

  /**
   * Helper to create a row with map data
   */
  private RowColumnGroup createMapRow(SchemaDescriptor schema, long id, Map<String, String> map) {
    return new SimpleRowColumnGroup(schema, new Object[] {id, map});
  }

  /**
   * Helper to create a row with map data (int id version)
   */
  private RowColumnGroup createMapRow(SchemaDescriptor schema, int id, Map<String, String> map) {
    return new SimpleRowColumnGroup(schema, new Object[] {id, map});
  }
}
