package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for reading MAP logical type
 */
public class MapTypeTest {

  @Test
  void testReadStringMap() throws IOException {
    // nested_maps.snappy.parquet stores map<string, map<int32, boolean>> plus scalar
    // columns; columns 0/1/2 are the outer key leaf and the inner map's key/value
    // leaves (nested repetition level 2).
    String filePath = "src/test/data/nested_maps.snappy.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();
      assertTrue(schema.getNumColumns() >= 3, "Expected nested map leaves in the schema");

      // Read the inner map: Map<Int32, Bool>
      NestedStructureReader nestedReader = new NestedStructureReader(rowGroup, schema);
      List<Map<Integer, Boolean>> maps = nestedReader.readMap(1, 2,
          obj -> {
            if (obj instanceof Integer) {
              return (Integer) obj;
            } else if (obj instanceof Long) {
              return ((Long) obj).intValue();
            }
            return Integer.parseInt(obj.toString());
          },
          obj -> {
            if (obj == null) {
              return null;
            } else if (obj instanceof Boolean) {
              return (Boolean) obj;
            }
            return Boolean.parseBoolean(obj.toString());
          });

      // Exact inner maps per row (outer keys a..f), verified with PyArrow/arrow-rs:
      // row 0 {"a": {1: true, 2: false}}, row 1 {"b": {1: true}}, row 2 {"c": null},
      // row 3 {"d": {}}, row 4 {"e": {1: true}}, row 5 {"f": {3: true, 4: false, 5: true}}.
      List<Map<Integer, Boolean>> expected = Arrays.asList(
          Map.of(1, true, 2, false),
          Map.of(1, true),
          null,
          Map.of(),
          Map.of(1, true),
          Map.of(3, true, 4, false, 5, true));
      assertEquals(expected, maps);
    }
  }

  @Test
  void testReadMapWithNullValues() throws IOException {
    // map_no_value.parquet (parquet-testing): a REQUIRED map whose entries all carry
    // null values. my_map holds INT32 keys 1..9 in three rows of three entries and an
    // optional INT32 value that is null for every entry.
    String filePath = "src/test/data/map_no_value.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();
      assertTrue(schema.getNumColumns() >= 2, "Expected map key and value leaves");

      NestedStructureReader nestedReader = new NestedStructureReader(rowGroup, schema);
      List<Map<Integer, Integer>> maps = nestedReader.readMap(0, 1,
          obj -> (Integer) obj,
          obj -> (Integer) obj);

      // Exact maps per row: every entry keeps its key and a null value entry.
      assertEquals(3, maps.size(), "Should have 3 rows");
      int[][] keysPerRow = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
      for (int row = 0; row < keysPerRow.length; row++) {
        Map<Integer, Integer> map = maps.get(row);
        assertNotNull(map, "Row " + row + " should hold a map");
        assertEquals(keysPerRow[row].length, map.size(), "Row " + row + " entry count");
        for (int key : keysPerRow[row]) {
          assertTrue(map.containsKey(key), "Row " + row + " must keep key " + key);
          assertNull(map.get(key),
              "Row " + row + " value for key " + key + " must stay a null entry");
        }
      }
    }
  }

  @Test
  void testDataWithAllTypes() throws IOException {
    String filePath = "src/test/data/data_with_all_types.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Expected row count
      assertEquals(1000L, reader.getTotalRowCount());

      // Expected columns
      SchemaDescriptor schema = reader.getSchema();
      assertEquals(9, schema.getNumLogicalColumns());
      assertEquals("id", schema.getColumn(0).getPathString());
      assertEquals("long_type", schema.getColumn(1).getPathString());
      assertEquals("string_type", schema.getColumn(2).getPathString());
      assertEquals("float32_type", schema.getColumn(3).getPathString());
      assertEquals("float64_type", schema.getColumn(4).getPathString());
      assertEquals("double_type", schema.getColumn(5).getPathString());
      assertEquals("map_string_string.key_value.key", schema.getColumn(6).getPathString());
      assertEquals("map_string_string.key_value.value", schema.getColumn(7).getPathString());
      assertEquals("map_string_int64.key_value.key", schema.getColumn(8).getPathString());
      assertEquals("map_string_int64.key_value.value", schema.getColumn(9).getPathString());
      assertEquals("map_string_double.key_value.key", schema.getColumn(10).getPathString());
      assertEquals("map_string_double.key_value.value", schema.getColumn(11).getPathString());

      // Verify first row values
      assertTrue(iterator.hasNext());
      RowColumnGroup firstRow = iterator.next();
      assertEquals(1L, firstRow.getColumnValue(0));
      assertEquals(1000L, firstRow.getColumnValue(1));
      assertEquals("2104eb0a-3478-478c-b6bc-943170d4723e", firstRow.getColumnValue(2));
      assertEquals(10.239F, firstRow.getColumnValue(3));
      assertEquals(3.199843746185117D, firstRow.getColumnValue(4));
      assertEquals(100.00119499285996D, firstRow.getColumnValue(5));
      assertEquals("{key1=value1, key2=value2}", firstRow.getColumnValue(6).toString());
      assertEquals("{key1=1, key2=1000}", firstRow.getColumnValue(7).toString());
      assertEquals("{key1=31.626555297724096, key2=34.644465647488346}",
          firstRow.getColumnValue(8).toString());
    }
  }
}
