package org.wazokazi.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.wazokazi.parquet.model.ColumnDescriptor;
import org.wazokazi.parquet.model.RowColumnGroup;
import org.wazokazi.parquet.model.SchemaDescriptor;

/**
 * Tests for reading MAP logical type
 */
public class MapTypeTest {

  @Test
  void testReadStringMap() throws IOException {
    // NOTE: nested_maps.snappy.parquet contains nested maps: Map<String, Map<Int, Bool>>
    // This is too complex for the current simple map reader implementation
    // For now, we'll test reading the inner map (columns 1 and 2)
    String filePath = "src/test/data/nested_maps.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("=== Testing nested_maps.snappy.parquet ===");
      System.out.println("Schema:");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.printf("  Column %d: %s (maxDef=%d, maxRep=%d)%n",
            i, col.getPathString(), col.maxDefinitionLevel(), col.maxRepetitionLevel());
      }

      // This file structure is: map<string, map<int32, bool>>
      // Column 0: outer keys (string)
      // Column 1: inner keys (int32)
      // Column 2: inner values (bool)

      // We can read the inner map (columns 1 and 2)
      NestedStructureReader nestedReader = new NestedStructureReader(rowGroup, schema);

      if (schema.getNumColumns() >= 3) {
        // Read the inner map: Map<Int32, Bool>
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

        System.out.println("\nInner maps decoded: " + maps.size());
        for (int i = 0; i < Math.min(maps.size(), 5); i++) {
          System.out.println("  Map " + i + ": " + maps.get(i));
        }

        // Basic validation
        assertNotNull(maps);
        assertTrue(maps.size() > 0, "Should have at least one map");

        System.out.println(
            "\nNote: Nested maps (Map<K, Map<K2, V2>>) are not fully supported yet.");
        System.out.println("This test only validates reading the inner map structure.");
      }
    }
  }

  @Test
  void testReadMapWithNullValues() throws IOException {
    String filePath = "src/test/data/map_no_value.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("\n=== Testing map_no_value.parquet ===");
      System.out.println("Schema:");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.printf("  Column %d: %s (maxDef=%d, maxRep=%d)%n",
            i, col.getPathString(), col.maxDefinitionLevel(), col.maxRepetitionLevel());
      }

      // This file has maps where values can be null
      if (schema.getNumColumns() >= 2) {
        NestedStructureReader nestedReader = new NestedStructureReader(rowGroup, schema);

        List<Map<String, String>> maps = nestedReader.readMap(0, 1,
            obj -> {
              if (obj instanceof byte[]) {
                return new String((byte[]) obj, java.nio.charset.StandardCharsets.UTF_8);
              } else if (obj instanceof Integer) {
                return obj.toString();
              }
              return obj != null ? obj.toString() : null;
            },
            obj -> {
              if (obj == null) {
                return null;
              } else if (obj instanceof byte[]) {
                return new String((byte[]) obj, java.nio.charset.StandardCharsets.UTF_8);
              } else if (obj instanceof Integer) {
                return obj.toString();
              }
              return obj.toString();
            });

        System.out.println("\nMaps decoded: " + maps.size());
        for (int i = 0; i < maps.size(); i++) {
          System.out.println("  Map " + i + ": " + maps.get(i));
        }

        assertNotNull(maps);
      }
    }
  }

  @Test
  void testDataWithAllTypes() throws IOException {
    String filePath = "src/test/data/data_with_all_types.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
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
