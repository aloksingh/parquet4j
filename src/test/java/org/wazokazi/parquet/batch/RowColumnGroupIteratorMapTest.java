package org.wazokazi.parquet.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.wazokazi.parquet.RowColumnGroupIterator;
import org.wazokazi.parquet.SerializedFileReader;
import org.wazokazi.parquet.model.LogicalColumnDescriptor;
import org.wazokazi.parquet.model.RowColumnGroup;
import org.wazokazi.parquet.model.SchemaDescriptor;

/**
 * Tests for RowColumnGroupIterator with Map<String, String> columns
 * <p>
 * Test file: data_with_map_column.parquet
 * Schema:
 * id: int64
 * item: map<string, string>
 * <p>
 * Sample data (from PyArrow):
 * Row 0: id=1,  item={"key1": "value1", "key2": "value2"}
 * Row 1: id=2,  item={"key3": "value1"}
 * Row 2: id=2,  item={"key1": "value2", "key2": "value3"}
 * Row 3: id=3,  item={"key3": "value2"}
 * Row 4: id=3,  item={"key1": "value3", "key2": "value4"}
 */
public class RowColumnGroupIteratorMapTest {

  private static final String TEST_DATA_DIR = "src/test/data/";
  private static final String TEST_FILE = "data_with_map_column.parquet";

  @Test
  void testMapSchemaDetection() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();

      // Should have 2 logical columns
      assertEquals(2, schema.getNumLogicalColumns());

      // First logical column should be "id" (primitive)
      LogicalColumnDescriptor col0 = schema.getLogicalColumn(0);
      assertEquals("id", col0.getName());
      assertTrue(col0.isPrimitive());

      // Second logical column should be "item" (map)
      LogicalColumnDescriptor col1 = schema.getLogicalColumn(1);
      assertEquals("item", col1.getName());
      assertTrue(col1.isMap());
    }
  }

  @Test
  void testMapColumnBasicIteration() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Should have 2000 total rows
      assertEquals(2000L, reader.getTotalRowCount());

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);

        // Should have 2 logical columns
        assertEquals(2, row.getColumnCount());

        rowCount++;
      }

      assertEquals(2000, rowCount);
    }
  }

  @Test
  void testMapColumnKnownValues() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Row 0: id=1, item={"key1": "value1", "key2": "value2"}
      assertTrue(iterator.hasNext());
      RowColumnGroup row0 = iterator.next();

      assertEquals(1L, row0.getColumnValue("id"));

      @SuppressWarnings("unchecked")
      Map<String, String> map0 = (Map<String, String>) row0.getColumnValue("item");
      assertNotNull(map0);
      assertEquals(2, map0.size());
      assertEquals("value1", map0.get("key1"));
      assertEquals("value2", map0.get("key2"));

      // Row 1: id=2, item={"key3": "value1"}
      assertTrue(iterator.hasNext());
      RowColumnGroup row1 = iterator.next();

      assertEquals(2L, row1.getColumnValue("id"));

      @SuppressWarnings("unchecked")
      Map<String, String> map1 = (Map<String, String>) row1.getColumnValue("item");
      assertNotNull(map1);
      assertEquals(1, map1.size());
      assertEquals("value1", map1.get("key3"));

      // Row 2: id=2, item={"key1": "value2", "key2": "value3"}
      assertTrue(iterator.hasNext());
      RowColumnGroup row2 = iterator.next();

      assertEquals(2L, row2.getColumnValue("id"));

      @SuppressWarnings("unchecked")
      Map<String, String> map2 = (Map<String, String>) row2.getColumnValue("item");
      assertNotNull(map2);
      assertEquals(2, map2.size());
      assertEquals("value2", map2.get("key1"));
      assertEquals("value3", map2.get("key2"));

      // Row 3: id=3, item={"key3": "value2"}
      assertTrue(iterator.hasNext());
      RowColumnGroup row3 = iterator.next();

      assertEquals(3L, row3.getColumnValue("id"));

      @SuppressWarnings("unchecked")
      Map<String, String> map3 = (Map<String, String>) row3.getColumnValue("item");
      assertNotNull(map3);
      assertEquals(1, map3.size());
      assertEquals("value2", map3.get("key3"));

      // Row 4: id=3, item={"key1": "value3", "key2": "value4"}
      assertTrue(iterator.hasNext());
      RowColumnGroup row4 = iterator.next();

      assertEquals(3L, row4.getColumnValue("id"));

      @SuppressWarnings("unchecked")
      Map<String, String> map4 = (Map<String, String>) row4.getColumnValue("item");
      assertNotNull(map4);
      assertEquals(2, map4.size());
      assertEquals("value3", map4.get("key1"));
      assertEquals("value4", map4.get("key2"));
    }
  }

  @Test
  void testMapColumnAccessByIndex() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup row = iterator.next();

      // Access by index
      Long id = (Long) row.getColumnValue(0);
      @SuppressWarnings("unchecked")
      Map<String, String> item = (Map<String, String>) row.getColumnValue(1);

      assertEquals(1L, id);
      assertNotNull(item);
      assertEquals(2, item.size());
    }
  }

  @Test
  void testMapColumnToString() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup row = iterator.next();

      String rowString = row.toString();
      assertNotNull(rowString);
      assertTrue(rowString.contains("id=1"));
      assertTrue(rowString.contains("item={"));
      assertTrue(rowString.contains("key1"));
      assertTrue(rowString.contains("value1"));
    }
  }

  @Test
  void testMapColumnAllRowsValid() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();

        // Verify all rows have valid id
        Long id = (Long) row.getColumnValue("id");
        assertNotNull(id);
        assertTrue(id >= 1 && id <= 1001);

        // Verify all rows have valid map
        @SuppressWarnings("unchecked")
        Map<String, String> item = (Map<String, String>) row.getColumnValue("item");
        assertNotNull(item);
        assertTrue(item.size() > 0);

        rowCount++;
      }

      assertEquals(2000, rowCount);
    }
  }

  @Test
  void testMapColumnPerformance() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    long startTime = System.currentTimeMillis();

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        // Access both columns to ensure they're decoded
        row.getColumnValue("id");
        row.getColumnValue("item");
        rowCount++;
      }

      assertEquals(2000, rowCount);
    }

    long duration = System.currentTimeMillis() - startTime;

    System.out.println("Performance metrics for Map<String, String> file:");
    System.out.println("  Rows: 2000");
    System.out.println("  Time: " + duration + "ms");

    // Should complete in reasonable time (< 2 seconds for 2000 rows)
    assertTrue(duration < 2000,
        "Reading 2000 rows with maps took too long: " + duration + "ms");
  }

  @Test
  void testMapKeysAndValuesAreStrings() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Check first 10 rows
      int rowCount = 0;
      while (iterator.hasNext() && rowCount < 10) {
        RowColumnGroup row = iterator.next();

        @SuppressWarnings("unchecked")
        Map<String, String> item = (Map<String, String>) row.getColumnValue("item");

        // Verify all keys and values are strings
        for (Map.Entry<String, String> entry : item.entrySet()) {
          assertInstanceOf(String.class, entry.getKey());
          assertInstanceOf(String.class, entry.getValue());
        }

        rowCount++;
      }
    }
  }

  @Test
  void testBackwardsCompatibilityWithNonMapFiles() throws IOException {
    // Test that files without maps still work correctly
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);
        assertTrue(row.getColumnCount() > 0);
        rowCount++;
      }

      assertEquals(8, rowCount);
    }
  }
}
