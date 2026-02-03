package io.github.aloksingh.parquet.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import io.github.aloksingh.parquet.RowColumnGroupIterator;
import io.github.aloksingh.parquet.SerializedFileReader;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;

/**
 * Tests for RowColumnGroupIterator with all data types
 * <p>
 * Test file: data_with_all_types.parquet
 * Schema:
 * id: int64
 * long_type: int64
 * string_type: string
 * float32_type: float
 * float64_type: double
 * double_type: double
 * map_string_string: map<string, string>
 * map_string_int64: map<string, int64>
 * map_string_double: map<string, double>
 * <p>
 * Sample data (from PyArrow):
 * Row 0: id=1, long_type=1000, string_type="2104eb0a-3478-478c-b6bc-943170d4723e",
 * float32_type=10.239, float64_type=3.199844, double_type=100.001195,
 * map_string_string={"key1": "value1", "key2": "value2"},
 * map_string_int64={"key1": 1, "key2": 1000},
 * map_string_double={"key1": 31.626555, "key2": 34.644466}
 * Row 1: id=2, long_type=2000, string_type="ce3e9ec0-5bb2-43c6-9cf1-03b745cbe80d",
 * float32_type=20.478, float64_type=4.525262, double_type=141.423046,
 * map_string_string={"key1": "value2", "key2": "value3"},
 * map_string_int64={"key1": 2, "key2": 2000},
 * map_string_double={"key1": 44.726703, "key2": 48.994673}
 */
public class RowColumnGroupIteratorAllTypesTest {

  private static final String TEST_DATA_DIR = "src/test/data/";
  private static final String TEST_FILE = "data_with_all_types.parquet";

  @Test
  void testAllTypesSchemaDetection() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();

      // Should have 9 logical columns
      assertEquals(9, schema.getNumLogicalColumns());

      // Verify column names and types
      LogicalColumnDescriptor col0 = schema.getLogicalColumn(0);
      assertEquals("id", col0.getName());
      assertTrue(col0.isPrimitive());

      LogicalColumnDescriptor col1 = schema.getLogicalColumn(1);
      assertEquals("long_type", col1.getName());
      assertTrue(col1.isPrimitive());

      LogicalColumnDescriptor col2 = schema.getLogicalColumn(2);
      assertEquals("string_type", col2.getName());
      assertTrue(col2.isPrimitive());

      LogicalColumnDescriptor col3 = schema.getLogicalColumn(3);
      assertEquals("float32_type", col3.getName());
      assertTrue(col3.isPrimitive());

      LogicalColumnDescriptor col4 = schema.getLogicalColumn(4);
      assertEquals("float64_type", col4.getName());
      assertTrue(col4.isPrimitive());

      LogicalColumnDescriptor col5 = schema.getLogicalColumn(5);
      assertEquals("double_type", col5.getName());
      assertTrue(col5.isPrimitive());

      LogicalColumnDescriptor col6 = schema.getLogicalColumn(6);
      assertEquals("map_string_string", col6.getName());
      assertTrue(col6.isMap());

      LogicalColumnDescriptor col7 = schema.getLogicalColumn(7);
      assertEquals("map_string_int64", col7.getName());
      assertTrue(col7.isMap());

      LogicalColumnDescriptor col8 = schema.getLogicalColumn(8);
      assertEquals("map_string_double", col8.getName());
      assertTrue(col8.isMap());
    }
  }

  @Test
  void testAllTypesBasicIteration() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Should have 1000 total rows
      assertEquals(1000L, reader.getTotalRowCount());

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);

        // Should have 9 logical columns
        assertEquals(9, row.getColumnCount());

        rowCount++;
      }

      assertEquals(1000, rowCount);
    }
  }

  @Test
  void testAllTypesKnownValues() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Row 0
      assertTrue(iterator.hasNext());
      RowColumnGroup row0 = iterator.next();

      assertEquals(1L, row0.getColumnValue("id"));
      assertEquals(1000L, row0.getColumnValue("long_type"));
      assertEquals("2104eb0a-3478-478c-b6bc-943170d4723e", row0.getColumnValue("string_type"));
      assertEquals(10.239f, (Float) row0.getColumnValue("float32_type"), 0.001f);
      assertEquals(3.199843746185117, (Double) row0.getColumnValue("float64_type"), 0.000001);
      assertEquals(100.00119499285996, (Double) row0.getColumnValue("double_type"), 0.000001);

      @SuppressWarnings("unchecked")
      Map<String, String> mapStr0 = (Map<String, String>) row0.getColumnValue("map_string_string");
      assertNotNull(mapStr0);
      assertEquals(2, mapStr0.size());
      assertEquals("value1", mapStr0.get("key1"));
      assertEquals("value2", mapStr0.get("key2"));

      @SuppressWarnings("unchecked")
      Map<String, Object> mapInt0 = (Map<String, Object>) row0.getColumnValue("map_string_int64");
      assertNotNull(mapInt0);
      assertEquals(2, mapInt0.size());
      assertEquals(1L, mapInt0.get("key1"));
      assertEquals(1000L, mapInt0.get("key2"));

      @SuppressWarnings("unchecked")
      Map<String, Object> mapDbl0 = (Map<String, Object>) row0.getColumnValue("map_string_double");
      assertNotNull(mapDbl0);
      assertEquals(2, mapDbl0.size());
      assertEquals(31.626555297724096, (Double) mapDbl0.get("key1"), 0.000001);
      assertEquals(34.644465647488346, (Double) mapDbl0.get("key2"), 0.000001);

      // Row 1
      assertTrue(iterator.hasNext());
      RowColumnGroup row1 = iterator.next();

      assertEquals(2L, row1.getColumnValue("id"));
      assertEquals(2000L, row1.getColumnValue("long_type"));
      assertEquals("ce3e9ec0-5bb2-43c6-9cf1-03b745cbe80d", row1.getColumnValue("string_type"));
      assertEquals(20.478f, (Float) row1.getColumnValue("float32_type"), 0.001f);
      assertEquals(4.525262423329724, (Double) row1.getColumnValue("float64_type"), 0.000001);
      assertEquals(141.423046212419, (Double) row1.getColumnValue("double_type"), 0.000001);

      @SuppressWarnings("unchecked")
      Map<String, String> mapStr1 = (Map<String, String>) row1.getColumnValue("map_string_string");
      assertNotNull(mapStr1);
      assertEquals(2, mapStr1.size());
      assertEquals("value2", mapStr1.get("key1"));
      assertEquals("value3", mapStr1.get("key2"));

      @SuppressWarnings("unchecked")
      Map<String, Object> mapInt1 = (Map<String, Object>) row1.getColumnValue("map_string_int64");
      assertNotNull(mapInt1);
      assertEquals(2, mapInt1.size());
      assertEquals(2L, mapInt1.get("key1"));
      assertEquals(2000L, mapInt1.get("key2"));

      @SuppressWarnings("unchecked")
      Map<String, Object> mapDbl1 = (Map<String, Object>) row1.getColumnValue("map_string_double");
      assertNotNull(mapDbl1);
      assertEquals(2, mapDbl1.size());
      assertEquals(44.72670343318408, (Double) mapDbl1.get("key1"), 0.000001);
      assertEquals(48.99467317984681, (Double) mapDbl1.get("key2"), 0.000001);

      // Row 2
      assertTrue(iterator.hasNext());
      RowColumnGroup row2 = iterator.next();

      assertEquals(3L, row2.getColumnValue("id"));
      assertEquals(3000L, row2.getColumnValue("long_type"));
      assertEquals("431da083-2d97-4219-966d-0e95d8a2b288", row2.getColumnValue("string_type"));
      assertEquals(30.717f, (Float) row2.getColumnValue("float32_type"), 0.001f);
      assertEquals(5.542291944674153, (Double) row2.getColumnValue("float64_type"), 0.000001);
      assertEquals(173.20715054523586, (Double) row2.getColumnValue("double_type"), 0.000001);

      @SuppressWarnings("unchecked")
      Map<String, String> mapStr2 = (Map<String, String>) row2.getColumnValue("map_string_string");
      assertNotNull(mapStr2);
      assertEquals(2, mapStr2.size());
      assertEquals("value3", mapStr2.get("key1"));
      assertEquals("value4", mapStr2.get("key2"));

      @SuppressWarnings("unchecked")
      Map<String, Object> mapInt2 = (Map<String, Object>) row2.getColumnValue("map_string_int64");
      assertNotNull(mapInt2);
      assertEquals(2, mapInt2.size());
      assertEquals(3L, mapInt2.get("key1"));
      assertEquals(3000L, mapInt2.get("key2"));

      @SuppressWarnings("unchecked")
      Map<String, Object> mapDbl2 = (Map<String, Object>) row2.getColumnValue("map_string_double");
      assertNotNull(mapDbl2);
      assertEquals(2, mapDbl2.size());
      assertEquals(54.77880064404477, (Double) mapDbl2.get("key1"), 0.000001);
      assertEquals(60.00597470252441, (Double) mapDbl2.get("key2"), 0.000001);
    }
  }

  @Test
  void testAllTypesAccessByIndex() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup row = iterator.next();

      // Access by index
      Long id = (Long) row.getColumnValue(0);
      Long longType = (Long) row.getColumnValue(1);
      String stringType = (String) row.getColumnValue(2);
      Float float32 = (Float) row.getColumnValue(3);
      Double float64 = (Double) row.getColumnValue(4);
      Double doubleType = (Double) row.getColumnValue(5);
      @SuppressWarnings("unchecked")
      Map<String, Object> mapStr = (Map<String, Object>) row.getColumnValue(6);
      @SuppressWarnings("unchecked")
      Map<String, Object> mapInt = (Map<String, Object>) row.getColumnValue(7);
      @SuppressWarnings("unchecked")
      Map<String, Object> mapDbl = (Map<String, Object>) row.getColumnValue(8);

      assertEquals(1L, id);
      assertEquals(1000L, longType);
      assertEquals("2104eb0a-3478-478c-b6bc-943170d4723e", stringType);
      assertNotNull(float32);
      assertNotNull(float64);
      assertNotNull(doubleType);
      assertNotNull(mapStr);
      assertNotNull(mapInt);
      assertNotNull(mapDbl);
    }
  }

  @Test
  void testAllTypesToString() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup row = iterator.next();

      String rowString = row.toString();
      assertNotNull(rowString);
      assertTrue(rowString.contains("id=1"));
      assertTrue(rowString.contains("long_type=1000"));
      assertTrue(rowString.contains("string_type=2104eb0a-3478-478c-b6bc-943170d4723e"));
      assertTrue(rowString.contains("map_string_string={"));
      assertTrue(rowString.contains("key1"));
    }
  }

  @Test
  void testAllTypesAllRowsValid() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();

        // Verify all rows have valid id
        Long id = (Long) row.getColumnValue("id");
        assertNotNull(id);
        assertTrue(id >= 1 && id <= 1000);

        // Verify all rows have valid long_type
        Long longType = (Long) row.getColumnValue("long_type");
        assertNotNull(longType);
        assertEquals(id * 1000, longType);

        // Verify all rows have valid string_type (UUID format)
        String stringType = (String) row.getColumnValue("string_type");
        assertNotNull(stringType);
        assertTrue(stringType.length() > 0);

        // Verify all rows have valid float values
        assertNotNull(row.getColumnValue("float32_type"));
        assertNotNull(row.getColumnValue("float64_type"));
        assertNotNull(row.getColumnValue("double_type"));

        // Verify all rows have valid maps
        @SuppressWarnings("unchecked")
        Map<String, Object> mapStr = (Map<String, Object>) row.getColumnValue("map_string_string");
        assertNotNull(mapStr);
        assertEquals(2, mapStr.size());

        @SuppressWarnings("unchecked")
        Map<String, Object> mapInt = (Map<String, Object>) row.getColumnValue("map_string_int64");
        assertNotNull(mapInt);
        assertEquals(2, mapInt.size());

        @SuppressWarnings("unchecked")
        Map<String, Object> mapDbl = (Map<String, Object>) row.getColumnValue("map_string_double");
        assertNotNull(mapDbl);
        assertEquals(2, mapDbl.size());

        rowCount++;
      }

      assertEquals(1000, rowCount);
    }
  }

  @Test
  void testAllTypesPerformance() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    long startTime = System.currentTimeMillis();

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        // Access all columns to ensure they're decoded
        row.getColumnValue("id");
        row.getColumnValue("long_type");
        row.getColumnValue("string_type");
        row.getColumnValue("float32_type");
        row.getColumnValue("float64_type");
        row.getColumnValue("double_type");
        row.getColumnValue("map_string_string");
        row.getColumnValue("map_string_int64");
        row.getColumnValue("map_string_double");
        rowCount++;
      }

      assertEquals(1000, rowCount);
    }

    long duration = System.currentTimeMillis() - startTime;

    System.out.println("Performance metrics for all types file:");
    System.out.println("  Rows: 1000");
    System.out.println("  Columns: 9 (6 primitives + 3 maps)");
    System.out.println("  Time: " + duration + "ms");

    // Should complete in reasonable time (< 2 seconds for 1000 rows)
    assertTrue(duration < 2000,
        "Reading 1000 rows with all types took too long: " + duration + "ms");
  }

  @Test
  void testMapTypesCorrect() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Check first 10 rows
      int rowCount = 0;
      while (iterator.hasNext() && rowCount < 10) {
        RowColumnGroup row = iterator.next();

        // Verify map_string_string has String keys and String values
        @SuppressWarnings("unchecked")
        Map<String, Object> mapStr = (Map<String, Object>) row.getColumnValue("map_string_string");
        for (Map.Entry<String, Object> entry : mapStr.entrySet()) {
          assertInstanceOf(String.class, entry.getKey());
          assertInstanceOf(String.class, entry.getValue());
        }

        // Verify map_string_int64 has String keys and Long values
        @SuppressWarnings("unchecked")
        Map<String, Object> mapInt = (Map<String, Object>) row.getColumnValue("map_string_int64");
        for (Map.Entry<String, Object> entry : mapInt.entrySet()) {
          assertInstanceOf(String.class, entry.getKey());
          assertInstanceOf(Long.class, entry.getValue(),
              "Expected Long but got " + entry.getValue().getClass().getName());
        }

        // Verify map_string_double has String keys and Double values
        @SuppressWarnings("unchecked")
        Map<String, Object> mapDbl = (Map<String, Object>) row.getColumnValue("map_string_double");
        for (Map.Entry<String, Object> entry : mapDbl.entrySet()) {
          assertInstanceOf(String.class, entry.getKey());
          assertInstanceOf(Double.class, entry.getValue(),
              "Expected Double but got " + entry.getValue().getClass().getName());
        }

        rowCount++;
      }
    }
  }

  @Test
  void testNumericTypePrecision() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup row = iterator.next();

      // Test that numeric types maintain their precision
      Float float32 = (Float) row.getColumnValue("float32_type");
      Double float64 = (Double) row.getColumnValue("float64_type");
      Double doubleType = (Double) row.getColumnValue("double_type");

      // float32 should be a Float object
      assertInstanceOf(Float.class, float32);

      // float64 and double_type should be Double objects
      assertInstanceOf(Double.class, float64);
      assertInstanceOf(Double.class, doubleType);

      // Verify values are in expected ranges
      assertTrue(float32 > 0);
      assertTrue(float64 > 0);
      assertTrue(doubleType > 0);
    }
  }

  @Test
  void testMapValueConsistency() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Check first few rows to verify map value patterns
      for (int i = 0; i < 5 && iterator.hasNext(); i++) {
        RowColumnGroup row = iterator.next();
        Long id = (Long) row.getColumnValue("id");

        @SuppressWarnings("unchecked")
        Map<String, Object> mapInt = (Map<String, Object>) row.getColumnValue("map_string_int64");

        // Verify the pattern: key1 -> id, key2 -> id*1000
        assertEquals(id, mapInt.get("key1"));
        assertEquals(id * 1000, mapInt.get("key2"));

        @SuppressWarnings("unchecked")
        Map<String, Object> mapStr = (Map<String, Object>) row.getColumnValue("map_string_string");

        // Verify the pattern: key1 -> value{id}, key2 -> value{id+1}
        assertEquals("value" + id, mapStr.get("key1"));
        assertEquals("value" + (id + 1), mapStr.get("key2"));
      }
    }
  }
}
