package org.wazokazi.parquet.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.wazokazi.parquet.RowColumnGroupIterator;
import org.wazokazi.parquet.SerializedFileReader;
import org.wazokazi.parquet.model.ColumnDescriptor;
import org.wazokazi.parquet.model.RowColumnGroup;
import org.wazokazi.parquet.model.SchemaDescriptor;
import org.wazokazi.parquet.model.Type;

/**
 * Tests for RowColumnGroupIterator with a larger LZ4-compressed file
 * <p>
 * File: lz4_raw_compressed_larger.parquet
 * Schema: a: string not null
 * Rows: 10,000
 * Content: UUID strings (all unique)
 */
public class RowColumnGroupIteratorLargerFileTest {

  private static final String TEST_DATA_DIR = "src/test/data/";
  private static final String TEST_FILE = "lz4_raw_compressed_larger.parquet";

  @Test
  void testLargerFileBasicIteration() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();
      assertNotNull(iterator);

      // Verify total row count from metadata
      assertEquals(10000L, reader.getTotalRowCount(),
          "File should contain exactly 10,000 rows");

      // Count rows while iterating
      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);

        // Verify single column
        assertEquals(1, row.getColumnCount(),
            "Each row should have exactly 1 column");

        rowCount++;
      }

      // Verify we read all 10,000 rows
      assertEquals(10000, rowCount,
          "Should have iterated through all 10,000 rows");
    }
  }

  @Test
  void testLargerFileSchema() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();

      assertNotNull(schema);
      assertEquals(1, schema.getNumColumns(),
          "Schema should have exactly 1 column");

      ColumnDescriptor col = schema.getColumn(0);
      assertEquals("a", col.getPathString(),
          "Column name should be 'a'");
      assertEquals(Type.BYTE_ARRAY, col.physicalType(),
          "Column type should be BYTE_ARRAY (string)");

      // Verify through iterator
      RowColumnGroupIterator iterator = reader.rowIterator();
      assertTrue(iterator.hasNext());

      RowColumnGroup firstRow = iterator.next();
      assertEquals("a", firstRow.getColumns().get(0).getPathString());
    }
  }

  @Test
  void testLargerFileKnownValues() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    // Expected values from PyArrow inspection
    String[] expectedFirst5 = {
        "c7ce6bef-d5b0-4863-b199-8ea8c7fb117b",
        "e8fb9197-cb9f-4118-b67f-fbfa65f61843",
        "885136e1-0aa1-4fdb-8847-63d87b07c205",
        "ce7b2019-8ebe-4906-a74d-0afa2409e5df",
        "a9ee2527-821b-4b71-a926-03f73c3fc8b7"
    };

    String[] expectedLast5 = {
        "219e000b-70a5-4469-bfb3-c8a92beeb615",  // Row 9995
        "892e54eb-5685-41d3-b6c7-a041aeb96b6a",  // Row 9996
        "cd4f538a-cb00-4323-8e05-09e0ec471914",  // Row 9997
        "ab52a0cc-c6bb-4d61-8a8f-166dc4b8b13c",  // Row 9998
        "85440778-460a-41ac-aa2e-ac3ee41696bf"   // Row 9999
    };

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      String lastValue = null;

      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        String value = (String) row.getColumnValue(0);

        // Verify first 5 values
        if (rowCount < 5) {
          assertEquals(expectedFirst5[rowCount], value,
              "Row " + rowCount + " value mismatch");
        }

        lastValue = value;
        rowCount++;
      }

      assertEquals(10000, rowCount);

      // To verify last 5 values, we need to re-read or collect them
      // Let's re-read and collect last 5
      iterator = reader.rowIterator();
      String[] actualLast5 = new String[5];
      rowCount = 0;

      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        String value = (String) row.getColumnValue(0);

        // Store last 5 values in a circular manner
        if (rowCount >= 9995) {
          actualLast5[rowCount - 9995] = value;
        }

        rowCount++;
      }

      // Verify last 5 values
      for (int i = 0; i < 5; i++) {
        assertEquals(expectedLast5[i], actualLast5[i],
            "Row " + (9995 + i) + " value mismatch");
      }
    }
  }

  @Test
  void testLargerFileAllValuesAreUUIDs() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        String value = (String) row.getColumnValue(0);

        assertNotNull(value, "Value should not be null at row " + rowCount);

        // UUID format: 8-4-4-4-12 characters with hyphens = 36 total
        assertEquals(36, value.length(),
            "UUID at row " + rowCount + " should be 36 characters");

        // Verify UUID format with regex
        assertTrue(value.matches(
                "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
            "Value at row " + rowCount + " should be a valid UUID: " + value);

        rowCount++;
      }

      assertEquals(10000, rowCount);
    }
  }

  @Test
  void testLargerFileAllValuesAreUnique() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      Set<String> uniqueValues = new HashSet<>();
      int rowCount = 0;

      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        String value = (String) row.getColumnValue(0);

        assertTrue(uniqueValues.add(value),
            "Duplicate UUID found at row " + rowCount + ": " + value);

        rowCount++;
      }

      assertEquals(10000, rowCount,
          "Should have processed 10,000 rows");
      assertEquals(10000, uniqueValues.size(),
          "All 10,000 values should be unique");
    }
  }

  @Test
  void testLargerFileAccessByColumnName() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();

        // Access by column name
        String valueByName = (String) row.getColumnValue("a");

        // Access by index
        String valueByIndex = (String) row.getColumnValue(0);

        // Both should return the same value
        assertEquals(valueByIndex, valueByName,
            "Column access by name and index should return same value at row " + rowCount);

        assertNotNull(valueByName);

        rowCount++;

        // Only check first 100 rows for performance
        if (rowCount >= 100) {
          break;
        }
      }

      assertTrue(rowCount > 0);
    }
  }

  @Test
  void testLargerFilePerformanceAndMemory() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    long startTime = System.currentTimeMillis();
    long startMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        // Just access the value to ensure it's decoded
        row.getColumnValue(0);
        rowCount++;
      }

      assertEquals(10000, rowCount);
    }

    long endTime = System.currentTimeMillis();
    long endMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

    long duration = endTime - startTime;
    long memoryUsed = endMemory - startMemory;

    System.out.println("Performance metrics for 10,000 row file:");
    System.out.println("  Time: " + duration + "ms");
    System.out.println("  Memory delta: " + (memoryUsed / 1024) + "KB");

    // Sanity check - reading 10k rows shouldn't take more than 5 seconds
    assertTrue(duration < 5000,
        "Reading 10,000 rows took too long: " + duration + "ms");
  }

  @Test
  void testLargerFileTypedAccess() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup firstRow = iterator.next();

      // Get column descriptor
      ColumnDescriptor colDescriptor = firstRow.getColumns().get(0);

      // Type-safe access
      String value = firstRow.getColumnValue(colDescriptor, String.class);

      assertNotNull(value);
      assertEquals(36, value.length());
      assertEquals("c7ce6bef-d5b0-4863-b199-8ea8c7fb117b", value);
    }
  }

  @Test
  void testLargerFileRowToString() throws IOException {
    String filePath = TEST_DATA_DIR + TEST_FILE;

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup firstRow = iterator.next();

      String rowString = firstRow.toString();
      assertNotNull(rowString);
      assertTrue(rowString.contains("a="));
      assertTrue(rowString.contains("c7ce6bef-d5b0-4863-b199-8ea8c7fb117b"));
    }
  }
}
