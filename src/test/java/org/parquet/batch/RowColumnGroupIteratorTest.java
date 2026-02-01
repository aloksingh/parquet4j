package org.parquet.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.parquet.ParquetRowIterator;
import org.parquet.RowColumnGroupIterator;
import org.parquet.SerializedFileReader;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.RowColumnGroup;
import org.parquet.model.SchemaDescriptor;

/**
 * Tests for RowColumnGroupIterator
 */
public class RowColumnGroupIteratorTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  @Test
  void testLz4RawCompressedFile() throws IOException {
    String filePath = TEST_DATA_DIR + "lz4_raw_compressed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Verify we can create the iterator
      RowColumnGroupIterator iterator = reader.rowIterator();
      assertNotNull(iterator);

      // Expected data from pyarrow inspection:
      // c0: int64 with values [1593604800, 1593604800, 1593604801, 1593604801]
      // c1: binary with values ['abc', 'def', 'abc', 'def']
      // v11: double with values [42.0, 7.7, 42.125, 7.7]

      long[] expectedC0 = {1593604800L, 1593604800L, 1593604801L, 1593604801L};
      String[] expectedC1 = {"abc", "def", "abc", "def"};
      double[] expectedV11 = {42.0, 7.7, 42.125, 7.7};

      List<RowColumnGroup> rows = new ArrayList<>();
      int rowCount = 0;

      // Iterate through all rows
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);
        rows.add(row);

        // Verify column count
        assertEquals(3, row.getColumnCount(),
            "Row should have 3 columns");

        // Verify schema
        SchemaDescriptor schema = row.getSchema();
        assertNotNull(schema);
        assertEquals(3, schema.getNumColumns());

        // Verify column names
        assertEquals("c0", schema.getColumn(0).getPathString());
        assertEquals("c1", schema.getColumn(1).getPathString());
        assertEquals("v11", schema.getColumn(2).getPathString());

        // Verify values for this row
        assertEquals(expectedC0[rowCount], row.getColumnValue(0),
            "Row " + rowCount + " column c0 mismatch");

        assertEquals(expectedC1[rowCount], row.getColumnValue(1),
            "Row " + rowCount + " column c1 mismatch");

        // For doubles, use delta comparison
        assertEquals(expectedV11[rowCount], (Double) row.getColumnValue(2), 0.001,
            "Row " + rowCount + " column v11 mismatch");

        // Test access by column name
        assertEquals(expectedC0[rowCount], row.getColumnValue("c0"));
        assertEquals(expectedC1[rowCount], row.getColumnValue("c1"));

        rowCount++;
      }

      // Verify total row count
      assertEquals(4, rowCount, "Should have exactly 4 rows");
      assertEquals(4, rows.size());

      // Verify hasNext returns false after iteration
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  void testLz4RawCompressedFileTypedAccess() throws IOException {
    String filePath = TEST_DATA_DIR + "lz4_raw_compressed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Get first row
      assertTrue(iterator.hasNext());
      RowColumnGroup firstRow = iterator.next();

      // Test typed access
      ColumnDescriptor c0Descriptor = firstRow.getColumns().get(0);
      ColumnDescriptor c1Descriptor = firstRow.getColumns().get(1);
      ColumnDescriptor v11Descriptor = firstRow.getColumns().get(2);

      Long c0Value = firstRow.getColumnValue(c0Descriptor, Long.class);
      assertEquals(1593604800L, c0Value);

      String c1Value = firstRow.getColumnValue(c1Descriptor, String.class);
      assertEquals("abc", c1Value);

      Double v11Value = firstRow.getColumnValue(v11Descriptor, Double.class);
      assertEquals(42.0, v11Value, 0.001);
    }
  }

  @Test
  void testLz4RawCompressedFileEnhancedFor() throws IOException {
    String filePath = TEST_DATA_DIR + "lz4_raw_compressed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int count = 0;
      // Test enhanced for-each style iteration
      for (RowColumnGroup row : (Iterable<RowColumnGroup>) () -> iterator) {
        assertNotNull(row);
        assertEquals(3, row.getColumnCount());
        count++;
      }

      assertEquals(4, count);
    }
  }

  @Test
  void testRowToString() throws IOException {
    String filePath = TEST_DATA_DIR + "lz4_raw_compressed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup firstRow = iterator.next();

      String rowString = firstRow.toString();
      assertNotNull(rowString);
      assertTrue(rowString.contains("c0=1593604800"));
      assertTrue(rowString.contains("c1=abc"));
      assertTrue(rowString.contains("v11=42.0"));
    }
  }

  @Test
  void testInvalidColumnAccess() throws IOException {
    String filePath = TEST_DATA_DIR + "lz4_raw_compressed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup row = iterator.next();

      // Test out of bounds access
      assertThrows(IndexOutOfBoundsException.class, () -> {
        row.getColumnValue(100);
      });

      assertThrows(IndexOutOfBoundsException.class, () -> {
        row.getColumnValue(-1);
      });

      // Test invalid column name
      assertThrows(IllegalArgumentException.class, () -> {
        row.getColumnValue("nonexistent_column");
      });
    }
  }

  @Test
  void testTypeMismatch() throws IOException {
    String filePath = TEST_DATA_DIR + "lz4_raw_compressed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup row = iterator.next();

      ColumnDescriptor c0Descriptor = row.getColumns().get(0);

      // Try to get a Long column as String - should throw ClassCastException
      assertThrows(ClassCastException.class, () -> {
        row.getColumnValue(c0Descriptor, String.class);
      });
    }
  }

  @Test
  void testAllTypesPlainWithIterator() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();

        // Verify we can access all columns
        assertNotNull(row);
        assertTrue(row.getColumnCount() > 0);

        // Sample verification for first row
        if (rowCount == 0) {
          // From the data we know the first row has id=4
          assertEquals(4, row.getColumnValue("id"));
        }

        rowCount++;
      }

      // Verify total rows
      assertEquals(8, rowCount);
    }
  }

  @Test
  void testGetTotalRowCount() throws IOException {
    String filePath = TEST_DATA_DIR + "lz4_raw_compressed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      assertEquals(4L, reader.getTotalRowCount());

      RowColumnGroupIterator iterator = reader.rowIterator();

      if (iterator instanceof ParquetRowIterator parquetIterator) {
        assertEquals(4L, parquetIterator.getTotalRowCount());
      }
    }
  }

  @Test
  void testGetSchema() throws IOException {
    String filePath = TEST_DATA_DIR + "lz4_raw_compressed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      if (iterator instanceof ParquetRowIterator parquetIterator) {
        SchemaDescriptor schema = parquetIterator.getSchema();

        assertNotNull(schema);
        assertEquals(3, schema.getNumColumns());
        assertEquals("c0", schema.getColumn(0).getPathString());
        assertEquals("c1", schema.getColumn(1).getPathString());
        assertEquals("v11", schema.getColumn(2).getPathString());
      }
    }
  }
}
