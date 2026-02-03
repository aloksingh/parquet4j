package org.wazokazi.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.wazokazi.parquet.model.ColumnDescriptor;
import org.wazokazi.parquet.model.ColumnValues;
import org.wazokazi.parquet.model.Encoding;
import org.wazokazi.parquet.model.Page;
import org.wazokazi.parquet.model.ParquetMetadata;
import org.wazokazi.parquet.model.SchemaDescriptor;
import org.wazokazi.parquet.model.Type;

/**
 * Tests for reading DELTA_BINARY_PACKED encoded data.
 * <p>
 * Test file: delta_binary_packed.parquet
 * - Created by: parquet-mr version 1.10.0
 * - Rows: 200
 * - Columns: 66 (bitwidth0-64 INT64, int_value INT32)
 * - Encoding: DELTA_BINARY_PACKED for all columns
 * <p>
 * Expected data (verified with pyarrow):
 * - bitwidth0: all values are 6374628540732951412
 * - bitwidth1: [0, -1, -1, -1, -1, -1, -2, -2, -3, -3, ...]
 * - int_value: first value is -2070986743
 */
public class DeltaBinaryPackedTest {

  @Test
  void testDeltaBinaryPackedFileStructure() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = reader.getSchema();

      // Verify basic structure
      assertEquals(200, metadata.fileMetadata().numRows());
      assertEquals(66, schema.getNumColumns());

      // Verify INT64 columns exist
      assertColumnExists(schema, "bitwidth0", Type.INT64);
      assertColumnExists(schema, "bitwidth1", Type.INT64);
      assertColumnExists(schema, "bitwidth32", Type.INT64);
      assertColumnExists(schema, "bitwidth64", Type.INT64);

      // Verify INT32 column
      assertColumnExists(schema, "int_value", Type.INT32);
    }
  }

  @Test
  void testDeltaBinaryPackedPageStructure() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Test a few columns to ensure pages can be read
      String[] testColumns = {"bitwidth0", "bitwidth1", "bitwidth32", "int_value"};

      for (String columnName : testColumns) {
        int idx = findColumnIndex(schema, columnName);
        assertNotEquals(-1, idx, "Column '" + columnName + "' should exist");

        // Verify we can read page structure
        PageReader pageReader = rowGroup.getColumnPageReader(idx);
        List<Page> pages = pageReader.readAllPages();

        assertTrue(pages.size() > 0, "Column '" + columnName + "' should have pages");

        // Verify encoding
        for (Page page : pages) {
          if (page instanceof Page.DataPageV2 v2) {
            assertEquals(Encoding.DELTA_BINARY_PACKED, v2.encoding(),
                "Column '" + columnName + "' should use DELTA_BINARY_PACKED");
          }
        }
      }
    }
  }

  @Test
  void testBitwidth0AllSameValues() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      int idx = findColumnIndex(schema, "bitwidth0");
      List<Long> values = rowGroup.readColumn(idx).decodeAsInt64();

      // All 200 values should be the same
      assertEquals(200, values.size());
      long expectedValue = 6374628540732951412L;

      for (int i = 0; i < values.size(); i++) {
        assertEquals(expectedValue, values.get(i),
            "bitwidth0 row " + i + " should be " + expectedValue);
      }
    }
  }

  @Test
  void testBitwidth1VaryingValues() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      int idx = findColumnIndex(schema, "bitwidth1");
      List<Long> values = rowGroup.readColumn(idx).decodeAsInt64();

      assertEquals(200, values.size());

      // Verify first 20 values (verified with pyarrow)
      long[] expected = {0, -1, -1, -1, -1, -1, -2, -2, -3, -3,
          -4, -4, -4, -5, -5, -6, -6, -6, -6, -7};

      for (int i = 0; i < expected.length; i++) {
        assertEquals(expected[i], values.get(i),
            "bitwidth1 row " + i);
      }
    }
  }

  @Test
  void testBitwidth8Values() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      int idx = findColumnIndex(schema, "bitwidth8");
      List<Long> values = rowGroup.readColumn(idx).decodeAsInt64();

      assertEquals(200, values.size());

      // Verify first few values (verified with pyarrow)
      assertEquals(0L, values.get(0));
      assertEquals(-128L, values.get(1));
      assertEquals(-65L, values.get(2));
      assertEquals(-70L, values.get(3));
    }
  }

  @Test
  void testBitwidth32Values() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      int idx = findColumnIndex(schema, "bitwidth32");
      List<Long> values = rowGroup.readColumn(idx).decodeAsInt64();

      assertEquals(200, values.size());

      // Verify first few values (verified with pyarrow)
      assertEquals(0L, values.get(0));
      assertEquals(-2147483648L, values.get(1));
      assertEquals(-1986952430L, values.get(2));
    }
  }

  @Test
  void testBitwidth64Values() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      int idx = findColumnIndex(schema, "bitwidth64");
      List<Long> values = rowGroup.readColumn(idx).decodeAsInt64();

      assertEquals(200, values.size());

      // Verify first few values (verified with pyarrow)
      assertEquals(0L, values.get(0));
      assertEquals(-9223372036854775808L, values.get(1)); // Long.MIN_VALUE
      assertEquals(-725202170854031360L, values.get(2));
    }
  }

  @Test
  void testIntValueColumn() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      int idx = findColumnIndex(schema, "int_value");
      List<Integer> values = rowGroup.readColumn(idx).decodeAsInt32();

      assertEquals(200, values.size());

      // Verify first few values (verified with pyarrow)
      assertEquals(-2070986743, values.get(0));
      assertEquals(-22783326, values.get(1));
      assertEquals(-1782018724, values.get(2));
    }
  }

  @Test
  void testAllColumnsDecode() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Verify all 66 columns can be decoded
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        ColumnValues columnValues = rowGroup.readColumn(i);

        List<?> values;
        if (col.physicalType() == Type.INT32) {
          values = columnValues.decodeAsInt32();
        } else {
          values = columnValues.decodeAsInt64();
        }

        assertEquals(200, values.size(),
            "Column " + col.getPathString() + " should have 200 values");

        // Verify no nulls
        for (int j = 0; j < values.size(); j++) {
          assertNotNull(values.get(j),
              "Column " + col.getPathString() + " row " + j + " should not be null");
        }
      }
    }
  }

  private void assertColumnExists(SchemaDescriptor schema, String columnName, Type expectedType) {
    int idx = findColumnIndex(schema, columnName);
    assertNotEquals(-1, idx, "Column '" + columnName + "' should exist");

    ColumnDescriptor col = schema.getColumn(idx);
    assertEquals(expectedType, col.physicalType(),
        "Column '" + columnName + "' should be " + expectedType);
  }

  private int findColumnIndex(SchemaDescriptor schema, String columnName) {
    for (int i = 0; i < schema.getNumColumns(); i++) {
      ColumnDescriptor col = schema.getColumn(i);
      if (col.getPathString().equals(columnName)) {
        return i;
      }
    }
    return -1;
  }
}
