package org.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ColumnValues;
import org.parquet.model.ParquetMetadata;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.Type;

/**
 * Tests for reading Data Page V2 format with Snappy compression.
 * <p>
 * Test file: datapage_v2.snappy.parquet
 * - Created by: parquet-mr version 1.8.1
 * - Format version: 1.0 (but uses Data Page V2)
 * - Rows: 5
 * - Columns: 5 (a: string, b: int32, c: double, d: boolean, e: list<int32>)
 * - Compression: SNAPPY
 * - Encodings: PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED, RLE
 * <p>
 * Expected data (verified with pyarrow):
 * - Column 'a' (string): ['abc', 'abc', 'abc', null, 'abc']
 * - Column 'b' (int32): [1, 2, 3, 4, 5]
 * - Column 'c' (double): [2.0, 3.0, 4.0, 5.0, 2.0]
 * - Column 'd' (boolean): [true, true, true, false, true]
 * - Column 'e' (list<int32>): [[1,2,3], null, null, [1,2,3], [1,2]]
 */
class DataPageV2SnappyTest {

  @Test
  void testDataPageV2FileStructure() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = reader.getSchema();

      // Verify file structure
      assertEquals(5, metadata.fileMetadata().numRows());
      assertEquals(5, schema.getNumColumns());

      // Verify column names and types
      assertEquals("a", schema.getColumn(0).getPathString());
      assertEquals(Type.BYTE_ARRAY, schema.getColumn(0).physicalType());

      assertEquals("b", schema.getColumn(1).getPathString());
      assertEquals(Type.INT32, schema.getColumn(1).physicalType());

      assertEquals("c", schema.getColumn(2).getPathString());
      assertEquals(Type.DOUBLE, schema.getColumn(2).physicalType());

      assertEquals("d", schema.getColumn(3).getPathString());
      assertEquals(Type.BOOLEAN, schema.getColumn(3).physicalType());
    }
  }

  @Test
  void testDataPageV2StringColumn() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Column 'a' is at index 0
      List<String> values = rowGroup.readColumn(0).decodeAsString();

      assertEquals(5, values.size(), "Should have 5 values");

      // Verify values: ['abc', 'abc', 'abc', null, 'abc']
      assertEquals("abc", values.get(0));
      assertEquals("abc", values.get(1));
      assertEquals("abc", values.get(2));
      assertNull(values.get(3), "Row 3 should be null");
      assertEquals("abc", values.get(4));
    }
  }

  @Test
  void testDataPageV2Int32Column() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Column 'b' is at index 1
      List<Integer> values = rowGroup.readColumn(1).decodeAsInt32();

      assertEquals(5, values.size(), "Should have 5 values");

      // Verify values: [1, 2, 3, 4, 5]
      assertEquals(1, values.get(0));
      assertEquals(2, values.get(1));
      assertEquals(3, values.get(2));
      assertEquals(4, values.get(3));
      assertEquals(5, values.get(4));

      // Verify no nulls
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }
    }
  }

  @Test
  void testDataPageV2DoubleColumn() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Column 'c' is at index 2
      List<Double> values = rowGroup.readColumn(2).decodeAsDouble();

      assertEquals(5, values.size(), "Should have 5 values");

      // Verify values: [2.0, 3.0, 4.0, 5.0, 2.0]
      assertEquals(2.0, values.get(0), 0.0001);
      assertEquals(3.0, values.get(1), 0.0001);
      assertEquals(4.0, values.get(2), 0.0001);
      assertEquals(5.0, values.get(3), 0.0001);
      assertEquals(2.0, values.get(4), 0.0001);

      // Verify no nulls
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }
    }
  }

  @Test
  void testDataPageV2BooleanColumn() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Column 'd' is at index 3
      List<Boolean> values = rowGroup.readColumn(3).decodeAsBoolean();

      assertEquals(5, values.size(), "Should have 5 values");

      // Verify values: [true, true, true, false, true]
      assertTrue(values.get(0), "Row 0 should be true");
      assertTrue(values.get(1), "Row 1 should be true");
      assertTrue(values.get(2), "Row 2 should be true");
      assertFalse(values.get(3), "Row 3 should be false");
      assertTrue(values.get(4), "Row 4 should be true");

      // Verify no nulls
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }
    }
  }

  @Test
  void testDataPageV2NullHandling() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Column 'a' has one null value at row 3
      List<String> values = rowGroup.readColumn(0).decodeAsString();

      // Count nulls
      long nullCount = values.stream().filter(v -> v == null).count();
      long nonNullCount = values.stream().filter(v -> v != null).count();

      assertEquals(1, nullCount, "Should have 1 null value");
      assertEquals(4, nonNullCount, "Should have 4 non-null values");

      // Verify the null is at position 3
      assertNull(values.get(3), "Row 3 should be null");

      // Verify other rows are not null
      assertNotNull(values.get(0));
      assertNotNull(values.get(1));
      assertNotNull(values.get(2));
      assertNotNull(values.get(4));
    }
  }

  @Test
  void testDataPageV2AllColumns() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Verify we can read all simple columns (not testing list column 'e' yet)
      for (int i = 0; i < 4; i++) {
        ColumnDescriptor col = schema.getColumn(i);
        ColumnValues columnValues = rowGroup.readColumn(i);

        List<?> values;
        switch (col.physicalType()) {
          case BYTE_ARRAY -> values = columnValues.decodeAsString();
          case INT32 -> values = columnValues.decodeAsInt32();
          case DOUBLE -> values = columnValues.decodeAsDouble();
          case BOOLEAN -> values = columnValues.decodeAsBoolean();
          default -> throw new AssertionError("Unexpected type: " + col.physicalType());
        }

        assertEquals(5, values.size(),
            "Column " + col.getPathString() + " should have 5 values");
      }
    }
  }

  @Test
  void testDataPageV2SnappyDecompression() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // If we can read all columns successfully, Snappy decompression is working
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read all simple columns
      List<String> colA = rowGroup.readColumn(0).decodeAsString();
      List<Integer> colB = rowGroup.readColumn(1).decodeAsInt32();
      List<Double> colC = rowGroup.readColumn(2).decodeAsDouble();
      List<Boolean> colD = rowGroup.readColumn(3).decodeAsBoolean();

      // Verify all have correct row count
      assertEquals(5, colA.size(), "Snappy decompression failed for column a");
      assertEquals(5, colB.size(), "Snappy decompression failed for column b");
      assertEquals(5, colC.size(), "Snappy decompression failed for column c");
      assertEquals(5, colD.size(), "Snappy decompression failed for column d");

      // Spot check values to ensure decompression produced correct data
      assertEquals("abc", colA.get(0));
      assertEquals(1, colB.get(0));
      assertEquals(2.0, colC.get(0), 0.0001);
      assertTrue(colD.get(0));
    }
  }

  @Test
  void testDataPageV2DeltaBinaryPackedEncoding() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Column 'b' uses DELTA_BINARY_PACKED encoding
      List<Integer> values = rowGroup.readColumn(1).decodeAsInt32();

      assertEquals(5, values.size());

      // Verify sequential values (good test for delta encoding)
      for (int i = 0; i < values.size(); i++) {
        assertEquals(i + 1, values.get(i),
            "DELTA_BINARY_PACKED decoding failed at position " + i);
      }
    }
  }

  @Test
  void testDataPageV2ValueRanges() throws IOException {
    String filePath = "src/test/data/datapage_v2.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Test integer column range
      List<Integer> intValues = rowGroup.readColumn(1).decodeAsInt32();
      int intMin = intValues.stream().mapToInt(Integer::intValue).min().orElseThrow();
      int intMax = intValues.stream().mapToInt(Integer::intValue).max().orElseThrow();
      assertEquals(1, intMin, "Min int32 value should be 1");
      assertEquals(5, intMax, "Max int32 value should be 5");

      // Test double column range
      List<Double> doubleValues = rowGroup.readColumn(2).decodeAsDouble();
      double doubleMin = doubleValues.stream().mapToDouble(Double::doubleValue).min().orElseThrow();
      double doubleMax = doubleValues.stream().mapToDouble(Double::doubleValue).max().orElseThrow();
      assertEquals(2.0, doubleMin, 0.0001, "Min double value should be 2.0");
      assertEquals(5.0, doubleMax, 0.0001, "Max double value should be 5.0");
    }
  }
}
