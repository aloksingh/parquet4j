package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for reading files with null pages (pages containing only null values).
 * <p>
 * Test file: int32_with_null_pages.parquet
 * - Created by: parquet-mr version 1.13.0-SNAPSHOT
 * - Rows: 1000
 * - Column: 'int32_field' - optional INT32
 * - Encoding: PLAIN with BIT_PACKED and RLE
 * - Compression: UNCOMPRESSED
 * - Contains 275 null values out of 1000 total values
 * <p>
 * Expected data (verified with pyarrow):
 * - Row 0: -654807448
 * - Row 4: null (first null value)
 * - Total null count: 275
 * - Total non-null count: 725
 */
class Int32WithNullPagesTest {

  @Test
  void testInt32WithNullPagesFileStructure() throws IOException {
    String filePath = "src/test/data/int32_with_null_pages.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = reader.getSchema();

      // Verify file structure
      assertEquals(1000, metadata.fileMetadata().numRows());
      assertEquals(1, schema.getNumColumns());

      // Verify column type and optionality
      ColumnDescriptor col = schema.getColumn(0);
      assertEquals("int32_field", col.getPathString());
      assertEquals(Type.INT32, col.physicalType());
      assertEquals(1, col.maxDefinitionLevel(), "Column should be optional (max def level = 1)");
    }
  }

  @Test
  void testInt32WithNullPagesNullCount() throws IOException {
    String filePath = "src/test/data/int32_with_null_pages.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      ColumnValues columnValues = rowGroup.readColumn(0);

      // Read all values
      List<Integer> values = columnValues.decodeAsInt32();

      assertEquals(1000, values.size(), "Should have 1000 total values");

      // Count nulls
      long nullCount = values.stream().filter(v -> v == null).count();
      long nonNullCount = values.stream().filter(v -> v != null).count();

      assertEquals(275, nullCount, "Should have 275 null values");
      assertEquals(725, nonNullCount, "Should have 725 non-null values");
    }
  }

  @Test
  void testInt32WithNullPagesFirstValues() throws IOException {
    String filePath = "src/test/data/int32_with_null_pages.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Integer> values = rowGroup.readColumn(0).decodeAsInt32();

      // Verify first 10 values (verified with pyarrow)
      assertEquals(-654807448, values.get(0));
      assertEquals(-465559769, values.get(1));
      assertEquals(-34563097, values.get(2));
      assertEquals(398454479, values.get(3));
      assertNull(values.get(4), "Row 4 should be null");
      assertEquals(2018642597, values.get(5));
      assertEquals(486675473, values.get(6));
      assertEquals(546345848, values.get(7));
      assertEquals(-117215667, values.get(8));
      assertEquals(-352034204, values.get(9));
    }
  }

  @Test
  void testInt32WithNullPagesNullPositions() throws IOException {
    String filePath = "src/test/data/int32_with_null_pages.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Integer> values = rowGroup.readColumn(0).decodeAsInt32();

      // Verify specific null positions (from pyarrow analysis)
      assertNull(values.get(4), "Row 4 should be null");

      // Verify there are nulls scattered throughout
      boolean hasNullInFirst100 = values.subList(0, 100).stream().anyMatch(v -> v == null);
      boolean hasNullInMiddle = values.subList(400, 600).stream().anyMatch(v -> v == null);
      boolean hasNullInLast100 = values.subList(900, 1000).stream().anyMatch(v -> v == null);

      assertTrue(hasNullInFirst100, "Should have nulls in first 100 rows");
      assertTrue(hasNullInMiddle, "Should have nulls in middle rows");
      assertTrue(hasNullInLast100, "Should have nulls in last 100 rows");
    }
  }

  @Test
  void testInt32WithNullPagesNonNullValues() throws IOException {
    String filePath = "src/test/data/int32_with_null_pages.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Integer> values = rowGroup.readColumn(0).decodeAsInt32();

      // Extract non-null values
      List<Integer> nonNullValues = values.stream()
          .filter(v -> v != null)
          .toList();

      assertEquals(725, nonNullValues.size(), "Should have exactly 725 non-null values");

      // Verify all non-null values are valid INT32
      for (Integer value : nonNullValues) {
        assertNotNull(value);
        // Value should fit in INT32 range (already validated by type, but checking)
        assertTrue(value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE);
      }
    }
  }

  @Test
  void testInt32WithNullPagesLastValues() throws IOException {
    String filePath = "src/test/data/int32_with_null_pages.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Integer> values = rowGroup.readColumn(0).decodeAsInt32();

      // Verify last 5 values (verified with pyarrow)
      assertEquals(-1451413579, values.get(995));
      assertEquals(43219732, values.get(996));
      assertEquals(211608450, values.get(997));
      assertEquals(1341709713, values.get(998));
      assertEquals(303403251, values.get(999));
    }
  }

  @Test
  void testInt32WithNullPagesDefinitionLevels() throws IOException {
    String filePath = "src/test/data/int32_with_null_pages.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();
      ColumnDescriptor col = schema.getColumn(0);

      // For optional columns, definition level indicates null/non-null
      // max_definition_level = 1 means: 0 = null, 1 = defined
      assertEquals(1, col.maxDefinitionLevel(),
          "Optional column should have max definition level = 1");
      assertEquals(0, col.maxRepetitionLevel(),
          "Non-repeated column should have max repetition level = 0");
    }
  }

  @Test
  void testInt32WithNullPagesValueRange() throws IOException {
    String filePath = "src/test/data/int32_with_null_pages.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Integer> values = rowGroup.readColumn(0).decodeAsInt32();

      // Find min/max of non-null values
      int min = values.stream()
          .filter(v -> v != null)
          .mapToInt(Integer::intValue)
          .min()
          .orElseThrow();

      int max = values.stream()
          .filter(v -> v != null)
          .mapToInt(Integer::intValue)
          .max()
          .orElseThrow();

      // Verify we have a reasonable range (random INT32 values)
      assertTrue(min < 0, "Should have negative values");
      assertTrue(max > 0, "Should have positive values");

      // Verify min and max are within INT32 bounds
      assertTrue(min >= Integer.MIN_VALUE);
      assertTrue(max <= Integer.MAX_VALUE);
    }
  }
}
