package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for reading DELTA_LENGTH_BYTE_ARRAY encoding.
 * <p>
 * Test file: delta_length_byte_array.parquet
 * - Rows: 1000
 * - Column: FRUIT (string)
 * - Compression: ZSTD
 * - Encoding: DELTA_LENGTH_BYTE_ARRAY
 * <p>
 * Expected data (verified with pyarrow):
 * - Format: "apple_banana_mango{i^2}" where i is the row index (0-999)
 * - First 5: ['apple_banana_mango0', 'apple_banana_mango1', 'apple_banana_mango4',
 * 'apple_banana_mango9', 'apple_banana_mango16']
 * - Last 5: ['apple_banana_mango980100', 'apple_banana_mango982081', 'apple_banana_mango984064',
 * 'apple_banana_mango986049', 'apple_banana_mango988036']
 */
public class DeltaLengthByteArrayTest {

  @Test
  void testDeltaLengthByteArrayFileStructure() throws IOException {
    String filePath = "src/test/data/delta_length_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = reader.getSchema();

      // Verify file structure
      assertEquals(1000, metadata.fileMetadata().numRows());
      assertEquals(1, schema.getNumColumns());

      // Verify column
      assertEquals("FRUIT", schema.getColumn(0).getPathString());
      assertEquals(Type.BYTE_ARRAY, schema.getColumn(0).physicalType());
    }
  }

  @Test
  void testDeltaLengthByteArrayFirstValues() throws IOException {
    String filePath = "src/test/data/delta_length_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read FRUIT column
      List<String> values = rowGroup.readColumn(0).decodeAsString();

      assertEquals(1000, values.size(), "Should have 1000 values");

      // Verify first 10 values (i^2: 0, 1, 4, 9, 16, 25, 36, 49, 64, 81)
      assertEquals("apple_banana_mango0", values.get(0));
      assertEquals("apple_banana_mango1", values.get(1));
      assertEquals("apple_banana_mango4", values.get(2));
      assertEquals("apple_banana_mango9", values.get(3));
      assertEquals("apple_banana_mango16", values.get(4));
      assertEquals("apple_banana_mango25", values.get(5));
      assertEquals("apple_banana_mango36", values.get(6));
      assertEquals("apple_banana_mango49", values.get(7));
      assertEquals("apple_banana_mango64", values.get(8));
      assertEquals("apple_banana_mango81", values.get(9));

      // Verify no nulls
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }
    }
  }

  @Test
  void testDeltaLengthByteArrayMiddleValues() throws IOException {
    String filePath = "src/test/data/delta_length_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read FRUIT column
      List<String> values = rowGroup.readColumn(0).decodeAsString();

      // Verify middle values around index 100 (100^2 = 10000, 101^2 = 10201, etc.)
      assertEquals("apple_banana_mango10000", values.get(100));
      assertEquals("apple_banana_mango10201", values.get(101));
      assertEquals("apple_banana_mango10404", values.get(102));
      assertEquals("apple_banana_mango10609", values.get(103));
      assertEquals("apple_banana_mango10816", values.get(104));
    }
  }

  @Test
  void testDeltaLengthByteArrayLastValues() throws IOException {
    String filePath = "src/test/data/delta_length_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read FRUIT column
      List<String> values = rowGroup.readColumn(0).decodeAsString();

      // Verify last 10 values (990^2 = 980100, ..., 999^2 = 998001)
      assertEquals("apple_banana_mango980100", values.get(990));
      assertEquals("apple_banana_mango982081", values.get(991));
      assertEquals("apple_banana_mango984064", values.get(992));
      assertEquals("apple_banana_mango986049", values.get(993));
      assertEquals("apple_banana_mango988036", values.get(994));
      assertEquals("apple_banana_mango990025", values.get(995));
      assertEquals("apple_banana_mango992016", values.get(996));
      assertEquals("apple_banana_mango994009", values.get(997));
      assertEquals("apple_banana_mango996004", values.get(998));
      assertEquals("apple_banana_mango998001", values.get(999));
    }
  }

  @Test
  void testDeltaLengthByteArrayAllValues() throws IOException {
    String filePath = "src/test/data/delta_length_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read FRUIT column
      List<String> values = rowGroup.readColumn(0).decodeAsString();

      assertEquals(1000, values.size(), "Should have 1000 values");

      // Verify the pattern: apple_banana_mango{i^2} for all values
      for (int i = 0; i < values.size(); i++) {
        int expectedNumber = i * i; // i^2
        String expectedValue = "apple_banana_mango" + expectedNumber;
        assertEquals(expectedValue, values.get(i),
            "Value at index " + i + " should match pattern");
      }
    }
  }

  @Test
  void testDeltaLengthByteArrayStringLengths() throws IOException {
    String filePath = "src/test/data/delta_length_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read FRUIT column
      List<String> values = rowGroup.readColumn(0).decodeAsString();

      // Verify string lengths vary
      // First value: "apple_banana_mango0" = 19 chars
      assertEquals(19, values.get(0).length());

      // Value at index 10: "apple_banana_mango100" = 21 chars
      assertEquals(21, values.get(10).length());

      // Value at index 100: "apple_banana_mango10000" = 23 chars
      assertEquals(23, values.get(100).length());

      // Last value: "apple_banana_mango998001" = 24 chars
      assertEquals(24, values.get(999).length());

      // All strings should start with same prefix
      String prefix = "apple_banana_mango";
      for (int i = 0; i < values.size(); i++) {
        assertTrue(values.get(i).startsWith(prefix),
            "Value at index " + i + " should start with '" + prefix + "'");
      }
    }
  }

  @Test
  void testDeltaLengthByteArrayCompression() throws IOException {
    String filePath = "src/test/data/delta_length_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();

      // Verify the file uses ZSTD compression
      ParquetMetadata.ColumnChunkMetadata columnChunk =
          metadata.rowGroups().get(0).columns().get(0);

      assertEquals("ZSTD", columnChunk.codec().name());
    }
  }
}
