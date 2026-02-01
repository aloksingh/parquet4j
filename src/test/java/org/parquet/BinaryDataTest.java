package org.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ColumnValues;
import org.parquet.model.Page;
import org.parquet.model.ParquetMetadata;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.Type;

/**
 * Tests for reading binary data from Parquet files
 * Test cases validated using pyarrow library
 */
class BinaryDataTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  @Test
  void testReadBinaryParquet() throws IOException {
    String filePath = TEST_DATA_DIR + "binary.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Verify file metadata
      ParquetMetadata metadata = reader.getMetadata();
      assertNotNull(metadata);
      assertEquals(1, metadata.getNumRowGroups(), "Should have 1 row group");

      ParquetMetadata.FileMetadata fileMetadata = metadata.fileMetadata();
      assertEquals(12, fileMetadata.numRows(), "Should have 12 rows");

      // Verify schema
      SchemaDescriptor schema = fileMetadata.schema();
      assertEquals(1, schema.getNumColumns(), "Should have 1 column");

      ColumnDescriptor col = schema.getColumn(0);
      assertEquals("foo", col.getPathString(), "Column name should be 'foo'");
      assertEquals(Type.BYTE_ARRAY, col.physicalType(), "Column type should be BYTE_ARRAY");

      System.out.println("\n=== Testing: " + filePath + " ===");
      reader.printMetadata();
    }
  }

  @Test
  void testReadBinaryValues() throws IOException {
    String filePath = TEST_DATA_DIR + "binary.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read the binary column
      ColumnValues values = rowGroup.readColumn(0);
      List<byte[]> binaryValues = values.decodeAsByteArray();

      assertNotNull(binaryValues, "Binary values should not be null");
      assertEquals(12, binaryValues.size(), "Should have 12 binary values");

      // Verify each value matches expected byte sequence (0x00 to 0x0b)
      // Expected values based on pyarrow output:
      // Row 0: b'\x00', Row 1: b'\x01', ..., Row 11: b'\x0b'
      for (int i = 0; i < 12; i++) {
        byte[] value = binaryValues.get(i);
        assertNotNull(value, "Value at row " + i + " should not be null");
        assertEquals(1, value.length, "Each value should be 1 byte");
        String msg = String.format("Row %d: expected 0x%02x, got 0x%02x", i, (byte) i, value[0]);
        System.out.println(msg);
        assertEquals((byte) i, value[0], msg);
      }

      System.out.println("\nBinary values read successfully:");
      for (int i = 0; i < binaryValues.size(); i++) {
        byte[] value = binaryValues.get(i);
        System.out.printf("  Row %2d: 0x%02x%n", i, value[0] & 0xFF);
      }
    }
  }

  @Test
  void testBinaryValuesDetailed() throws IOException {
    String filePath = TEST_DATA_DIR + "binary.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Get column info
      ColumnDescriptor col = schema.getColumn(0);
      System.out.println("\n=== Binary Column Details ===");
      System.out.println("Column name: " + col.getPathString());
      System.out.println("Physical type: " + col.physicalType());

      // Read and verify pages
      PageReader pageReader = rowGroup.getColumnPageReader(0);
      List<Page> pages = pageReader.readAllPages();

      System.out.println("Number of pages: " + pages.size());
      for (int i = 0; i < pages.size(); i++) {
        Page page = pages.get(i);
        if (page instanceof Page.DataPage dataPage) {
          System.out.printf("  Page %d: DataPage, %d values, encoding: %s%n",
              i, dataPage.numValues(), dataPage.encoding());
        } else if (page instanceof Page.DictionaryPage dictPage) {
          System.out.printf("  Page %d: DictionaryPage, %d values%n",
              i, dictPage.numValues());
        }
      }

      // Read column values
      ColumnValues values = rowGroup.readColumn(0);
      List<byte[]> binaryValues = values.decodeAsByteArray();

      // Additional assertions
      assertTrue(binaryValues.size() > 0, "Should have at least one value");

      // Verify sequential byte pattern
      for (int i = 0; i < binaryValues.size(); i++) {
        byte[] value = binaryValues.get(i);
        byte expected = (byte) i;
        assertArrayEquals(new byte[] {expected}, value,
            "Row " + i + " should contain byte " + expected);
      }
    }
  }
}
