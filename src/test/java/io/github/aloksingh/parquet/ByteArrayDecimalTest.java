package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;

/**
 * Tests for reading BYTE_ARRAY DECIMAL type from Parquet files
 * <p>
 * Test file: byte_array_decimal.parquet
 * - Physical type: BYTE_ARRAY
 * - Logical type: DECIMAL(precision=4, scale=2)
 * - Contains 24 rows with values 1.00 through 24.00
 * - Values are encoded as big-endian signed integers
 * <p>
 * Test cases validated using pyarrow library
 */
class ByteArrayDecimalTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  @Test
  void testReadByteArrayDecimalMetadata() throws IOException {
    String filePath = TEST_DATA_DIR + "byte_array_decimal.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Verify file metadata
      ParquetMetadata metadata = reader.getMetadata();
      assertNotNull(metadata);
      assertEquals(1, metadata.getNumRowGroups(), "Should have 1 row group");

      ParquetMetadata.FileMetadata fileMetadata = metadata.fileMetadata();
      assertEquals(24, fileMetadata.numRows(), "Should have 24 rows");

      // Verify schema
      SchemaDescriptor schema = fileMetadata.schema();
      assertEquals(1, schema.getNumColumns(), "Should have 1 column");

      ColumnDescriptor col = schema.getColumn(0);
      assertEquals("value", col.getPathString(), "Column name should be 'value'");
      assertEquals(Type.BYTE_ARRAY, col.physicalType(), "Column type should be BYTE_ARRAY");

      System.out.println("\n=== Testing: " + filePath + " ===");
      reader.printMetadata();
    }
  }

  @Test
  void testReadByteArrayDecimalValues() throws IOException {
    String filePath = TEST_DATA_DIR + "byte_array_decimal.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read the decimal column as byte arrays
      ColumnValues values = rowGroup.readColumn(0);
      List<byte[]> byteArrayValues = values.decodeAsByteArray();

      assertNotNull(byteArrayValues, "Byte array values should not be null");
      assertEquals(24, byteArrayValues.size(), "Should have 24 decimal values");

      // Expected unscaled integer values for 1.00 through 24.00 with scale=2
      // Each decimal value is multiplied by 10^scale (10^2 = 100)
      // Verified with pyarrow
      int[] expectedUnscaledValues = {
          100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200,
          1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300, 2400
      };

      int scale = 2;

      System.out.println("\n=== Verifying byte array values ===");
      // Verify each byte array by converting to BigInteger and checking the value
      for (int i = 0; i < expectedUnscaledValues.length; i++) {
        byte[] actual = byteArrayValues.get(i);
        assertNotNull(actual, "Byte array at row " + i + " should not be null");

        // Convert to BigInteger (big-endian)
        BigInteger actualUnscaled = new BigInteger(actual);
        int expectedUnscaled = expectedUnscaledValues[i];

        String msg = String.format("Row %2d: bytes=%s -> unscaled=%d (expected=%d)",
            i, bytesToHex(actual), actualUnscaled.intValue(), expectedUnscaled);
        System.out.println(msg);

        assertEquals(expectedUnscaled, actualUnscaled.intValue(), msg);

        // Also verify the decimal value
        BigDecimal decimal = new BigDecimal(actualUnscaled, scale);
        BigDecimal expected = new BigDecimal(i + 1).setScale(scale);
        assertEquals(expected, decimal,
            String.format("Row %d decimal value should be %s", i, expected));
      }
    }
  }

  @Test
  void testConvertByteArrayToDecimal() throws IOException {
    String filePath = TEST_DATA_DIR + "byte_array_decimal.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read the decimal column as byte arrays
      ColumnValues values = rowGroup.readColumn(0);
      List<byte[]> byteArrayValues = values.decodeAsByteArray();

      System.out.println("\n=== Converting byte arrays to decimal values ===");
      System.out.println("Precision: 4, Scale: 2");
      System.out.println();

      // Convert each byte array to a BigDecimal
      // The byte arrays represent big-endian signed integers of the unscaled value
      int scale = 2;

      for (int i = 0; i < byteArrayValues.size(); i++) {
        byte[] bytes = byteArrayValues.get(i);

        // Convert big-endian bytes to BigInteger
        BigInteger unscaled = new BigInteger(bytes);

        // Create BigDecimal with the unscaled value and scale
        BigDecimal decimal = new BigDecimal(unscaled, scale);

        // Expected value is (i + 1).00
        BigDecimal expected = new BigDecimal(i + 1).setScale(scale);

        System.out.printf("Row %2d: bytes=%s -> unscaled=%d -> decimal=%s (expected=%s)%n",
            i, bytesToHex(bytes), unscaled.intValue(), decimal, expected);

        assertEquals(expected, decimal,
            String.format("Row %d should have decimal value %s", i, expected));
      }
    }
  }

  @Test
  void testDecimalValueRange() throws IOException {
    String filePath = TEST_DATA_DIR + "byte_array_decimal.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      ColumnValues values = rowGroup.readColumn(0);
      List<byte[]> byteArrayValues = values.decodeAsByteArray();

      int scale = 2;

      // Verify all values are in the range 1.00 to 24.00
      for (int i = 0; i < byteArrayValues.size(); i++) {
        BigInteger unscaled = new BigInteger(byteArrayValues.get(i));
        BigDecimal decimal = new BigDecimal(unscaled, scale);

        BigDecimal minValue = new BigDecimal("1.00");
        BigDecimal maxValue = new BigDecimal("24.00");

        assertEquals(1, decimal.compareTo(minValue) >= 0 ? 1 : 0,
            "Value should be >= 1.00");
        assertEquals(1, decimal.compareTo(maxValue) <= 0 ? 1 : 0,
            "Value should be <= 24.00");
      }
    }
  }

  /**
   * Helper method to convert byte array to hex string for display
   */
  private String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < bytes.length; i++) {
        if (i > 0) {
            sb.append(" ");
        }
      sb.append(String.format("0x%02x", bytes[i]));
    }
    sb.append("]");
    return sb.toString();
  }
}
