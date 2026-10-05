package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Specific tests for boolean data type support.
 *
 * <p>File-based assertions compare against exact golden values (pyarrow exports).
 * Unexpected decode failures fail the test.
 */
class BooleanReaderTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  /** alltypes_plain.parquet bool_col: alternating true/false over 8 rows. */
  private static final List<Boolean> ALLTYPES_PLAIN_BOOL_COL =
      List.of(true, false, true, false, true, false, true, false);

  @Test
  void testBitPackedBooleanReading() {
    // Test the bit-packed boolean reader directly
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(new byte[] {
        (byte) 0b10101010  // Alternating true/false for 8 values
    });
    boolean[] result = BitPackedReader.readBooleans(buffer, 8);

    assertEquals(8, result.length);
    assertFalse(result[0]);  // LSB first, so bit 0 = false
    assertTrue(result[1]);   // bit 1 = true
    assertFalse(result[2]);  // bit 2 = false
    assertTrue(result[3]);   // bit 3 = true
    assertFalse(result[4]);  // bit 4 = false
    assertTrue(result[5]);   // bit 5 = true
    assertFalse(result[6]);  // bit 6 = false
    assertTrue(result[7]);   // bit 7 = true
  }

  @Test
  void testBitPackedBooleanReadingPartial() {
    // Test reading fewer than 8 values
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(new byte[] {
        (byte) 0b00001111  // First 4 bits true, next 4 false
    });
    boolean[] result = BitPackedReader.readBooleans(buffer, 5);

    assertEquals(5, result.length);
    assertTrue(result[0]);   // bit 0 = true
    assertTrue(result[1]);   // bit 1 = true
    assertTrue(result[2]);   // bit 2 = true
    assertTrue(result[3]);   // bit 3 = true
    assertFalse(result[4]);  // bit 4 = false
  }

  @Test
  void testBytesForBits() {
    assertEquals(0, BitPackedReader.bytesForBits(0));
    assertEquals(1, BitPackedReader.bytesForBits(1));
    assertEquals(1, BitPackedReader.bytesForBits(8));
    assertEquals(2, BitPackedReader.bytesForBits(9));
    assertEquals(2, BitPackedReader.bytesForBits(16));
    assertEquals(3, BitPackedReader.bytesForBits(17));
  }

  @Test
  void testReadBooleanFromParquetFile() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find the boolean column
      boolean found = false;
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.physicalType() == Type.BOOLEAN) {
          found = true;
          ColumnValues values = rowGroup.readColumn(i);
          List<Boolean> boolValues = values.decodeAsBoolean();

          assertNotNull(boolValues);
          assertEquals(ALLTYPES_PLAIN_BOOL_COL, boolValues,
              col.getPathString() + " must match the golden values exactly");
          break;
        }
      }
      assertTrue(found, "alltypes_plain.parquet must contain a BOOLEAN column");
    }
  }

  @Test
  void testBooleanWithDifferentFiles() throws IOException {
    // Exact golden values per file; read or decode failures fail the test.
    assertEquals(ALLTYPES_PLAIN_BOOL_COL,
        readBooleanColumn("alltypes_plain.parquet"), "alltypes_plain bool_col");
    assertEquals(List.of(true, false),
        readBooleanColumn("alltypes_plain.snappy.parquet"), "alltypes_plain.snappy bool_col");
  }

  private List<Boolean> readBooleanColumn(String fileName) throws IOException {
    String filePath = TEST_DATA_DIR + fileName;
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.physicalType() == Type.BOOLEAN) {
          ColumnValues values = rowGroup.readColumn(i);
          List<Boolean> boolValues = values.decodeAsBoolean();
          assertNotNull(boolValues, fileName + ": " + col.getPathString());
          return boolValues;
        }
      }
    }
    throw new AssertionError("No BOOLEAN column found in " + fileName);
  }
}
