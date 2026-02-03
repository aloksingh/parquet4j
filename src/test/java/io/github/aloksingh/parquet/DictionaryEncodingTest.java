package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.SchemaDescriptor;

/**
 * Tests for dictionary encoding support
 */
class DictionaryEncodingTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  @Test
  void testReadDictionaryEncodedFile() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_dictionary.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      System.out.println("\n=== Testing Dictionary-Encoded File ===");
      reader.printMetadata();

      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Try reading each column
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.println(
            "\nColumn " + i + ": " + col.getPathString() + " (" + col.physicalType() + ")");

        try {
          ColumnValues values = rowGroup.readColumn(i);

          switch (col.physicalType()) {
            case BOOLEAN -> {
              List<Boolean> bools = values.decodeAsBoolean();
              System.out.println("  Read " + bools.size() + " boolean values");
              System.out.println("  Sample: " + bools.subList(0, Math.min(3, bools.size())));
            }
            case INT32 -> {
              List<Integer> ints = values.decodeAsInt32();
              System.out.println("  Read " + ints.size() + " int32 values");
              System.out.println("  Sample: " + ints.subList(0, Math.min(3, ints.size())));
              assertNotNull(ints);
              assertTrue(ints.size() > 0);
            }
            case INT64 -> {
              List<Long> longs = values.decodeAsInt64();
              System.out.println("  Read " + longs.size() + " int64 values");
              System.out.println("  Sample: " + longs.subList(0, Math.min(3, longs.size())));
            }
            case FLOAT -> {
              List<Float> floats = values.decodeAsFloat();
              System.out.println("  Read " + floats.size() + " float values");
              System.out.println("  Sample: " + floats.subList(0, Math.min(3, floats.size())));
            }
            case DOUBLE -> {
              List<Double> doubles = values.decodeAsDouble();
              System.out.println("  Read " + doubles.size() + " double values");
              System.out.println("  Sample: " + doubles.subList(0, Math.min(3, doubles.size())));
            }
            case BYTE_ARRAY -> {
              List<String> strings = values.decodeAsString();
              System.out.println("  Read " + strings.size() + " string values");
              System.out.println("  Sample: " + strings.subList(0, Math.min(3, strings.size())));
            }
            default -> System.out.println("  [Skipped - type not supported]");
          }
        } catch (Exception e) {
          fail("Unable to parse " + "Column " + i + ": " + col.getPathString() + " (" +
              col.physicalType() + ")", e);
        }
      }
    }
  }

  @Test
  void testRleDecoderBasic() {
    // Test basic RLE decoding
    byte[] data = new byte[] {
        0x02,  // RLE run: header = 2 (LSB=0, length = 2>>1 = 1)
        0x05   // Value = 5
    };

    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(data);
    RleDecoder decoder = new RleDecoder(buffer, 8, 1);

    int[] values = decoder.readAll();
    assertEquals(1, values.length);
    assertEquals(5, values[0]);
  }

  @Test
  void testRleDecoderBitPacked() {
    // Test bit-packed run
    // Header with LSB=1, then bit-packed values
    byte[] data = new byte[] {
        0x03,  // Bit-packed run: header = 3 (LSB=1, num_groups = 3>>1 = 1)
        0x01, 0x02, 0x03  // 3 bytes for bit width 3 (8 values)
    };

    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(data);
    RleDecoder decoder = new RleDecoder(buffer, 3, 8);

    int[] values = decoder.readAll();
    assertEquals(8, values.length);
    System.out.println("Bit-packed values: " + java.util.Arrays.toString(values));
  }

}
