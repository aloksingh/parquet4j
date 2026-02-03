package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for RleEncoder, verifying round-trip compatibility with RleDecoder
 */
class RleEncoderTest {

  /**
   * Helper method to decode RLE-encoded data
   */
  private int[] decodeRle(byte[] encoded, int bitWidth, int numValues) {
    ByteBuffer buffer = ByteBuffer.wrap(encoded);
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    int length = buffer.getInt();
    buffer = buffer.slice();
    buffer.limit(length);
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

    RleDecoder decoder = new RleDecoder(buffer, bitWidth, numValues);
    return decoder.readAll();
  }

  /**
   * Test encoding and decoding a simple RLE run
   */
  @Test
  void testSimpleRleRun() throws IOException {
    RleEncoder encoder = new RleEncoder(2);
    int[] values = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // 10 twos

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 2, values.length);
    assertArrayEquals(values, decoded);
  }

  /**
   * Test encoding definition levels for optional column
   */
  @Test
  void testDefinitionLevels() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    // Pattern: non-null, null, non-null, non-null, null
    int[] defLevels = {1, 0, 1, 1, 0};

    byte[] encoded = encoder.encode(defLevels);
    int[] decoded = decodeRle(encoded, 1, defLevels.length);
    assertArrayEquals(defLevels, decoded);
  }

  /**
   * Test all zeros (common for required columns with all values present)
   */
  @Test
  void testAllZeros() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    int[] values = new int[100];
    Arrays.fill(values, 0);

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 1, values.length);
    assertArrayEquals(values, decoded);
  }

  /**
   * Test all ones
   */
  @Test
  void testAllOnes() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    int[] values = new int[100];
    Arrays.fill(values, 1);

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 1, values.length);
    assertArrayEquals(values, decoded);
  }

  /**
   * Test bit-packed values
   */
  @Test
  void testBitPackedRun() throws IOException {
    RleEncoder encoder = new RleEncoder(2);
    int[] values = {0, 1, 2, 3, 0, 1, 2, 3}; // 8 different values

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 2, values.length);
    assertArrayEquals(values, decoded);
  }

  /**
   * Test mixed RLE and bit-packed runs
   */
  @Test
  void testMixedRuns() throws IOException {
    RleEncoder encoder = new RleEncoder(3);
    // RLE run of 10 fives, then varied values, then RLE run of 8 sevens
    int[] values = {
        5, 5, 5, 5, 5, 5, 5, 5, 5, 5, // RLE
        1, 2, 3, 4, 5, 6, 7, 0,       // Bit-packed
        7, 7, 7, 7, 7, 7, 7, 7        // RLE
    };

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 3, values.length);
    assertArrayEquals(values, decoded);
  }

  /**
   * Test single value
   */
  @Test
  void testSingleValue() throws IOException {
    RleEncoder encoder = new RleEncoder(3);
    int[] values = {5};

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 3, values.length);
    assertArrayEquals(values, decoded);
  }

  /**
   * Test empty values
   */
  @Test
  void testEmptyValues() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    int[] values = {};

    byte[] encoded = encoder.encode(values);

    assertEquals(4, encoded.length); // Just the length field
    ByteBuffer buffer = ByteBuffer.wrap(encoded);
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    int length = buffer.getInt();
    assertEquals(0, length);
  }

  /**
   * Test bit width 0 (all values must be 0)
   */
  @Test
  void testBitWidthZero() throws IOException {
    RleEncoder encoder = new RleEncoder(0);
    int[] values = {0, 0, 0, 0, 0};

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 0, values.length);
    assertArrayEquals(values, decoded);
  }

  /**
   * Test large RLE run
   */
  @Test
  void testLargeRleRun() throws IOException {
    RleEncoder encoder = new RleEncoder(3);
    int[] values = new int[1000];
    Arrays.fill(values, 7);

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 3, values.length);
    assertArrayEquals(values, decoded);
  }

  /**
   * Test alternating pattern (stress test for bit-packing)
   */
  @Test
  void testAlternatingPattern() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    int[] values = new int[100];
    for (int i = 0; i < values.length; i++) {
      values[i] = i % 2;
    }

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 1, values.length);
    assertArrayEquals(values, decoded);
  }

  /**
   * Test with List<Integer> instead of int[]
   */
  @Test
  void testWithList() throws IOException {
    RleEncoder encoder = new RleEncoder(2);
    List<Integer> values = Arrays.asList(1, 1, 1, 2, 2, 2, 3, 3);

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 2, values.size());

    int[] expected = {1, 1, 1, 2, 2, 2, 3, 3};
    assertArrayEquals(expected, decoded);
  }

  /**
   * Test bit width calculation
   */
  @Test
  void testBitWidthCalculation() {
    assertEquals(0, RleEncoder.bitWidth(0));
    assertEquals(1, RleEncoder.bitWidth(1));
    assertEquals(2, RleEncoder.bitWidth(2));
    assertEquals(2, RleEncoder.bitWidth(3));
    assertEquals(3, RleEncoder.bitWidth(4));
    assertEquals(3, RleEncoder.bitWidth(7));
    assertEquals(4, RleEncoder.bitWidth(8));
    assertEquals(4, RleEncoder.bitWidth(15));
    assertEquals(5, RleEncoder.bitWidth(16));
  }

  /**
   * Test realistic definition levels for optional column with many nulls
   */
  @Test
  void testManyNulls() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    int[] defLevels = new int[50];
    // Pattern: mostly nulls with occasional non-null values
    for (int i = 0; i < defLevels.length; i++) {
      defLevels[i] = (i % 10 == 0) ? 1 : 0;
    }

    byte[] encoded = encoder.encode(defLevels);
    int[] decoded = decodeRle(encoded, 1, defLevels.length);
    assertArrayEquals(defLevels, decoded);
  }

  /**
   * Test realistic definition levels for optional column with few nulls
   */
  @Test
  void testFewNulls() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    int[] defLevels = new int[50];
    Arrays.fill(defLevels, 1);
    // Set a few nulls
    defLevels[10] = 0;
    defLevels[25] = 0;
    defLevels[40] = 0;

    byte[] encoded = encoder.encode(defLevels);
    int[] decoded = decodeRle(encoded, 1, defLevels.length);
    assertArrayEquals(defLevels, decoded);
  }
}
