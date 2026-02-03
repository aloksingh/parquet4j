package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Debug test for RLE encoder issues
 */
class RleEncoderDebugTest {

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

  @Test
  void testSimplePattern() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    // Simple: 1, 0
    int[] values = {1, 0};

    byte[] encoded = encoder.encode(values);
    int[] decoded = decodeRle(encoded, 1, values.length);

    System.out.println("Input:  " + Arrays.toString(values));
    System.out.println("Output: " + Arrays.toString(decoded));

    assertArrayEquals(values, decoded);
  }

  @Test
  void testManyNullsFirst10() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    // First 11 values: 1,0,0,0,0,0,0,0,0,0,1
    int[] values = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};

    byte[] encoded = encoder.encode(values);

    System.out.println("\n=== Test: 11 values ===");
    System.out.println("Input:  " + Arrays.toString(values));
    System.out.println("Encoded bytes: " + Arrays.toString(encoded));
    System.out.println("Encoded hex: " + bytesToHex(encoded));

    int[] decoded = decodeRle(encoded, 1, values.length);

    System.out.println("Output: " + Arrays.toString(decoded));

    for (int i = 0; i < values.length; i++) {
      if (values[i] != decoded[i]) {
        System.out.println(
            "MISMATCH at index " + i + ": expected=" + values[i] + " actual=" + decoded[i]);
      }
    }

    assertArrayEquals(values, decoded);
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02X ", b));
    }
    return sb.toString().trim();
  }

  @Test
  void testManyNullsSmall() throws IOException {
    RleEncoder encoder = new RleEncoder(1);
    // Pattern from testManyNulls but smaller
    int[] defLevels = new int[20];
    for (int i = 0; i < defLevels.length; i++) {
      defLevels[i] = (i % 10 == 0) ? 1 : 0;
    }

    byte[] encoded = encoder.encode(defLevels);
    int[] decoded = decodeRle(encoded, 1, defLevels.length);

    System.out.println("Input:  " + Arrays.toString(defLevels));
    System.out.println("Output: " + Arrays.toString(decoded));

    assertArrayEquals(defLevels, decoded);
  }
}
