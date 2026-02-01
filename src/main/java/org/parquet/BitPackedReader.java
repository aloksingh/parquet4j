package org.parquet;

import java.nio.ByteBuffer;
import org.parquet.model.ParquetException;

/**
 * Utility for reading bit-packed data from Parquet files.
 * Booleans in Parquet are stored as individual bits, packed into bytes.
 */
public class BitPackedReader {

  /**
   * Read bit-packed boolean values from a buffer.
   * In Parquet, booleans are stored LSB first (least significant bit first).
   *
   * @param buffer    ByteBuffer containing bit-packed data
   * @param numValues Number of boolean values to read
   * @return Array of boolean values
   */
  public static boolean[] readBooleans(ByteBuffer buffer, int numValues) {
    boolean[] result = new boolean[numValues];

    int currentByte = 0;
    int bitIndex = 0;

    for (int i = 0; i < numValues; i++) {
      // Read a new byte when needed
      if (bitIndex == 0) {
        if (buffer.hasRemaining()) {
          currentByte = buffer.get() & 0xFF;
        } else {
          throw new ParquetException("Buffer underflow while reading bit-packed booleans");
        }
      }

      // Extract the bit at the current position (LSB first)
      result[i] = ((currentByte >> bitIndex) & 1) != 0;

      // Move to next bit
      bitIndex++;
      if (bitIndex == 8) {
        bitIndex = 0;
      }
    }

    return result;
  }

  /**
   * Calculate the number of bytes needed to store a given number of bits
   */
  public static int bytesForBits(int numBits) {
    return (numBits + 7) / 8;
  }
}
