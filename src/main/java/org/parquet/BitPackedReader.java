package org.parquet;

import java.nio.ByteBuffer;
import org.parquet.model.ParquetException;

/**
 * Utility for reading bit-packed data from Parquet files.
 * <p>
 * In Parquet, boolean values are stored as individual bits packed into bytes to minimize storage space.
 * The bits are stored in LSB-first (least significant bit first) order, meaning the first boolean value
 * is stored in the least significant bit of the first byte.
 * <p>
 * For example, if we have 10 boolean values {@code [true, false, true, true, false, false, true, false, true, false]},
 * they are packed into 2 bytes as follows:
 * <ul>
 *   <li>Byte 0: bits 0-7 = {@code 01001101} (LSB to MSB)</li>
 *   <li>Byte 1: bits 8-9 = {@code 00000001} (LSB to MSB)</li>
 * </ul>
 */
public class BitPackedReader {

  /**
   * Reads bit-packed boolean values from a ByteBuffer.
   * <p>
   * Booleans are stored in LSB-first (least significant bit first) order. This method
   * extracts the specified number of boolean values by reading bits sequentially from
   * the buffer, consuming bytes as needed.
   * <p>
   * The buffer's position is advanced by the number of bytes consumed (calculated as
   * {@code ceil(numValues / 8)}).
   *
   * @param buffer    the ByteBuffer containing bit-packed boolean data
   * @param numValues the number of boolean values to read
   * @return an array of boolean values extracted from the buffer
   * @throws ParquetException if the buffer does not contain enough data to read all requested values
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
   * Calculates the number of bytes required to store a given number of bits.
   * <p>
   * This is computed as {@code ceil(numBits / 8)}, which is equivalent to {@code (numBits + 7) / 8}.
   * For example:
   * <ul>
   *   <li>1-8 bits require 1 byte</li>
   *   <li>9-16 bits require 2 bytes</li>
   *   <li>17-24 bits require 3 bytes</li>
   * </ul>
   *
   * @param numBits the number of bits to store
   * @return the number of bytes needed to store the bits
   */
  public static int bytesForBits(int numBits) {
    return (numBits + 7) / 8;
  }
}
