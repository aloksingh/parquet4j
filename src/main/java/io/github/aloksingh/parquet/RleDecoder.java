package io.github.aloksingh.parquet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import io.github.aloksingh.parquet.model.ParquetException;

/**
 * RLE/Bit-Packing Hybrid decoder for Parquet.
 * <p>
 * This encoding is used for definition/repetition levels and dictionary indices.
 * The encoding consists of runs of either RLE-encoded values or bit-packed values.
 * The grammer for run length encoding and bit packing is below:
 * <pre>
 * {@code
 * rle-bit-packed-hybrid: <length> <encoded-data>
 * length := length of the <encoded-data> in bytes stored as 4 bytes little endian
 * encoded-data := <run>*
 * run := <bit-packed-run> | <rle-run>
 * bit-packed-run := <bit-packed-header> <bit-packed-values>
 * bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
 * // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
 * bit-pack-count := (number of values in this run) / 8
 * bit-packed-values :=  bit packed back to back, from LSB to MSB
 * rle-run := <rle-header> <repeated-value>
 * rle-header := varint-encode( (number of times repeated) << 1)
 * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
 * }
 * </pre>
 */
public class RleDecoder {
  private final ByteBuffer buffer;
  private final int bitWidth;
  private final int totalValues;

  private int valuesRead;
  private int[] currentRun;
  private int currentRunIndex;

  /**
   * Create an RLE decoder
   *
   * @param buffer      Buffer containing RLE-encoded data
   * @param bitWidth    Bit width of values (1-32)
   * @param totalValues Total number of values to decode
   */
  public RleDecoder(ByteBuffer buffer, int bitWidth, int totalValues) {
    this.buffer = buffer.duplicate();
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
    this.bitWidth = bitWidth;
    this.totalValues = totalValues;
    this.valuesRead = 0;
  }

  /**
   * Read all values from the RLE-encoded buffer.
   *
   * @return Array containing all decoded values
   * @throws ParquetException if the buffer contains fewer values than expected
   */
  public int[] readAll() {
    // Special case: if we expect 0 values, return empty array
    if (totalValues == 0) {
      return new int[0];
    }

    int[] result = new int[totalValues];
    int index = 0;

    while (index < totalValues) {
      int value = readNext();
      if (value == -1) {
        break;
      }
      result[index++] = value;
    }

    if (index < totalValues) {
      throw new ParquetException(String.format(
          "RLE decoder underflow: expected %d values, got %d", totalValues, index));
    }

    return result;
  }

  /**
   * Read the next value from the RLE stream
   *
   * @return The next value, or -1 if no more values
   */
  public int readNext() {
    if (valuesRead >= totalValues) {
      return -1;
    }

    // If we have values in the current run, return them
    if (currentRun != null && currentRunIndex < currentRun.length) {
      valuesRead++;
      return currentRun[currentRunIndex++];
    }

    // Read the next run
    if (!buffer.hasRemaining()) {
      return -1;
    }

    int header = readUnsignedVarInt();

    if ((header & 1) == 0) {
      // RLE run
      int runLength = header >> 1;
      int value = readBitPackedValue(bitWidth);

      currentRun = new int[runLength];
      for (int i = 0; i < runLength; i++) {
        currentRun[i] = value;
      }
      currentRunIndex = 0;
    } else {
      // Bit-packed run
      int numGroups = header >> 1;
      int totalGroupValues = numGroups * 8;

      // According to spec, we always read full groups of 8 values
      // but only return the values we need
      int valuesToReturn = Math.min(totalGroupValues, totalValues - valuesRead);

      currentRun = readBitPackedRun(bitWidth, numGroups, valuesToReturn);
      currentRunIndex = 0;
    }

    // Return the first value from the new run
    if (currentRun != null && currentRun.length > 0) {
      valuesRead++;
      return currentRun[currentRunIndex++];
    }

    return -1;
  }

  /**
   * Read an unsigned variable-length integer from the buffer.
   * <p>
   * Variable-length integers use the LEB128 encoding where the high bit
   * of each byte indicates whether more bytes follow.
   *
   * @return The decoded unsigned integer value
   * @throws ParquetException if the varint exceeds the maximum representable value
   */
  private int readUnsignedVarInt() {
    int value = 0;
    int shift = 0;

    while (buffer.hasRemaining()) {
      int b = buffer.get() & 0xFF;
      value |= (b & 0x7F) << shift;

      if ((b & 0x80) == 0) {
        break;
      }

      shift += 7;
      if (shift > 28) {
        throw new ParquetException("Variable-length integer is too large");
      }
    }

    return value;
  }

  /**
   * Read a single bit-packed value of the specified bit width.
   * <p>
   * This method reads the minimum number of bytes needed to represent
   * a value with the given bit width, in little-endian order.
   *
   * @param bitWidth The number of bits used to represent the value (0-32)
   * @return The decoded value
   */
  private int readBitPackedValue(int bitWidth) {
    if (bitWidth == 0) {
      return 0;
    }

    // For bit widths <= 8, read from a single byte
    if (bitWidth <= 8) {
      int b = buffer.get() & 0xFF;
      return b & ((1 << bitWidth) - 1);
    }

    // For larger bit widths, read multiple bytes
    int value = 0;
    int bytesNeeded = (bitWidth + 7) / 8;

    for (int i = 0; i < bytesNeeded && buffer.hasRemaining(); i++) {
      int b = buffer.get() & 0xFF;
      value |= (b << (i * 8));
    }

    return value & ((1 << bitWidth) - 1);
  }

  /**
   * Read a bit-packed run of values
   * <p>
   * Bit-packed encoding packs 8 values at a time, with each value using bitWidth bits.
   * According to the spec, we always pack/read full groups of 8 values.
   *
   * @param bitWidth          Bit width of each value
   * @param numGroups         Number of groups (each group contains 8 values)
   * @param numValuesToReturn Number of values to actually return (may be less than numGroups * 8)
   * @return Array of decoded values
   */
  private int[] readBitPackedRun(int bitWidth, int numGroups, int numValuesToReturn) {
    if (bitWidth == 0) {
      return new int[numValuesToReturn];
    }

    // According to spec: we always read full groups of 8 values
    int totalGroupValues = numGroups * 8;
    int totalBits = totalGroupValues * bitWidth;
    int bytesNeeded = (totalBits + 7) / 8;

    // Read the bytes into a temporary buffer
    byte[] packedBytes = new byte[bytesNeeded];
    int bytesRead = Math.min(bytesNeeded, buffer.remaining());
    buffer.get(packedBytes, 0, bytesRead);

    // Unpack all values from the groups
    BitUnpacker unpacker = new BitUnpacker(packedBytes, bitWidth);
    int[] allValues = new int[totalGroupValues];
    for (int i = 0; i < totalGroupValues; i++) {
      allValues[i] = unpacker.unpack();
    }

    // Return only the values we need
    if (numValuesToReturn == totalGroupValues) {
      return allValues;
    } else {
      int[] result = new int[numValuesToReturn];
      System.arraycopy(allValues, 0, result, 0, numValuesToReturn);
      return result;
    }
  }

  /**
   * Helper class to unpack bit-packed values from a byte array.
   * <p>
   * This class reads values sequentially from a byte array where values
   * are packed tightly with no padding, using LSB-to-MSB bit ordering.
   */
  private static class BitUnpacker {
    private final byte[] data;
    private final int bitWidth;
    private int bitOffset;

    /**
     * Create a new bit unpacker.
     *
     * @param data     Byte array containing packed values
     * @param bitWidth Number of bits per value
     */
    BitUnpacker(byte[] data, int bitWidth) {
      this.data = data;
      this.bitWidth = bitWidth;
      this.bitOffset = 0;
    }

    /**
     * Unpack the next value from the bit-packed data.
     * <p>
     * Reads bitWidth bits from the current position, advancing the
     * internal bit offset for the next call.
     *
     * @return The unpacked value
     */
    int unpack() {
      if (bitWidth == 0) {
        return 0;
      }

      int value = 0;
      int bitsRemaining = bitWidth;

      while (bitsRemaining > 0) {
        int byteIndex = bitOffset / 8;
        int bitIndex = bitOffset % 8;

        if (byteIndex >= data.length) {
          break;
        }

        int currentByte = data[byteIndex] & 0xFF;
        int bitsAvailable = 8 - bitIndex;
        int bitsToRead = Math.min(bitsRemaining, bitsAvailable);

        // Extract bits from current byte
        int mask = (1 << bitsToRead) - 1;
        int bits = (currentByte >> bitIndex) & mask;

        // Add to result
        value |= bits << (bitWidth - bitsRemaining);

        bitOffset += bitsToRead;
        bitsRemaining -= bitsToRead;
      }

      return value;
    }
  }
}
