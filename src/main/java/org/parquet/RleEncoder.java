package org.parquet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * RLE/Bit-Packing Hybrid encoder for Parquet.
 * <p>
 * This encoding is used for definition/repetition levels and dictionary indices.
 * The encoding consists of runs of either RLE-encoded values or bit-packed values.
 *
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
 * bit-packed-values := bit packed back to back, from LSB to MSB
 * rle-run := <rle-header> <repeated-value>
 * rle-header := varint-encode( (number of times repeated) << 1)
 * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
 * }
 * </pre>
 */
public class RleEncoder {
  private final int bitWidth;
  private final ByteArrayOutputStream buffer;

  /**
   * Create an RLE encoder.
   *
   * @param bitWidth Bit width of values (0-32)
   * @throws IllegalArgumentException If bitWidth is not between 0 and 32
   */
  public RleEncoder(int bitWidth) {
    if (bitWidth < 0 || bitWidth > 32) {
      throw new IllegalArgumentException("Bit width must be between 0 and 32");
    }
    this.bitWidth = bitWidth;
    this.buffer = new ByteArrayOutputStream();
  }

  /**
   * Encode a list of values using RLE/Bit-Packing Hybrid encoding.
   *
   * @param values Values to encode
   * @return Encoded bytes with 4-byte little-endian length prefix
   * @throws IOException If an I/O error occurs during encoding
   */
  public byte[] encode(List<Integer> values) throws IOException {
    if (values.isEmpty()) {
      // Empty encoding: just the length field (0)
      ByteBuffer result = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
      result.putInt(0);
      return result.array();
    }

    buffer.reset();

    int i = 0;
    while (i < values.size()) {
      // Try to find an RLE run
      int rleRunLength = getRleRunLength(values, i);

      // Use RLE for any repeated run of 3+ values
      if (rleRunLength >= 3) {
        writeRleRun(values.get(i), rleRunLength);
        i += rleRunLength;
      } else {
        // Use bit-packing for non-repeated values
        int bitPackRunLength = getBitPackRunLength(values, i);

        // Only use bit-packing if we have at least 8 values
        // Otherwise, use RLE for small runs to avoid padding issues
        if (bitPackRunLength >= 8) {
          writeBitPackedRun(values, i, bitPackRunLength);
          i += bitPackRunLength;
        } else {
          // Small run: encode each value individually with RLE
          writeRleRun(values.get(i), rleRunLength);
          i += rleRunLength;
        }
      }
    }

    // Prepend the length as a 4-byte little-endian integer
    byte[] encodedData = buffer.toByteArray();
    ByteBuffer result = ByteBuffer.allocate(4 + encodedData.length);
    result.order(ByteOrder.LITTLE_ENDIAN);
    result.putInt(encodedData.length);
    result.put(encodedData);

    return result.array();
  }

  /**
   * Encode a simple array of values using RLE/Bit-Packing Hybrid encoding.
   *
   * @param values Array of values to encode
   * @return Encoded bytes with 4-byte little-endian length prefix
   * @throws IOException If an I/O error occurs during encoding
   */
  public byte[] encode(int[] values) throws IOException {
    List<Integer> list = new ArrayList<>(values.length);
    for (int value : values) {
      list.add(value);
    }
    return encode(list);
  }

  /**
   * Get the length of an RLE run starting at the given position.
   *
   * @param values List of values to analyze
   * @param start Starting position in the list
   * @return Length of consecutive identical values starting at the given position
   */
  private int getRleRunLength(List<Integer> values, int start) {
    if (start >= values.size()) {
      return 0;
    }

    int value = values.get(start);
    int length = 1;

    while (start + length < values.size() && values.get(start + length).equals(value)) {
      length++;
    }

    return length;
  }

  /**
   * Get the length of a bit-packed run starting at the given position.
   * A bit-packed run continues until we encounter a long RLE run.
   * We always pack in multiples of 8 values.
   *
   * @param values List of values to analyze
   * @param start Starting position in the list
   * @return Length of values suitable for bit-packing (at least 1)
   */
  private int getBitPackRunLength(List<Integer> values, int start) {
    int length = 0;
    int i = start;

    while (i < values.size()) {
      int rleRunLength = getRleRunLength(values, i);

      // If we find a long RLE run, stop before it
      if (rleRunLength >= 8) {
        break;
      }

      // Add this short run or single value to the bit-packed run
      length += rleRunLength;
      i += rleRunLength;
    }

    // Ensure we have at least 1 value
    if (length == 0 && start < values.size()) {
      length = 1;
    }

    // Don't return 0
    return Math.max(1, length);
  }

  /**
   * Write an RLE run to the buffer.
   * Encodes the run length and the repeated value according to the Parquet RLE format.
   *
   * @param value The value to repeat
   * @param length Number of times the value is repeated
   * @throws IOException If an I/O error occurs while writing
   */
  private void writeRleRun(int value, int length) throws IOException {
    // RLE header: (length << 1) with LSB = 0 to indicate RLE
    writeUnsignedVarInt(length << 1);

    // Write the repeated value using fixed width
    writeFixedWidthValue(value);
  }

  /**
   * Write a bit-packed run to the buffer.
   * Packs values tightly according to the configured bit width, padding to multiples of 8 values.
   *
   * @param values List of values to encode
   * @param start Starting position in the list
   * @param length Number of values to bit-pack
   * @throws IOException If an I/O error occurs while writing
   */
  private void writeBitPackedRun(List<Integer> values, int start, int length) throws IOException {
    // Pad to multiple of 8 if needed
    int paddedLength = ((length + 7) / 8) * 8;
    int numGroups = paddedLength / 8;

    // Bit-packed header: (numGroups << 1) | 1 with LSB = 1 to indicate bit-packing
    writeUnsignedVarInt((numGroups << 1) | 1);

    // Pack the values
    if (bitWidth == 0) {
      // No data to write for bit width 0
      return;
    }

    // Collect values (padding with 0s if needed)
    int[] valuesToPack = new int[paddedLength];
    for (int i = 0; i < paddedLength; i++) {
      if (start + i < values.size()) {
        valuesToPack[i] = values.get(start + i);
      } else {
        valuesToPack[i] = 0; // Padding
      }
    }

    // Bit-pack the values
    bitPackValues(valuesToPack);
  }

  /**
   * Bit-pack values according to the bit width.
   * Values are packed from LSB to MSB, matching the Parquet spec.
   *
   * @param values Array of values to bit-pack
   * @throws IOException If an I/O error occurs while writing
   */
  private void bitPackValues(int[] values) throws IOException {
    if (bitWidth == 0) {
      return;
    }

    int bitOffset = 0;
    int totalBits = values.length * bitWidth;
    int bytesNeeded = (totalBits + 7) / 8;
    byte[] packed = new byte[bytesNeeded];

    for (int value : values) {
      // Mask value to bit width
      value = value & ((1 << bitWidth) - 1);

      int bitsRemaining = bitWidth;

      while (bitsRemaining > 0) {
        int byteIndex = bitOffset / 8;
        int bitIndex = bitOffset % 8;
        int bitsAvailable = 8 - bitIndex;
        int bitsToWrite = Math.min(bitsRemaining, bitsAvailable);

        // Extract the low bits from the value
        int mask = (1 << bitsToWrite) - 1;
        int bits = value & mask;

        // Place these bits into the byte at the correct position
        packed[byteIndex] |= (byte) (bits << bitIndex);

        // Shift value right to get next bits
        value >>>= bitsToWrite;
        bitOffset += bitsToWrite;
        bitsRemaining -= bitsToWrite;
      }
    }

    buffer.write(packed);
  }

  /**
   * Write a value using the fixed width (rounded up to next byte).
   *
   * @param value The value to write
   * @throws IOException If an I/O error occurs while writing
   */
  private void writeFixedWidthValue(int value) throws IOException {
    if (bitWidth == 0) {
      return;
    }

    int bytesNeeded = (bitWidth + 7) / 8;

    for (int i = 0; i < bytesNeeded; i++) {
      buffer.write((byte) (value & 0xFF));
      value >>>= 8;
    }
  }

  /**
   * Write an unsigned variable-length integer using the VarInt encoding.
   *
   * @param value The integer value to encode
   * @throws IOException If an I/O error occurs while writing
   */
  private void writeUnsignedVarInt(int value) throws IOException {
    while ((value & ~0x7F) != 0) {
      buffer.write((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    buffer.write((byte) (value & 0x7F));
  }

  /**
   * Calculate the bit width needed to represent a maximum value.
   *
   * @param maxValue The maximum value to represent
   * @return Minimum number of bits needed to represent the value
   */
  public static int bitWidth(int maxValue) {
    if (maxValue == 0) {
      return 0;
    }
    return 32 - Integer.numberOfLeadingZeros(maxValue);
  }
}
