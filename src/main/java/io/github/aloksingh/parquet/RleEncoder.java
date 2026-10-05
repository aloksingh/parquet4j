package io.github.aloksingh.parquet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;

/**
 * Parquet RLE/bit-packed hybrid encoder for widths 0–32. Primitive arrays are
 * encoded directly, without per-value boxing or temporary unpacked run arrays.
 * V1 {@code encode} methods retain their four-byte little-endian length prefix;
 * {@link #encodeRaw(int[])} produces unprefixed runs for V2 levels or dictionary IDs.
 */
public class RleEncoder {
  private final int bitWidth;
  private final long mask;

  /**
   * @param bitWidth value width from 0 through 32
   * @throws IllegalArgumentException if the width is invalid
   */
  public RleEncoder(int bitWidth) {
    if (bitWidth < 0 || bitWidth > 32) {
      throw new IllegalArgumentException("Bit width must be between 0 and 32");
    }
    this.bitWidth = bitWidth;
    this.mask = (1L << bitWidth) - 1;
  }

  /**
   * Encodes V1 data, preserving the four-byte little-endian byte-length prefix.
   * @param values the values to encode
   * @return prefixed hybrid runs
   * @throws IOException if encoding fails
   * @throws IllegalArgumentException if a value does not fit the configured width
   */
  public byte[] encode(List<Integer> values) throws IOException {
    Objects.requireNonNull(values, "values");
    int[] primitive = new int[values.size()];
    int index = 0;
    for (int value : values) primitive[index++] = value;
    return encode(primitive);
  }

  /**
   * Encodes V1 data directly from primitive values.
   * @param values the values to encode
   * @return four-byte little-endian byte length followed by hybrid runs
   * @throws IOException if encoding fails
   * @throws IllegalArgumentException if a value does not fit the configured width
   */
  public byte[] encode(int[] values) throws IOException {
    byte[] raw = encodeRaw(values);
    return ByteBuffer.allocate(Math.addExact(Integer.BYTES, raw.length)).order(ByteOrder.LITTLE_ENDIAN)
        .putInt(raw.length).put(raw).array();
  }

  /**
   * Encodes raw hybrid runs, with no V1 byte-length prefix.
   * @param values the values to encode (all int bit patterns are valid at width 32)
   * @return raw hybrid runs suitable for V2 level sections
   * @throws IOException if encoding fails
   * @throws IllegalArgumentException if a value does not fit the configured width
   */
  public byte[] encodeRaw(int[] values) throws IOException {
    Objects.requireNonNull(values, "values");
    for (int value : values) {
      if ((value & 0xffffffffL) > mask) {
        throw new IllegalArgumentException("Value " + value + " does not fit bit width " + bitWidth);
      }
    }
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    int offset = 0;
    while (offset < values.length) {
      int repeated = repeatedLength(values, offset);
      if (repeated >= 3) {
        writeRepeated(output, values[offset], repeated);
        offset += repeated;
      } else {
        int packed = packedLength(values, offset);
        if (packed >= 8) {
          // Only full groups before another run; padding here would consume that
          // next run's logical values. Small tails use repeated runs instead.
          int length = packed / 8 * 8;
          writePacked(output, values, offset, length);
          offset += length;
        } else {
          writeRepeated(output, values[offset], repeated);
          offset += repeated;
        }
      }
    }
    return output.toByteArray();
  }

  private static int repeatedLength(int[] values, int start) {
    int count = 1;
    while (start + count < values.length && values[start + count] == values[start]) count++;
    return count;
  }

  private static int packedLength(int[] values, int start) {
    int offset = start;
    while (offset < values.length) {
      int repeated = repeatedLength(values, offset);
      if (repeated >= 8) break;
      offset += repeated;
    }
    return Math.max(1, offset - start);
  }

  private void writeRepeated(ByteArrayOutputStream output, int value, int length) {
    writeUnsignedVarInt(output, length << 1);
    for (int i = 0; i < (bitWidth + 7) / 8; i++) {
      output.write(value & 0xff);
      value >>>= 8;
    }
  }

  private void writePacked(ByteArrayOutputStream output, int[] values, int start, int length) {
    writeUnsignedVarInt(output, (length / 8 << 1) | 1);
    if (bitWidth == 0) return;
    long word = 0;
    int bits = 0;
    for (int i = start; i < start + length; i++) {
      word |= (values[i] & mask) << bits;
      bits += bitWidth;
      while (bits >= Byte.SIZE) {
        output.write((int) word & 0xff);
        word >>>= Byte.SIZE;
        bits -= Byte.SIZE;
      }
    }
    if (bits > 0) output.write((int) word & 0xff);
  }

  private static void writeUnsignedVarInt(ByteArrayOutputStream output, int value) {
    while ((value & ~0x7f) != 0) {
      output.write((value & 0x7f) | 0x80);
      value >>>= 7;
    }
    output.write(value);
  }

  /**
   * @param maxValue maximum value, or an unsigned int bit pattern
   * @return the required width (negative int bit patterns require width 32)
   */
  public static int bitWidth(int maxValue) {
    return Integer.SIZE - Integer.numberOfLeadingZeros(maxValue);
  }
}
