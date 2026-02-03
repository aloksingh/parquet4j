package org.parquet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Decoder for BYTE_STREAM_SPLIT encoding.
 * <p>
 * BYTE_STREAM_SPLIT is an encoding specifically for floating-point types (FLOAT and DOUBLE).
 * It splits the bytes of each value and groups them together by byte position.
 * <p>
 * For example, if you have float values [v0, v1, v2], each float is 4 bytes.
 * Instead of storing: [v0_b0, v0_b1, v0_b2, v0_b3, v1_b0, v1_b1, v1_b2, v1_b3, ...]
 * BYTE_STREAM_SPLIT stores: [v0_b0, v1_b0, v2_b0, ..., v0_b1, v1_b1, v2_b1, ..., ...]
 * <p>
 * This encoding is efficient for compression because bytes at the same position often
 * have similar values in floating-point data.
 */
public class ByteStreamSplitDecoder {
  private final ByteBuffer buffer;
  private final int numValues;
  private final int bytesPerValue;

  /**
   * Creates a decoder for BYTE_STREAM_SPLIT encoded data.
   *
   * @param buffer        the buffer containing encoded data positioned at the start of the encoded stream
   * @param numValues     the number of values to decode
   * @param bytesPerValue the number of bytes per value (4 for FLOAT, 8 for DOUBLE)
   */
  public ByteStreamSplitDecoder(ByteBuffer buffer, int numValues, int bytesPerValue) {
    this.buffer = buffer;
    this.numValues = numValues;
    this.bytesPerValue = bytesPerValue;
  }

  /**
   * Decodes BYTE_STREAM_SPLIT encoded data into float values.
   * <p>
   * This method reconstructs float values by reading bytes from separate byte streams
   * and reassembling them in little-endian order. The buffer position is advanced
   * by {@code 4 * numValues} bytes after decoding.
   *
   * @return an array of decoded float values
   * @throws IllegalArgumentException if bytesPerValue is not 4
   */
  public float[] decodeFloat() {
    if (bytesPerValue != 4) {
      throw new IllegalArgumentException(
          "Expected 4 bytes per value for FLOAT, got: " + bytesPerValue);
    }

    float[] result = new float[numValues];
    int startPos = buffer.position();

    for (int valueIdx = 0; valueIdx < numValues; valueIdx++) {
      // Collect bytes for this value from each stream
      byte[] valueBytes = new byte[4];
      for (int byteIdx = 0; byteIdx < 4; byteIdx++) {
        valueBytes[byteIdx] = buffer.get(startPos + byteIdx * numValues + valueIdx);
      }
      // Convert bytes to float (little-endian)
      ByteBuffer bb = ByteBuffer.wrap(valueBytes);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      result[valueIdx] = bb.getFloat();
    }

    // Advance buffer position past all consumed data
    buffer.position(startPos + 4 * numValues);

    return result;
  }

  /**
   * Decodes BYTE_STREAM_SPLIT encoded data into double values.
   * <p>
   * This method reconstructs double values by reading bytes from separate byte streams
   * and reassembling them in little-endian order. The buffer position is advanced
   * by {@code 8 * numValues} bytes after decoding.
   *
   * @return an array of decoded double values
   * @throws IllegalArgumentException if bytesPerValue is not 8
   */
  public double[] decodeDouble() {
    if (bytesPerValue != 8) {
      throw new IllegalArgumentException(
          "Expected 8 bytes per value for DOUBLE, got: " + bytesPerValue);
    }

    double[] result = new double[numValues];
    int startPos = buffer.position();

    for (int valueIdx = 0; valueIdx < numValues; valueIdx++) {
      // Collect bytes for this value from each stream
      byte[] valueBytes = new byte[8];
      for (int byteIdx = 0; byteIdx < 8; byteIdx++) {
        valueBytes[byteIdx] = buffer.get(startPos + byteIdx * numValues + valueIdx);
      }
      // Convert bytes to double (little-endian)
      ByteBuffer bb = ByteBuffer.wrap(valueBytes);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      result[valueIdx] = bb.getDouble();
    }

    // Advance buffer position past all consumed data
    buffer.position(startPos + 8 * numValues);

    return result;
  }
}
