package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ParquetException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * BYTE_STREAM_SPLIT decoding for FLOAT and DOUBLE. Byte planes are reassembled
 * directly into primitive bit patterns: decoding allocates only its output array,
 * not a byte array or ByteBuffer wrapper per value. Caller buffer order is ignored
 * and preserved; its position advances by exactly the consumed encoded bytes.
 */
public class ByteStreamSplitDecoder {
  private final ByteBuffer buffer;
  private final int numValues;
  private final int bytesPerValue;
  private final int dataBytes;

  /**
   * @param buffer encoded data at the start of its byte planes
   * @param numValues nonnegative number of values
   * @param bytesPerValue 4 for FLOAT or 8 for DOUBLE
   * @throws IllegalArgumentException if the width or count is invalid
   * @throws ParquetException if the complete payload is unavailable
   */
  public ByteStreamSplitDecoder(ByteBuffer buffer, int numValues, int bytesPerValue) {
    if (numValues < 0 || (bytesPerValue != 4 && bytesPerValue != 8)) {
      throw new IllegalArgumentException("BYTE_STREAM_SPLIT requires a nonnegative count and width 4 or 8");
    }
    this.buffer = Objects.requireNonNull(buffer, "buffer");
    this.numValues = numValues;
    this.bytesPerValue = bytesPerValue;
    long bytes = (long) numValues * bytesPerValue;
    if (bytes > buffer.remaining()) {
      throw new ParquetException("Truncated BYTE_STREAM_SPLIT payload: need " + bytes
          + " bytes, have " + buffer.remaining());
    }
    this.dataBytes = (int) bytes;
  }

  /**
   * @return decoded float values, including their original NaN payload/signed-zero bits
   * @throws IllegalArgumentException if configured for DOUBLE
   * @throws ParquetException if the caller changed the buffer to truncate the payload
   */
  public float[] decodeFloat() {
    if (bytesPerValue != 4) {
      throw new IllegalArgumentException("Expected 4 bytes per FLOAT value, got " + bytesPerValue);
    }
    checkPayload();
    float[] result = new float[numValues];
    int start = buffer.position();
    for (int i = 0; i < numValues; i++) {
      int bits = (buffer.get(start + i) & 0xff)
          | ((buffer.get(start + numValues + i) & 0xff) << 8)
          | ((buffer.get(start + 2 * numValues + i) & 0xff) << 16)
          | ((buffer.get(start + 3 * numValues + i) & 0xff) << 24);
      result[i] = Float.intBitsToFloat(bits);
    }
    buffer.position(start + dataBytes);
    return result;
  }

  /**
   * @return decoded double values, including their original NaN payload/signed-zero bits
   * @throws IllegalArgumentException if configured for FLOAT
   * @throws ParquetException if the caller changed the buffer to truncate the payload
   */
  public double[] decodeDouble() {
    if (bytesPerValue != 8) {
      throw new IllegalArgumentException("Expected 8 bytes per DOUBLE value, got " + bytesPerValue);
    }
    checkPayload();
    double[] result = new double[numValues];
    int start = buffer.position();
    for (int i = 0; i < numValues; i++) {
      long bits = (buffer.get(start + i) & 0xffL)
          | ((buffer.get(start + numValues + i) & 0xffL) << 8)
          | ((buffer.get(start + 2 * numValues + i) & 0xffL) << 16)
          | ((buffer.get(start + 3 * numValues + i) & 0xffL) << 24)
          | ((buffer.get(start + 4 * numValues + i) & 0xffL) << 32)
          | ((buffer.get(start + 5 * numValues + i) & 0xffL) << 40)
          | ((buffer.get(start + 6 * numValues + i) & 0xffL) << 48)
          | ((buffer.get(start + 7 * numValues + i) & 0xffL) << 56);
      result[i] = Double.longBitsToDouble(bits);
    }
    buffer.position(start + dataBytes);
    return result;
  }

  private void checkPayload() {
    if (dataBytes > buffer.remaining()) {
      throw new ParquetException("Truncated BYTE_STREAM_SPLIT payload");
    }
  }
}
