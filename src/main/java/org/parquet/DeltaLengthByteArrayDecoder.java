package org.parquet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.parquet.model.ParquetException;

/**
 * Decoder for DELTA_LENGTH_BYTE_ARRAY encoding.
 * <p>
 * Format:
 * 1. Lengths block: encoded using DELTA_BINARY_PACKED encoding
 * 2. Data block: concatenated byte array data (no separators)
 * <p>
 * Example:
 * Input: ["hello", "world", "test"]
 * - Lengths: [5, 5, 4] (encoded with DELTA_BINARY_PACKED)
 * - Data: "helloworldtest" (concatenated)
 */
public class DeltaLengthByteArrayDecoder {
  private final ByteBuffer buffer;
  private int[] lengths;
  private int currentIndex;
  private int dataOffset;

  public DeltaLengthByteArrayDecoder(ByteBuffer buffer) {
    this.buffer = buffer;
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
    this.currentIndex = 0;
  }

  /**
   * Initialize the decoder by reading the lengths
   */
  public void init(int numValues) {
    // Decode lengths using DELTA_BINARY_PACKED
    DeltaBinaryPackedDecoder lengthDecoder = new DeltaBinaryPackedDecoder(buffer, false);
    lengths = lengthDecoder.decodeInt32(numValues);

    // Remember where the actual data starts (after the length encoding)
    dataOffset = buffer.position();
    currentIndex = 0;
  }

  /**
   * Decode the specified number of byte array values
   */
  public byte[][] decode(int numValues) {
    if (lengths == null) {
      init(numValues);
    }

    byte[][] result = new byte[numValues][];
    buffer.position(dataOffset);

    for (int i = 0; i < numValues && currentIndex < lengths.length; i++) {
      int length = lengths[currentIndex++];
      byte[] value = new byte[length];
      buffer.get(value);
      result[i] = value;
    }

    return result;
  }

  /**
   * Decode a single byte array value
   */
  public byte[] decodeOne() {
    if (currentIndex >= lengths.length) {
      throw new ParquetException("No more values to decode");
    }

    int length = lengths[currentIndex++];
    byte[] value = new byte[length];
    buffer.get(value);
    return value;
  }

  /**
   * Get the number of values remaining
   */
  public int valuesLeft() {
    return lengths == null ? 0 : lengths.length - currentIndex;
  }
}
