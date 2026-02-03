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
  /** The buffer containing encoded length and data blocks */
  private final ByteBuffer buffer;

  /** Array of decoded lengths for each byte array value */
  private int[] lengths;

  /** Current position in the lengths array */
  private int currentIndex;

  /** Offset in buffer where concatenated data begins */
  private int dataOffset;

  /**
   * Constructs a new decoder for DELTA_LENGTH_BYTE_ARRAY encoded data.
   *
   * @param buffer the buffer containing the encoded data in little-endian byte order
   */
  public DeltaLengthByteArrayDecoder(ByteBuffer buffer) {
    this.buffer = buffer;
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
    this.currentIndex = 0;
  }

  /**
   * Initializes the decoder by reading and decoding the lengths block.
   * <p>
   * This method decodes the lengths using DELTA_BINARY_PACKED encoding
   * and records the offset where the concatenated data begins.
   *
   * @param numValues the number of byte array values to decode
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
   * Decodes the specified number of byte array values.
   * <p>
   * If the decoder has not been initialized, it will be initialized with the
   * provided number of values. Each byte array is extracted based on the
   * corresponding length from the lengths array.
   *
   * @param numValues the number of byte array values to decode
   * @return an array of decoded byte arrays
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
   * Decodes a single byte array value.
   * <p>
   * Reads the next length from the lengths array and extracts that many
   * bytes from the data block.
   *
   * @return the decoded byte array
   * @throws ParquetException if no more values are available to decode
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
   * Gets the number of values remaining to be decoded.
   *
   * @return the number of values left, or 0 if not initialized
   */
  public int valuesLeft() {
    return lengths == null ? 0 : lengths.length - currentIndex;
  }
}
