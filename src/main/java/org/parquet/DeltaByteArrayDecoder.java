package org.parquet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Decoder for DELTA_BYTE_ARRAY encoding.
 * <p>
 * This encoding is used for byte arrays (typically strings) where many values share common prefixes.
 * It exploits this by storing prefix lengths and suffixes separately.
 * <p>
 * Format (from Apache Parquet spec):
 * 1. Prefix lengths block: encoded using DELTA_BINARY_PACKED encoding
 * 2. Suffix data: encoded using DELTA_LENGTH_BYTE_ARRAY encoding, which consists of:
 *    a. Suffix lengths: encoded using DELTA_BINARY_PACKED encoding
 *    b. Suffix bytes: concatenated suffix data
 * <p>
 * Example:
 * Input: ["apple", "apply", "application"]
 * - Prefix lengths: [0, 4, 4]  (first value has no prefix, others share "appl")
 * - Suffix lengths: [5, 1, 7]
 * - Suffix data: "applyication" (concatenated)
 * <p>
 * The decoder reconstructs values by taking the prefix from the previous value (up to prefix_length)
 * and appending the suffix.
 */
public class DeltaByteArrayDecoder {
  private final ByteBuffer buffer;
  private int[] prefixLengths;

  public DeltaByteArrayDecoder(ByteBuffer buffer) {
    this.buffer = buffer;
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Decode the specified number of byte array values
   */
  public byte[][] decode(int numValues) {
    if (numValues == 0) {
      return new byte[0][];
    }

    // Step 1: Decode prefix lengths using DELTA_BINARY_PACKED
    DeltaBinaryPackedDecoder prefixDecoder = new DeltaBinaryPackedDecoder(buffer, false);
    int totalPrefixes = prefixDecoder.getTotalValueCount();
    prefixLengths = prefixDecoder.decodeInt32(totalPrefixes);

    // Verify that the block contains exactly the number of values we expect
    if (totalPrefixes != numValues) {
      throw new RuntimeException("DELTA_BYTE_ARRAY block contains " + totalPrefixes +
          " values but expected " + numValues);
    }

    // Step 2: Decode suffix data using DELTA_LENGTH_BYTE_ARRAY encoding
    // This consists of: suffix lengths (DELTA_BINARY_PACKED) followed by concatenated suffix bytes
    DeltaBinaryPackedDecoder suffixLengthDecoder = new DeltaBinaryPackedDecoder(buffer, false);
    int totalSuffixes = suffixLengthDecoder.getTotalValueCount();
    int[] suffixLengths = suffixLengthDecoder.decodeInt32(totalSuffixes);

    // Verify that prefix and suffix counts match
    if (totalPrefixes != totalSuffixes) {
      throw new RuntimeException("DELTA_BYTE_ARRAY prefix count (" + totalPrefixes +
          ") != suffix count (" + totalSuffixes + ")");
    }

    // Step 3: Read all suffix data (concatenated byte arrays)
    // Calculate total suffix bytes needed
    int totalSuffixBytes = 0;
    for (int suffixLength : suffixLengths) {
      totalSuffixBytes += suffixLength;
    }

    // Read all suffix data at once
    byte[] allSuffixData = new byte[totalSuffixBytes];
    buffer.get(allSuffixData);

    // Step 4: Reconstruct values by combining prefixes and suffixes
    byte[][] result = new byte[numValues][];
    byte[] previousValue = new byte[0];
    int suffixDataOffset = 0;

    for (int i = 0; i < numValues; i++) {
      int prefixLength = prefixLengths[i];
      int suffixLength = suffixLengths[i];

      // Reconstruct value: prefix + suffix
      byte[] value = new byte[prefixLength + suffixLength];

      // Copy prefix from previous value
      if (prefixLength > 0) {
        System.arraycopy(previousValue, 0, value, 0, prefixLength);
      }

      // Copy suffix from concatenated suffix data
      if (suffixLength > 0) {
        System.arraycopy(allSuffixData, suffixDataOffset, value, prefixLength, suffixLength);
        suffixDataOffset += suffixLength;
      }

      result[i] = value;
      previousValue = value;
    }

    return result;
  }
}
