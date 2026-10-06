package io.github.aloksingh.parquet.model;

import io.github.aloksingh.parquet.DecodeChecks;
import java.nio.ByteBuffer;

/**
 * Decoder for DELTA_BYTE_ARRAY encoding.
 * <p>
 * Format (Parquet encodings spec): one DELTA_BINARY_PACKED block of prefix lengths
 * (bytes shared with the previous value), one DELTA_BINARY_PACKED block of suffix
 * lengths, then the concatenated suffix bytes. Each value is the previous value's
 * first {@code prefix} bytes followed by its suffix; the first value has prefix
 * length zero. Decoding validates both blocks first (see
 * {@link DecodeChecks#validatedDelta}) and builds one shared payload with per-value
 * offsets ({@link BinaryValues}).
 */
public final class DeltaByteArrayDecoder {

  private DeltaByteArrayDecoder() {
  }

  /**
   * Decodes {@code count} values from {@code data}, leaving its position just past
   * the consumed payload.
   *
   * @param data      the encoded prefix lengths, suffix lengths, and suffix bytes
   * @param count     the number of present values to decode
   * @param fixedWidth the required value width for FIXED_LEN_BYTE_ARRAY leaves,
   *                   or 0 for variable-length BYTE_ARRAY
   * @return offsets plus the shared reconstructed value payload
   * @throws ParquetException if either block is malformed, a prefix exceeds the
   *                          previous value's length, a suffix length is negative,
   *                          a fixed-width value differs from {@code fixedWidth},
   *                          or the suffix payload is truncated
   */
  public static BinaryValues decode(ByteBuffer data, int count, int fixedWidth) {
    int[] prefixes = DecodeChecks.validatedDelta(data, count, false).decodeInt32(count);
    int[] lengths = DecodeChecks.validatedDelta(data, count, false).decodeInt32(count);
    int[] offsets = new int[count + 1];
    long suffixBytes = 0;
    for (int i = 0; i < count; i++) {
      int previous = i == 0 ? 0 : offsets[i] - offsets[i - 1];
      if (prefixes[i] < 0 || prefixes[i] > previous) {
        throw new ParquetException("Invalid binary prefix " + prefixes[i] + ", previous length " + previous);
      }
      if (lengths[i] < 0) throw new ParquetException("Negative binary suffix length: " + lengths[i]);
      if (fixedWidth > 0 && (long) prefixes[i] + lengths[i] != fixedWidth) {
        throw new ParquetException("Fixed binary value width does not match " + fixedWidth);
      }
      offsets[i + 1] = DecodeChecks.checkedSize((long) offsets[i] + prefixes[i] + lengths[i], "binary payload");
      suffixBytes += lengths[i];
    }
    DecodeChecks.requireBytes(data, suffixBytes, "binary suffixes");
    ByteBuffer payload = ByteBuffer.allocate(offsets[count]);
    for (int i = 0; i < count; i++) {
      int prefix = prefixes[i];
      if (prefix > 0) {
        for (int j = 0; j < prefix; j++) payload.put(offsets[i] + j, payload.get(offsets[i - 1] + j));
      }
      payload.put(offsets[i] + prefix, data, data.position(), lengths[i]);
      data.position(data.position() + lengths[i]);
    }
    return new BinaryValues(offsets, payload);
  }
}
