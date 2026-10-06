package io.github.aloksingh.parquet.model;

import io.github.aloksingh.parquet.DecodeChecks;
import java.nio.ByteBuffer;

/**
 * Decoder for DELTA_LENGTH_BYTE_ARRAY encoding.
 * <p>
 * Format (Parquet encodings spec): one DELTA_BINARY_PACKED block of value lengths
 * followed by the concatenated value bytes with no separators. Decoding validates
 * the block shape and declared count first (see
 * {@link DecodeChecks#validatedDelta}), then builds one shared payload with
 * per-value offsets ({@link BinaryValues}) so no per-value byte arrays are allocated.
 */
public final class DeltaLengthByteArrayDecoder {

  private DeltaLengthByteArrayDecoder() {
  }

  /**
   * Decodes {@code count} values from {@code data}, leaving its position just past
   * the consumed payload.
   *
   * @param data   the encoded lengths block followed by the concatenated values
   * @param count  the number of present values to decode
   * @return offsets plus the shared value payload
   * @throws ParquetException if the lengths block is malformed, a length is negative,
   *                          or the value payload is truncated
   */
  public static BinaryValues decode(ByteBuffer data, int count) {
    int[] lengths = count == 0 && !data.hasRemaining() ? new int[0]
        : DecodeChecks.validatedDelta(data, count, false).decodeInt32(count);
    int[] offsets = new int[count + 1];
    for (int i = 0; i < count; i++) {
      if (lengths[i] < 0) throw new ParquetException("Negative binary length: " + lengths[i]);
      offsets[i + 1] = DecodeChecks.checkedSize((long) offsets[i] + lengths[i], "binary payload");
    }
    DecodeChecks.requireBytes(data, offsets[count], "binary payload");
    ByteBuffer payload = data.slice();
    payload.limit(offsets[count]);
    data.position(data.position() + offsets[count]);
    return new BinaryValues(offsets, payload);
  }
}
