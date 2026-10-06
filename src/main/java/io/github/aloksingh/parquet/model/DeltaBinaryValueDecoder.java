package io.github.aloksingh.parquet.model;

import io.github.aloksingh.parquet.DecodeChecks;
import io.github.aloksingh.parquet.DeltaBinaryPackedDecoder;
import java.nio.ByteBuffer;

/**
 * DELTA_BINARY_PACKED value decoding for INT32 and INT64.
 * <p>
 * Format invariants (Parquet encodings spec): a header of block size, miniblock
 * count, and value count (all unsigned varints) precedes a zigzag first value;
 * each miniblock of 32 values is preceded by a zigzag minimum delta and one width
 * byte per miniblock. The header is validated against the present-value count
 * before any value read (see {@link DecodeChecks#validatedDelta}).
 */
final class DeltaBinaryValueDecoder {

  private DeltaBinaryValueDecoder() {
  }

  /**
   * Decodes {@code present} DELTA_BINARY_PACKED values, leaving {@code data} just
   * past the block.
   *
   * @throws ParquetException if the block is malformed or the physical type is not
   *                          INT32 or INT64
   */
  static Object decode(ColumnDescriptor descriptor, ByteBuffer data, int present) {
    DeltaBinaryPackedDecoder delta = present == 0 && !data.hasRemaining() ? null
        : DecodeChecks.validatedDelta(data, present, descriptor.physicalType() == Type.INT64);
    return switch (descriptor.physicalType()) {
      case INT32 -> delta == null ? new int[0] : delta.decodeInt32(present);
      case INT64 -> delta == null ? new long[0] : delta.decodeInt64(present);
      default ->
          throw new ParquetException("Unsupported DELTA_BINARY_PACKED type " + descriptor.physicalType());
    };
  }
}
