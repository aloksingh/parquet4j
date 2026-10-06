package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ParquetException;
import java.nio.ByteBuffer;

/**
 * Low-level byte framing checks and varints shared by the page decoders and the
 * per-encoding value decoders. Every method validates before consuming so a
 * malformed stream fails with context instead of decoding zero-filled or
 * wrapped-around values.
 */
public final class DecodeChecks {

  private DecodeChecks() {
  }

  /** Returns {@code size} as an int, rejecting negative or unrepresentable sizes. */
  public static int checkedSize(long size, String kind) {
    if (size < 0 || size > Integer.MAX_VALUE) throw new ParquetException("Invalid " + kind + " size: " + size);
    return (int) size;
  }

  /** Rejects unconsumed bytes so over-long sections cannot hide a framing error. */
  public static void requireConsumed(ByteBuffer data, String kind) {
    if (data.hasRemaining()) throw new ParquetException("Trailing " + kind + " bytes: " + data.remaining());
  }

  /** Rejects short buffers before any allocation or positional access of {@code count} bytes. */
  public static void requireBytes(ByteBuffer data, long count, String kind) {
    if (count < 0 || count > data.remaining()) {
      throw new ParquetException("Truncated " + kind + ": need " + count + " bytes, remaining " + data.remaining());
    }
  }

  /** Reads one LEB128 varint capped at 35 bits (the size/count framing widths). */
  public static long unsignedVarInt(ByteBuffer data, String kind) {
    long value = 0;
    for (int shift = 0; shift <= 28; shift += 7) {
      requireBytes(data, 1, kind + " varint");
      int b = data.get() & 0xff;
      if (shift == 28 && (b & 0xf0) != 0) {
        throw new ParquetException("Invalid " + kind + " varint");
      }
      value |= (long) (b & 0x7f) << shift;
      if ((b & 0x80) == 0) return value;
    }
    throw new ParquetException("Invalid " + kind + " varint");
  }

  /**
   * Reads one zigzag-encoded signed varint. The 32-bit carrier reuses the 35-bit
   * unsigned reader; the 64-bit carrier decodes at most 10 bytes and rejects
   * continuations past the 64-bit range.
   */
  public static long zigzagVarLong(ByteBuffer data, boolean is64Bit, String kind) {
    long encoded;
    if (!is64Bit) {
      encoded = unsignedVarInt(data, kind);
    } else {
      encoded = 0;
      boolean complete = false;
      for (int shift = 0; shift <= 63; shift += 7) {
        requireBytes(data, 1, kind + " varint");
        int value = data.get() & 0xff;
        if (shift == 63 && (value & 0xfe) != 0) throw new ParquetException("Invalid " + kind + " varint");
        encoded |= (long) (value & 0x7f) << shift;
        if ((value & 0x80) == 0) {
          complete = true;
          break;
        }
      }
      if (!complete) throw new ParquetException("Invalid " + kind + " varint");
    }
    return (encoded >>> 1) ^ -(encoded & 1);
  }

  /**
   * Validates the DELTA_BINARY_PACKED header and miniblock layout of {@code source}
   * without consuming it, then returns the decoder positioned at the block start.
   * The pre-scan bounds every later read: the declared value count must equal
   * {@code expected} (the count of present values), and block shape must follow the
   * Parquet encodings spec (block size a positive multiple of 128, miniblocks of 32
   * values, widths within the carrier width).
   */
  public static DeltaBinaryPackedDecoder validatedDelta(ByteBuffer source, int expected, boolean is64Bit) {
    ByteBuffer data = source.duplicate();
    long block = unsignedVarInt(data, "delta block size");
    long minis = unsignedVarInt(data, "delta miniblock count");
    long encoded = unsignedVarInt(data, "delta value count");
    if (block == 0 || block > Integer.MAX_VALUE || block % 128 != 0 || minis == 0
        || minis > block || block % minis != 0 || (block / minis) % 32 != 0) {
      throw new ParquetException("Invalid delta block/miniblock size: " + block + "/" + minis);
    }
    if (encoded != expected) {
      throw new ParquetException("Delta value count " + encoded + " does not match present count " + expected);
    }
    zigzagVarLong(data, is64Bit, "delta first value");
    int decoded = expected == 0 ? 0 : 1;
    while (decoded < expected) {
      zigzagVarLong(data, is64Bit, "minimum delta");
      requireBytes(data, minis, "delta miniblock widths");
      int widths = data.position();
      data.position(widths + (int) minis);
      for (int mini = 0; mini < minis && decoded < expected; mini++) {
        int width = data.get(widths + mini) & 0xff;
        if (width > (is64Bit ? 64 : 32)) throw new ParquetException("Invalid delta bit width: " + width);
        long bytes = (block / minis) * width / 8;
        requireBytes(data, bytes, "delta miniblock");
        data.position(data.position() + (int) bytes);
        decoded += (int) Math.min(block / minis, expected - decoded);
      }
    }
    return new DeltaBinaryPackedDecoder(source, is64Bit);
  }
}
