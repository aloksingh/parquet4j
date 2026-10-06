package io.github.aloksingh.parquet.model;

import io.github.aloksingh.parquet.DecodeChecks;
import io.github.aloksingh.parquet.RleDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Definition/repetition-level framing shared by the page decoders.
 * <p>
 * Format invariants (Parquet encodings spec): V1 pages prefix each RLE level
 * section with its little-endian byte length; V2 pages declare section lengths in
 * the page header and carry raw hybrid-RLE streams. Hybrid RLE runs carry a varint
 * header whose LSB selects bit-packed (1, groups of 8 values) versus repeated
 * (0, exact count) runs; a short run must fail rather than decode zero-filled
 * values. Level bit width is ceil(log2(maxLevel + 1)).
 */
final class LevelStreams {

  private LevelStreams() {
  }

  static void validateHybrid(ByteBuffer source, int width, int count, String kind) {
      if (width < 0 || width > 32 || count < 0) {
          throw new ParquetException("Invalid " + kind + " width/count: " + width + "/" + count);
      }
      ByteBuffer data = source.duplicate();
      int decoded = 0;
      while (decoded < count) {
          long header = DecodeChecks.unsignedVarInt(data, kind);
          long run = header >>> 1;
          if (run == 0) throw new ParquetException("Zero-length " + kind + " run");
          if ((header & 1) == 0) {
              int bytes = (width + 7) / 8;
              DecodeChecks.requireBytes(data, bytes, kind);
              long value = 0;
              for (int i = 0; i < bytes; i++) value |= (data.get() & 0xffL) << (8 * i);
              if ((value >>> width) != 0) throw new ParquetException("Invalid " + kind + " value for width " + width);
              if (run > count - decoded) throw new ParquetException("Too many " + kind + " values");
              decoded += (int) run;
          } else {
              long values = run * 8;
              long bytes = run * width;
              DecodeChecks.requireBytes(data, bytes, kind);
              if (values > (long) count - decoded + 7) throw new ParquetException("Too many " + kind + " values");
              data.position(data.position() + (int) bytes);
              decoded += (int) Math.min(values, count - decoded);
          }
      }
      if (data.hasRemaining()) throw new ParquetException("Trailing " + kind + " bytes");
  }

  static int[] readV1Levels(ByteBuffer data, int byteLength, int count, int maxLevel, String kind) {
      if (maxLevel == 0) {
          if (byteLength != 0) throw new ParquetException("Unexpected " + kind + " level section");
          return null;
      }
      if (count == 0 && byteLength == 0) return new int[0];
      if (byteLength < 4) throw new ParquetException("Missing " + kind + " level section");
      DecodeChecks.requireBytes(data, byteLength, kind + " level section");
      int length = data.getInt();
      if (length < 0 || 4L + length != byteLength) {
          throw new ParquetException("Invalid " + kind + " level section length: " + length + "/" + byteLength);
      }
      DecodeChecks.requireBytes(data, length, kind + " levels");
      ByteBuffer levels = data.slice().order(ByteOrder.LITTLE_ENDIAN);
      levels.limit(length);
      data.position(data.position() + length);
      return readLevels(levels, count, maxLevel, kind);
  }

  static int[] readLevels(ByteBuffer data, int count, int maxLevel, String kind) {
      if (maxLevel == 0) {
          // Some legacy V2 writers explicitly encode the implicit zero levels.
          // Validate their count/framing rather than rejecting a correct width-zero stream.
          if (data != null && data.hasRemaining()) validateHybrid(data, 0, count, kind + " levels");
          return null;
      }
      if (data == null || !data.hasRemaining()) {
          if (count == 0) return new int[0];
          throw new ParquetException("Missing " + kind + " levels");
      }
      int width = 32 - Integer.numberOfLeadingZeros(maxLevel);
      validateHybrid(data, width, count, kind + " levels");
      int[] levels = new RleDecoder(data, width, count).readAll();
      for (int level : levels) {
          if (level < 0 || level > maxLevel) {
              throw new ParquetException("Invalid " + kind + " level " + level + ", maximum " + maxLevel);
          }
      }
      return levels;
  }
}
