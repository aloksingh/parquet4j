package io.github.aloksingh.parquet.model;

import io.github.aloksingh.parquet.DecodeChecks;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * PLAIN value decoding for every physical type.
 * <p>
 * Format invariants (Parquet encodings spec): fixed-width primitives are
 * little-endian raw values; BOOLEAN is bit-packed LSB-first, one bit per value;
 * BYTE_ARRAY values carry a little-endian 4-byte length prefix; FIXED_LEN_BYTE_ARRAY
 * and INT96 are raw fixed-width bytes. Decoding validates the whole section size
 * before reading and builds one shared payload with per-value offsets for binary
 * types.
 */
final class PlainValueDecoder {

  private PlainValueDecoder() {
  }

  /**
   * Decodes {@code count} PLAIN values, leaving {@code data} just past the section.
   *
   * @throws ParquetException if the section is truncated or the physical type has
   *                          no PLAIN representation
   */
  static Object decode(ColumnDescriptor descriptor, ByteBuffer data, int count) {
    long required = switch (descriptor.physicalType()) {
      case BOOLEAN -> (count + 7L) / 8;
      case INT32, FLOAT, BYTE_ARRAY -> count * 4L;
      case INT64, DOUBLE -> count * 8L;
      case INT96 -> count * 12L;
      case FIXED_LEN_BYTE_ARRAY -> count * (long) descriptor.typeLength();
    };
    DecodeChecks.requireBytes(data, required, "PLAIN " + descriptor.physicalType());
    return switch (descriptor.physicalType()) {
      case INT32 -> {
        int[] values = new int[count];
        for (int i = 0; i < count; i++) values[i] = data.getInt();
        yield values;
      }
      case INT64 -> {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) values[i] = data.getLong();
        yield values;
      }
      case FLOAT -> {
        float[] values = new float[count];
        for (int i = 0; i < count; i++) values[i] = data.getFloat();
        yield values;
      }
      case DOUBLE -> {
        double[] values = new double[count];
        for (int i = 0; i < count; i++) values[i] = data.getDouble();
        yield values;
      }
      case BOOLEAN -> {
        boolean[] values = new boolean[count];
        int packed = 0;
        for (int i = 0; i < count; i++) {
          if ((i & 7) == 0) packed = data.get() & 0xff;
          values[i] = (packed & (1 << (i & 7))) != 0;
        }
        yield values;
      }
      case FIXED_LEN_BYTE_ARRAY, INT96 -> {
        int width = descriptor.physicalType() == Type.INT96 ? 12 : descriptor.typeLength();
        int[] offsets = new int[count + 1];
        for (int i = 0; i < count; i++) offsets[i + 1] = Math.addExact(offsets[i], width);
        ByteBuffer payload = data.slice();
        payload.limit(offsets[count]);
        data.position(data.position() + offsets[count]);
        yield new BinaryValues(offsets, payload);
      }
      case BYTE_ARRAY -> readPlainBinary(data, count);
      default -> throw new ParquetException("Unsupported PLAIN type " + descriptor.physicalType());
    };
  }

  private static BinaryValues readPlainBinary(ByteBuffer data, int count) {
    int[] offsets = new int[count + 1];
    ByteBuffer scan = data.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < count; i++) {
      DecodeChecks.requireBytes(scan, 4, "binary length");
      int length = scan.getInt();
      DecodeChecks.requireBytes(scan, length, "binary value");
      offsets[i + 1] = Math.addExact(offsets[i], length);
      scan.position(scan.position() + length);
    }
    ByteBuffer payload = ByteBuffer.allocate(offsets[count]);
    for (int i = 0; i < count; i++) {
      int length = data.getInt();
      payload.put(offsets[i], data, data.position(), length);
      data.position(data.position() + length);
    }
    return new BinaryValues(offsets, payload);
  }
}
