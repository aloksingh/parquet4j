package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ParquetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Bounded DELTA_BINARY_PACKED decoding. Headers, used miniblock widths and full
 * padded payloads are checked before allocating output. Miniblock tables and
 * padding are read in place, never allocated according to an untrusted block size.
 */
public class DeltaBinaryPackedDecoder {
  private final ByteBuffer buffer;
  private final boolean is64Bit;
  private final int maxValues;
  private final int startPosition;
  private final int blockSize;
  private final int numMiniBlocks;
  private final int totalValueCount;
  private final int valuesPerMiniBlock;
  private final long firstValue;

  /**
   * Constructs a decoder with the safe default value-count bound (16 Mi values).
   * @param buffer encoded stream; its position advances as values are decoded
   * @param is64Bit true for INT64, false for INT32
   */
  public DeltaBinaryPackedDecoder(ByteBuffer buffer, boolean is64Bit) {
    this(buffer, is64Bit, PageReadOptions.DEFAULT.maxValuesPerPage());
  }

  /**
   * Constructs a decoder bounded by the caller's page/non-null value count.
   * The bound does not restrict the legal block shape: a 128-value block can hold
   * a two-value page, but no output/padding arrays are allocated for that block.
   * @param buffer encoded stream
   * @param is64Bit true for INT64, false for INT32
   * @param maxValues maximum permitted wire and requested value count (may be zero)
   * @throws IllegalArgumentException if the bound is negative
   * @throws ParquetException for malformed/truncated data or counts exceeding the bound
   */
  public DeltaBinaryPackedDecoder(ByteBuffer buffer, boolean is64Bit, int maxValues) {
    if (maxValues < 0) throw new IllegalArgumentException("Negative DELTA value count bound");
    this.buffer = Objects.requireNonNull(buffer, "buffer");
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
    this.is64Bit = is64Bit;
    this.maxValues = maxValues;
    this.startPosition = buffer.position();
    this.blockSize = readUnsignedVarInt(buffer, "block size");
    this.numMiniBlocks = readUnsignedVarInt(buffer, "miniblock count");
    this.totalValueCount = readUnsignedVarInt(buffer, "value count");
    if (blockSize == 0 || blockSize % 128 != 0 || numMiniBlocks == 0
        || blockSize % numMiniBlocks != 0 || (blockSize / numMiniBlocks) % 32 != 0) {
      throw new ParquetException("Invalid DELTA block/miniblock shape");
    }
    if (totalValueCount > maxValues) {
      throw new ParquetException("DELTA value count " + totalValueCount + " exceeds limit " + maxValues);
    }
    this.valuesPerMiniBlock = blockSize / numMiniBlocks;
    this.firstValue = readZigzagVarLong(buffer);
    validateSignedWidth(firstValue);
    // Validate without advancing the caller or allocating tables/output arrays.
    validatePayload(buffer.duplicate());
  }

  /** @return the bytes actually consumed from the caller buffer since construction */
  public int getBytesConsumed() {
    return buffer.position() - startPosition;
  }

  /** @return the bounded wire count read from the header */
  public int getTotalValueCount() {
    return totalValueCount;
  }

  /**
   * Decodes a requested prefix. Positive prefix requests consume the full encoded
   * stream so consecutive DELTA streams remain correctly aligned.
   * @param expectedValues requested count, no greater than the bounded wire count
   * @return decoded INT32 values
   * @throws ParquetException if the stream supplies fewer values than requested
   */
  public int[] decodeInt32(int expectedValues) {
    validateRequested(expectedValues);
    int[] result = new int[expectedValues];
    if (expectedValues != 0) decodeValues(expectedValues, result, null);
    return result;
  }

  /**
   * @param expectedValues requested count, no greater than the bounded wire count
   * @return decoded INT64 values, consuming the full stream for positive prefix requests
   * @throws ParquetException if the stream supplies fewer values than requested
   */
  public long[] decodeInt64(int expectedValues) {
    validateRequested(expectedValues);
    long[] result = new long[expectedValues];
    if (expectedValues != 0) decodeValues(expectedValues, null, result);
    return result;
  }

  private void validateRequested(int requested) {
    if (requested < 0) throw new IllegalArgumentException("Negative requested DELTA value count");
    if (requested > maxValues || requested > totalValueCount) {
      throw new ParquetException("Requested DELTA value count " + requested
          + " exceeds wire count " + totalValueCount + " or limit " + maxValues);
    }
  }

  private void validatePayload(ByteBuffer input) {
    int remaining = Math.max(0, totalValueCount - 1);
    while (remaining > 0) {
      validateSignedWidth(readZigzagVarLong(input));
      int widths = readWidths(input);
      int inBlock = Math.min(blockSize, remaining);
      for (int mini = 0; inBlock > 0; mini++) {
        int width = input.get(widths + mini) & 0xff;
        int bytes = payloadBytes(input, width);
        input.position(input.position() + bytes);
        int count = Math.min(valuesPerMiniBlock, inBlock);
        inBlock -= count;
        remaining -= count;
      }
      // Unused width bytes may be arbitrary per the pinned Parquet specification;
      // no corresponding payload exists and their values must not be validated.
    }
  }

  private void decodeValues(int requested, int[] intValues, long[] longValues) {
    long last = firstValue;
    if (intValues != null) intValues[0] = (int) last; else longValues[0] = last;
    int written = 1;
    int remaining = Math.max(0, totalValueCount - 1);
    while (remaining > 0) {
      long minDelta = readZigzagVarLong(buffer);
      validateSignedWidth(minDelta);
      int widths = readWidths(buffer);
      int inBlock = Math.min(blockSize, remaining);
      for (int mini = 0; inBlock > 0; mini++) {
        int width = buffer.get(widths + mini) & 0xff;
        int bytes = payloadBytes(buffer, width);
        int start = buffer.position();
        buffer.position(start + bytes); // consume the full padded miniblock
        int count = Math.min(valuesPerMiniBlock, inBlock);
        int wanted = Math.min(count, requested - written);
        for (int i = 0; i < wanted; i++) {
          last += minDelta + unpack(buffer, start, (long) i * width, width);
          if (intValues != null) intValues[written++] = (int) last;
          else longValues[written++] = last;
        }
        inBlock -= count;
        remaining -= count;
      }
    }
  }

  private int readWidths(ByteBuffer input) {
    if (numMiniBlocks > input.remaining()) throw new ParquetException("Truncated DELTA miniblock width table");
    int start = input.position();
    input.position(start + numMiniBlocks);
    return start;
  }

  private int payloadBytes(ByteBuffer input, int width) {
    int maximum = is64Bit ? 64 : 32;
    if (width > maximum) throw new ParquetException("DELTA miniblock bit width exceeds " + maximum + ": " + width);
    long bytes = (long) valuesPerMiniBlock * width / 8;
    if (bytes > input.remaining()) throw new ParquetException("Truncated DELTA miniblock payload");
    return (int) bytes;
  }

  private void validateSignedWidth(long value) {
    if (!is64Bit && (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)) {
      throw new ParquetException("DELTA INT32 first value/minimum delta is out of range");
    }
  }

  private static long unpack(ByteBuffer input, int start, long bitOffset, int width) {
    long value = 0;
    int written = 0;
    while (written < width) {
      int bit = (int) (bitOffset & 7);
      int count = Math.min(width - written, 8 - bit);
      int bits = (input.get(start + (int) (bitOffset >>> 3)) & 0xff) >>> bit;
      value |= (long) (bits & ((1 << count) - 1)) << written;
      written += count;
      bitOffset += count;
    }
    return value;
  }

  private static int readUnsignedVarInt(ByteBuffer input, String field) {
    long value = 0;
    for (int index = 0; index < 5; index++) {
      if (!input.hasRemaining()) throw new ParquetException("Truncated DELTA " + field + " varint");
      int b = input.get() & 0xff;
      if (index == 4 && (b & 0xf0) != 0) throw new ParquetException("Overflowed DELTA " + field + " varint");
      value |= (long) (b & 0x7f) << (7 * index);
      if ((b & 0x80) == 0) {
        if (value > Integer.MAX_VALUE) throw new ParquetException("DELTA " + field + " exceeds supported integer range");
        return (int) value;
      }
    }
    throw new ParquetException("Overflowed DELTA " + field + " varint");
  }

  private static long readZigzagVarLong(ByteBuffer input) {
    long value = 0;
    for (int index = 0; index < 10; index++) {
      if (!input.hasRemaining()) throw new ParquetException("Truncated DELTA signed varint");
      int b = input.get() & 0xff;
      if (index == 9 && (b & 0xfe) != 0) throw new ParquetException("Overflowed DELTA signed varint");
      value |= (long) (b & 0x7f) << (7 * index);
      if ((b & 0x80) == 0) return (value >>> 1) ^ -(value & 1);
    }
    throw new ParquetException("Overflowed DELTA signed varint");
  }
}
