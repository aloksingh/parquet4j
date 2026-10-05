package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ParquetException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Parquet RLE/bit-packed hybrid decoding for widths 0–32. The input is the raw
 * run stream: callers remove V1's four-byte length prefix (V2 has no prefix).
 * Run headers and complete payloads are validated before yielding any values;
 * repeated runs use scalar state rather than allocating their advertised length.
 */
public class RleDecoder {
  private final ByteBuffer buffer;
  private final int bitWidth;
  private final int totalValues;
  private final long mask;
  private int valuesRead;
  private int runRemaining;
  private int repeatedValue;
  private BitPackedReader.IntReader packed;

  /**
   * Creates a decoder without consuming or changing the caller's buffer.
   * @param buffer raw hybrid-encoded run stream
   * @param bitWidth width from 0 through 32
   * @param totalValues number of values requested (may be a prefix of a run)
   * @throws IllegalArgumentException if width or count is invalid
   */
  public RleDecoder(ByteBuffer buffer, int bitWidth, int totalValues) {
    if (bitWidth < 0 || bitWidth > 32 || totalValues < 0) {
      throw new IllegalArgumentException("RLE bit width must be 0–32 and value count nonnegative");
    }
    this.buffer = Objects.requireNonNull(buffer, "buffer").slice().asReadOnlyBuffer();
    this.bitWidth = bitWidth;
    this.totalValues = totalValues;
    this.mask = (1L << bitWidth) - 1;
  }

  /**
   * Decodes the remaining requested values, filling repeated runs in bulk.
   * @return all as-yet-undecoded values
   * @throws ParquetException if a run header or full run payload is malformed/truncated
   */
  public int[] readAll() {
    int[] result = new int[totalValues - valuesRead];
    int offset = 0;
    while (offset < result.length) {
      if (runRemaining == 0) readRun();
      int count = Math.min(runRemaining, result.length - offset);
      if (packed == null) {
        Arrays.fill(result, offset, offset + count, repeatedValue);
      } else {
        packed.readInto(result, offset, count);
      }
      offset += count;
      valuesRead += count;
      runRemaining -= count;
    }
    return result;
  }

  /**
   * Reads one requested value. At width 32 any int bit pattern (including -1) is
   * a valid value; use the declared value count rather than a sentinel to loop.
   * @return next value, or -1 only after the requested count is exhausted
   * @throws ParquetException if the encoded stream ends before that count
   */
  public int readNext() {
    if (valuesRead == totalValues) return -1;
    if (runRemaining == 0) readRun();
    int value = packed == null ? repeatedValue : packed.read();
    valuesRead++;
    runRemaining--;
    return value;
  }

  private void readRun() {
    int header = readUnsignedVarInt();
    int count = header >>> 1; // The header is unsigned, including the high bit.
    if (count == 0) throw new ParquetException("Zero-length RLE/bit-packed run");
    if ((header & 1) == 0) {
      int bytes = (bitWidth + 7) / 8;
      if (bytes > buffer.remaining()) throw new ParquetException("Truncated RLE repeated value");
      long value = 0;
      for (int i = 0; i < bytes; i++) value |= (buffer.get() & 0xffL) << (8 * i);
      if (value > mask) throw new ParquetException("RLE repeated value exceeds its bit width");
      repeatedValue = (int) value;
      packed = null;
      runRemaining = Math.min(count, totalValues - valuesRead);
    } else {
      long values = (long) count * 8;
      long bytes = (long) count * bitWidth;
      if (values > Integer.MAX_VALUE) throw new ParquetException("Bit-packed run value count overflows");
      if (bytes > buffer.remaining()) throw new ParquetException("Truncated bit-packed run payload");
      // Even a partial final group must supply its entire on-disk payload. Slice
      // and advance over all groups, but unpack only the values the caller needs.
      ByteBuffer payload = buffer.slice(buffer.position(), (int) bytes);
      buffer.position(buffer.position() + (int) bytes);
      packed = new BitPackedReader.IntReader(payload, bitWidth);
      runRemaining = (int) Math.min(values, totalValues - valuesRead);
    }
  }

  private int readUnsignedVarInt() {
    long value = 0;
    for (int index = 0; index < 5; index++) {
      if (!buffer.hasRemaining()) throw new ParquetException("Truncated RLE run header");
      int b = buffer.get() & 0xff;
      if (index == 4 && (b & 0xf0) != 0) {
        throw new ParquetException("RLE run header exceeds unsigned 32 bits");
      }
      value |= (long) (b & 0x7f) << (7 * index);
      if ((b & 0x80) == 0) return (int) value;
    }
    throw new ParquetException("Unterminated RLE run header");
  }
}
