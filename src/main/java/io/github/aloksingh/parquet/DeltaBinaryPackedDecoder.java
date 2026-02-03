package io.github.aloksingh.parquet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Decoder for DELTA_BINARY_PACKED encoding.
 * <p>
 * Format (from Apache Parquet spec):
 * - Header:
 * - block_size (varint) - number of values in each block (multiple of 128)
 * - num_mini_blocks (varint) - number of mini-blocks per block
 * - total_value_count (varint) - total values in the page
 * - first_value (zigzag varint) - the first value
 * - For each block:
 * - min_delta (zigzag varint) - minimum delta in this block
 * - For each mini-block:
 * - bit_width (1 byte) - bits needed for deltas in this mini-block
 * - bit-packed deltas - packed values representing (value[i] - value[i-1] - min_delta)
 */
public class DeltaBinaryPackedDecoder {
  private final ByteBuffer buffer;
  private final boolean is64Bit;
  private final int startPosition;

  // Header fields
  private int blockSize;
  private int numMiniBlocks;
  private int totalValueCount;
  private long firstValue;

  // Per-block state
  private long minDelta;
  private int valuesPerMiniBlock;
  private byte[] miniBlockBitWidths;

  // Decoding state
  private long lastValue;
  private boolean firstValueConsumed;
  private int blockEndOffset;

  /**
   * Constructs a new DELTA_BINARY_PACKED decoder for the given buffer.
   *
   * @param buffer the ByteBuffer containing encoded DELTA_BINARY_PACKED data
   * @param is64Bit true if decoding 64-bit integers, false for 32-bit integers
   */
  public DeltaBinaryPackedDecoder(ByteBuffer buffer, boolean is64Bit) {
    this.buffer = buffer;
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
    this.is64Bit = is64Bit;
    this.firstValueConsumed = false;
    this.startPosition = buffer.position();
    // Initialize header immediately to ensure it's only read once
    initializeHeader();
  }

  /**
   * Get the number of bytes consumed from the buffer since construction.
   * <p>
   * This method returns the total number of bytes read from the buffer during decoding.
   * If all values have been consumed and a block end offset is available, it returns
   * the maximum of the current position and block end offset to account for any trailing
   * padding or unread miniblocks.
   *
   * @return the number of bytes consumed from the buffer
   */
  public int getBytesConsumed() {
    int currentOffset = buffer.position() - startPosition;
    // If all values have been consumed and we have a block end offset,
    // return the maximum of current position and block end offset
    // to account for any trailing padding or unread miniblocks
    if (blockEndOffset > 0) {
      return Math.max(currentOffset, blockEndOffset - startPosition);
    }
    return currentOffset;
  }

  /**
   * Get the total number of values in this DELTA_BINARY_PACKED block.
   * <p>
   * This value is read from the header during initialization and represents
   * the total count of values encoded in this DELTA_BINARY_PACKED stream.
   *
   * @return the total number of encoded values
   */
  public int getTotalValueCount() {
    return totalValueCount;
  }

  /**
   * Decode all values as 32-bit integers.
   * <p>
   * This method decodes up to {@code expectedValues} from the DELTA_BINARY_PACKED stream.
   * It processes all encoded blocks and mini-blocks to ensure all bytes are consumed from
   * the buffer, even if fewer values are requested than are encoded.
   * <p>
   * The decoder maintains internal state and processes:
   * <ul>
   *   <li>The first value (stored directly in the header)</li>
   *   <li>All subsequent blocks containing mini-blocks of bit-packed delta values</li>
   * </ul>
   *
   * @param expectedValues the number of values to decode and return
   * @return an array of decoded 32-bit integer values
   */
  public int[] decodeInt32(int expectedValues) {
    // Guard against zero expectedValues to avoid division by zero in header initialization
    if (expectedValues == 0) {
      return new int[0];
    }

    int[] result = new int[expectedValues];
    int index = 0;

    // Consume first value
    if (index < expectedValues && index < totalValueCount) {
      result[index++] = (int) firstValue;
      lastValue = firstValue;
      firstValueConsumed = true;
    }

    // Track how many values have been processed from the encoded blocks
    int valuesProcessed = 1; // We've processed the first value

    // Process blocks until we have consumed ALL encoded values
    // IMPORTANT: We must process all blocks to consume all bytes, not just until we have expectedValues!
    while (valuesProcessed < totalValueCount) {
      // Read block header (min delta + bit widths)
      readBlockHeader(valuesProcessed);

      // Process each mini-block in this block
      // IMPORTANT: We must process ALL mini-blocks to consume all bytes from the buffer
      for (int mb = 0; mb < numMiniBlocks; mb++) {
        int bitWidth = miniBlockBitWidths[mb] & 0xFF;

        // Calculate how many values are encoded in this mini-block
        int valuesRemainingOverall = totalValueCount - valuesProcessed;
        if (valuesRemainingOverall <= 0) {
          // No more values to process, exit the miniblock loop
          break;
        }
        int valuesInThisMiniBlock = Math.min(valuesPerMiniBlock, valuesRemainingOverall);

        // Calculate how many values we actually want to use
        int valuesToUse = Math.min(valuesInThisMiniBlock, expectedValues - index);

        if (bitWidth == 0) {
          // All deltas are min_delta - no bytes to consume from buffer
          for (int i = 0; i < valuesToUse; i++) {
            lastValue += minDelta;
            result[index++] = (int) lastValue;
          }
          valuesProcessed += valuesInThisMiniBlock;
        } else {
          // Mini-blocks are atomic: we MUST read the FULL miniblock (valuesPerMiniBlock values)
          // even if we only need valuesInThisMiniBlock values, matching parquet-java behavior
          // This is because the file format stores full miniblocks with padding
          long[] deltas = readBitPackedInt64Values(bitWidth, valuesPerMiniBlock);
          // But only use the values we actually need
          for (int i = 0; i < valuesToUse && i < valuesInThisMiniBlock; i++) {
            lastValue += minDelta + deltas[i];
            result[index++] = (int) lastValue;
          }
          valuesProcessed += valuesInThisMiniBlock;
        }
      }
    }

    // DO NOT advance buffer to blockEndOffset
    // The decoder should leave the buffer exactly where it stopped reading
    // This is important for DELTA_BYTE_ARRAY which has TWO consecutive DELTA_BINARY_PACKED streams

    return result;
  }

  /**
   * Decode all values as 64-bit integers.
   * <p>
   * This method decodes up to {@code expectedValues} from the DELTA_BINARY_PACKED stream.
   * It processes all encoded blocks and mini-blocks to ensure all bytes are consumed from
   * the buffer, even if fewer values are requested than are encoded.
   * <p>
   * The decoder maintains internal state and processes:
   * <ul>
   *   <li>The first value (stored directly in the header)</li>
   *   <li>All subsequent blocks containing mini-blocks of bit-packed delta values</li>
   * </ul>
   *
   * @param expectedValues the number of values to decode and return
   * @return an array of decoded 64-bit integer values
   */
  public long[] decodeInt64(int expectedValues) {
    // Guard against zero expectedValues to avoid division by zero in header initialization
    if (expectedValues == 0) {
      return new long[0];
    }

    long[] result = new long[expectedValues];
    int index = 0;

    // Consume first value
    if (index < expectedValues && index < totalValueCount) {
      result[index++] = firstValue;
      lastValue = firstValue;
      firstValueConsumed = true;
    }

    // Track how many values have been processed from the encoded blocks
    int valuesProcessed = 1; // We've processed the first value

    // Process blocks until we have consumed ALL encoded values
    // IMPORTANT: We must process all blocks to consume all bytes, not just until we have expectedValues!
    while (valuesProcessed < totalValueCount) {
      // Read block header (min delta + bit widths)
      readBlockHeader(valuesProcessed);

      // Process each mini-block in this block
      // IMPORTANT: We must process ALL mini-blocks to consume all bytes from the buffer
      for (int mb = 0; mb < numMiniBlocks; mb++) {
        int bitWidth = miniBlockBitWidths[mb] & 0xFF;

        // Calculate how many values are encoded in this mini-block
        int valuesRemainingOverall = totalValueCount - valuesProcessed;
        if (valuesRemainingOverall <= 0) {
          // No more values to process, exit the miniblock loop
          break;
        }
        int valuesInThisMiniBlock = Math.min(valuesPerMiniBlock, valuesRemainingOverall);

        // Calculate how many values we actually want to use
        int valuesToUse = Math.min(valuesInThisMiniBlock, expectedValues - index);

        if (bitWidth == 0) {
          // All deltas are min_delta - no bytes to consume from buffer
          for (int i = 0; i < valuesToUse; i++) {
            lastValue += minDelta;
            result[index++] = lastValue;
          }
          valuesProcessed += valuesInThisMiniBlock;
        } else {
          // Mini-blocks are atomic: we MUST read the FULL miniblock (valuesPerMiniBlock values)
          // even if we only need valuesInThisMiniBlock values, matching parquet-java behavior
          // This is because the file format stores full miniblocks with padding
          long[] deltas = readBitPackedInt64Values(bitWidth, valuesPerMiniBlock);
          // But only use the values we actually need
          for (int i = 0; i < valuesToUse && i < valuesInThisMiniBlock; i++) {
            lastValue += minDelta + deltas[i];
            result[index++] = lastValue;
          }
          valuesProcessed += valuesInThisMiniBlock;
        }
      }
    }

    // DO NOT advance buffer to blockEndOffset
    // The decoder should leave the buffer exactly where it stopped reading
    // This is important for DELTA_BYTE_ARRAY which has TWO consecutive DELTA_BINARY_PACKED streams

    return result;
  }

  /**
   * Initialize header by reading block_size, num_mini_blocks, total_value_count, and first_value.
   * <p>
   * This method reads the DELTA_BINARY_PACKED header from the buffer, which contains:
   * <ul>
   *   <li>block_size: number of values in each block (multiple of 128)</li>
   *   <li>num_mini_blocks: number of mini-blocks per block</li>
   *   <li>total_value_count: total values in the stream</li>
   *   <li>first_value: the first value (zigzag encoded)</li>
   * </ul>
   * It also calculates the values per mini-block and allocates storage for mini-block bit widths.
   */
  private void initializeHeader() {
    int posBefore = buffer.position();
    blockSize = readUnsignedVarInt();
    numMiniBlocks = readUnsignedVarInt();
    totalValueCount = readUnsignedVarInt();
    firstValue = readZigzagVarLong();

    // Calculate values per mini-block
    valuesPerMiniBlock = blockSize / numMiniBlocks;

    // Allocate array for mini-block bit widths
    miniBlockBitWidths = new byte[numMiniBlocks];
  }

  /**
   * Read block header: min_delta and bit widths for all mini-blocks.
   * <p>
   * This method reads the per-block header containing:
   * <ul>
   *   <li>min_delta: the minimum delta value in this block (zigzag encoded)</li>
   *   <li>bit widths: one byte per mini-block indicating bits needed for deltas</li>
   * </ul>
   * It also computes the block end offset to properly handle trailing mini-blocks that
   * may contain no actual values but still occupy space in the file format.
   *
   * @param valuesProcessed the number of values already processed from the stream
   */
  private void readBlockHeader(int valuesProcessed) {
    // Read min delta
    minDelta = readZigzagVarLong();

    // Read bit widths for all mini-blocks
    for (int i = 0; i < numMiniBlocks; i++) {
      miniBlockBitWidths[i] = buffer.get();
    }

    // Compute the end offset of the current block
    // This accounts for all miniblocks, even trailing ones with no values
    int offset = buffer.position();
    int remaining = totalValueCount - valuesProcessed;

    for (int i = 0; i < numMiniBlocks; i++) {
      int bitWidth = miniBlockBitWidths[i] & 0xFF;
      if (remaining == 0) {
        // Trailing miniblocks with no values - set bit width to 0
        miniBlockBitWidths[i] = 0;
      }
      // Calculate how many values are actually in this miniblock
      int valuesInMiniBlock = Math.min(valuesPerMiniBlock, remaining);
      remaining = Math.max(0, remaining - valuesPerMiniBlock);

      // Mini-blocks are atomic: file format stores FULL miniblocks (valuesPerMiniBlock values)
      // Bit-packing reads in chunks of 8, so pad to 8-value boundary
      if (bitWidth > 0 && valuesInMiniBlock > 0) {
        int valuesWithPadding = ((valuesPerMiniBlock + 7) / 8) * 8;
        offset += (bitWidth * valuesWithPadding) / 8;
      }
    }
    blockEndOffset = offset;
  }

  /**
   * Read bit-packed values from the buffer as signed int64.
   * <p>
   * This method unpacks bit-packed delta values from the buffer. Bit-packed data is read
   * in chunks of 8 values, padding to 8-value boundaries as required by the Parquet format.
   * Each value uses exactly {@code bitWidth} bits in the packed representation.
   *
   * @param bitWidth the number of bits used to encode each value
   * @param numValues the number of values to unpack
   * @return an array of unpacked long values
   */
  private long[] readBitPackedInt64Values(int bitWidth, int numValues) {
    long[] result = new long[numValues];

    // Bit-packed data is read in chunks of 8 values (BytePacker.unpack8Values)
    // Pad to 8-value boundaries like parquet-java does
    int valuesWithPadding = ((numValues + 7) / 8) * 8;
    int totalBits = valuesWithPadding * bitWidth;
    int numBytes = totalBits / 8;

    // Read bytes
    byte[] bytes = new byte[numBytes];
    buffer.get(bytes);

    // Unpack values
    int bitOffset = 0;
    for (int i = 0; i < numValues; i++) {
      long value = 0;
      int bitsRemaining = bitWidth;

      while (bitsRemaining > 0) {
        int byteIndex = bitOffset / 8;
        int bitIndex = bitOffset % 8;

        if (byteIndex >= bytes.length) {
          break;
        }

        int currentByte = bytes[byteIndex] & 0xFF;
        int bitsAvailable = 8 - bitIndex;
        int bitsToRead = Math.min(bitsRemaining, bitsAvailable);

        int mask = (1 << bitsToRead) - 1;
        long bits = (currentByte >> bitIndex) & mask;

        value |= bits << (bitWidth - bitsRemaining);

        bitOffset += bitsToRead;
        bitsRemaining -= bitsToRead;
      }

      result[i] = value;
    }

    return result;
  }

  /**
   * Read an unsigned variable-length integer using varint encoding.
   * <p>
   * Varint encoding uses the high bit of each byte as a continuation flag.
   * If the high bit is set, more bytes follow. The remaining 7 bits of each
   * byte contain the actual value, with least significant bits first.
   *
   * @return the decoded unsigned integer value
   */
  private int readUnsignedVarInt() {
    int result = 0;
    int shift = 0;
    while (true) {
      byte b = buffer.get();
      result |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return result;
  }

  /**
   * Read a zigzag-encoded variable-length long.
   * <p>
   * Zigzag encoding maps signed integers to unsigned integers so that numbers with
   * small absolute values have small encoded values. The encoding is:
   * <ul>
   *   <li>0 encodes as 0</li>
   *   <li>-1 encodes as 1</li>
   *   <li>1 encodes as 2</li>
   *   <li>-2 encodes as 3</li>
   *   <li>etc.</li>
   * </ul>
   * The formula to decode is: (n >>> 1) ^ -(n & 1)
   *
   * @return the decoded signed long value
   */
  private long readZigzagVarLong() {
    long encoded = 0;
    int shift = 0;
    while (true) {
      byte b = buffer.get();
      encoded |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return (encoded >>> 1) ^ -(encoded & 1);
  }
}
