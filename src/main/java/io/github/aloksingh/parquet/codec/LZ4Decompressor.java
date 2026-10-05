package io.github.aloksingh.parquet.codec;

import io.github.aloksingh.parquet.PageReadOptions;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

/**
 * Decodes only the deprecated Parquet LZ4 codec's Apache Hadoop block stream.
 * Outer uncompressed lengths and inner compressed chunk lengths are big-endian.
 * No raw/private framing detection or fallback is performed. Prefer LZ4_RAW for
 * new files; its separate decoder is {@link Lz4RawDecompressor}.
 */
public class LZ4Decompressor extends BoundedDecompressor {
  private static final LZ4SafeDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor();

  /** Constructs a legacy Hadoop-framed decoder with safe default allocation limits. */
  public LZ4Decompressor() {
  }

  /**
   * Constructs a legacy Hadoop-framed decoder with explicit allocation bounds.
   * @param options input and output byte limits
   */
  public LZ4Decompressor(PageReadOptions options) {
    super(options);
  }

  @Override
  protected ByteBuffer decompressBounded(ByteBuffer compressed, int uncompressedSize) throws IOException {
    ByteBuffer input = compressed.slice().order(ByteOrder.BIG_ENDIAN);
    ByteBuffer output = ByteBuffer.allocate(uncompressedSize);
    int written = 0;
    while (input.hasRemaining()) {
      int blockSize = readLength(input, "uncompressed block");
      if (blockSize == 0) {
        // Hadoop emits a zero-sized outer block on an empty finish; it may terminate a stream.
        if (written != uncompressedSize || input.hasRemaining()) {
          throw new IOException("Invalid Hadoop LZ4 zero-length terminator");
        }
        break;
      }
      if (blockSize < 0 || blockSize > uncompressedSize - written) {
        throw new IOException("Hadoop LZ4 uncompressed block size exceeds declared output: " + blockSize);
      }
      int remainingInBlock = blockSize;
      while (remainingInBlock > 0) {
        int compressedLength = readLength(input, "compressed chunk");
        if (compressedLength <= 0 || compressedLength > input.remaining()) {
          throw new IOException("Invalid Hadoop LZ4 compressed chunk length: " + compressedLength);
        }
        int count = DECOMPRESSOR.decompress(input, input.position(), compressedLength,
            output, written, remainingInBlock);
        if (count <= 0) {
          throw new IOException("Hadoop LZ4 chunk produced no data");
        }
        input.position(input.position() + compressedLength);
        written += count;
        remainingInBlock -= count;
      }
    }
    if (written != uncompressedSize) {
      throw new IOException("Hadoop LZ4 output size mismatch: expected " + uncompressedSize + ", got " + written);
    }
    output.position(written);
    return output.flip();
  }

  private static int readLength(ByteBuffer input, String field) throws EOFException {
    if (input.remaining() < Integer.BYTES) {
      throw new EOFException("Truncated Hadoop LZ4 " + field + " length");
    }
    return input.getInt();
  }
}
