package io.github.aloksingh.parquet.codec;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import io.github.aloksingh.parquet.Decompressor;
import io.github.aloksingh.parquet.PageReadOptions;

/** GZIP decompression with an exact, bounded output buffer, including concatenated members. */
public class GzipDecompressor extends BoundedDecompressor {
  /** Constructs a GZIP decompressor. */
  public GzipDecompressor() {
  }

  /** Constructs a decompressor with explicit page byte limits.
   * @param options the input and output allocation limits
   */
  public GzipDecompressor(io.github.aloksingh.parquet.PageReadOptions options) {
    super(options);
  }

  @Override
  protected ByteBuffer decompressBounded(ByteBuffer compressed, int uncompressedSize) throws IOException {
    byte[] compressedBytes = new byte[compressed.remaining()];
    compressed.get(compressedBytes);
    byte[] output = new byte[uncompressedSize];
    try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressedBytes))) {
      int offset = 0;
      while (offset < output.length) {
        int count = gzip.read(output, offset, output.length - offset);
        if (count < 0) {
          throw new EOFException("GZIP output is shorter than declared size " + uncompressedSize);
        }
        if (count == 0) {
          throw new IOException("GZIP decompression made no progress");
        }
        offset += count;
      }
      // One byte is sufficient to detect expansion beyond the declared allocation;
      // reaching EOF also verifies GZIP trailers (including concatenated members).
      if (gzip.read() != -1) {
        throw new IOException("GZIP output exceeds declared size " + uncompressedSize);
      }
    }
    return ByteBuffer.wrap(output);
  }
}
