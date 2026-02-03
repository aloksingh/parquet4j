package org.parquet.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import org.parquet.Decompressor;

/**
 * GZIP decompressor implementation.
 * <p>
 * This decompressor uses the standard Java {@link GZIPInputStream} to decompress data
 * according to the GZIP file format specification (RFC 1952).
 * </p>
 */
public class GzipDecompressor implements Decompressor {

  /**
   * Constructs a new GZIP decompressor.
   */
  public GzipDecompressor() {
  }
  @Override
  public ByteBuffer decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
    byte[] compressedBytes = new byte[compressed.remaining()];
    compressed.get(compressedBytes);

    try (GZIPInputStream gzipStream = new GZIPInputStream(
        new ByteArrayInputStream(compressedBytes))) {
      ByteArrayOutputStream out = new ByteArrayOutputStream(uncompressedSize);
      byte[] buffer = new byte[8192];
      int len;
      while ((len = gzipStream.read(buffer)) != -1) {
        out.write(buffer, 0, len);
      }
      return ByteBuffer.wrap(out.toByteArray());
    }
  }
}
