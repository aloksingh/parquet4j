package org.parquet.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import org.parquet.Decompressor;

/**
 * GZIP decompressor
 */
public class GzipDecompressor implements Decompressor {
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
