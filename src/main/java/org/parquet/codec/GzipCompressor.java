package org.parquet.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import org.parquet.Compressor;

/**
 * GZIP compressor
 */
public class GzipCompressor implements Compressor {
  @Override
  public byte[] compress(byte[] uncompressed) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipStream = new GZIPOutputStream(out)) {
      gzipStream.write(uncompressed);
    }
    return out.toByteArray();
  }
}
