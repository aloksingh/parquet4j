package org.wazokazi.parquet.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import org.wazokazi.parquet.Compressor;

/**
 * GZIP compression implementation.
 * <p>
 * This compressor uses the standard Java {@link GZIPOutputStream} to compress data
 * according to the GZIP file format specification (RFC 1952).
 * </p>
 */
public class GzipCompressor implements Compressor {

  /**
   * Constructs a new GZIP compressor.
   */
  public GzipCompressor() {
  }
  /**
   * Compresses the input byte array using GZIP compression.
   *
   * @param uncompressed the uncompressed byte array to compress
   * @return the compressed byte array in GZIP format
   * @throws IOException if an I/O error occurs during compression
   */
  @Override
  public byte[] compress(byte[] uncompressed) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipStream = new GZIPOutputStream(out)) {
      gzipStream.write(uncompressed);
    }
    return out.toByteArray();
  }
}
