package org.wazokazi.parquet.codec;

import org.wazokazi.parquet.Compressor;

/**
 * No-op compressor for uncompressed data.
 * <p>
 * This compressor is used when no compression is desired. It simply returns
 * the input data unchanged.
 * </p>
 */
public class UncompressedCompressor implements Compressor {

  /**
   * Constructs a new uncompressed compressor (no-op).
   */
  public UncompressedCompressor() {
  }
  @Override
  public byte[] compress(byte[] uncompressed) {
    return uncompressed;
  }
}
