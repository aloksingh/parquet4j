package org.parquet.codec;

import org.parquet.Compressor;

/**
 * No-op compressor for uncompressed data
 */
public class UncompressedCompressor implements Compressor {
  @Override
  public byte[] compress(byte[] uncompressed) {
    return uncompressed;
  }
}
