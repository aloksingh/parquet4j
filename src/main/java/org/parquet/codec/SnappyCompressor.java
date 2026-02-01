package org.parquet.codec;

import java.io.IOException;
import org.parquet.Compressor;
import org.xerial.snappy.Snappy;

/**
 * Snappy compressor
 */
public class SnappyCompressor implements Compressor {
  @Override
  public byte[] compress(byte[] uncompressed) throws IOException {
    return Snappy.compress(uncompressed);
  }
}
