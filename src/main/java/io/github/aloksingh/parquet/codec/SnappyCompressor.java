package io.github.aloksingh.parquet.codec;

import java.io.IOException;
import io.github.aloksingh.parquet.Compressor;
import org.xerial.snappy.Snappy;

/**
 * Snappy compressor implementation.
 * <p>
 * This compressor uses the Snappy compression algorithm via the xerial snappy-java library.
 * Snappy is designed for high-speed compression and decompression with reasonable compression ratios.
 * </p>
 */
public class SnappyCompressor implements Compressor {

  /**
   * Constructs a new Snappy compressor.
   */
  public SnappyCompressor() {
  }
  @Override
  public byte[] compress(byte[] uncompressed) throws IOException {
    return Snappy.compress(uncompressed);
  }
}
