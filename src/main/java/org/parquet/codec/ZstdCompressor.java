package org.parquet.codec;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import org.parquet.Compressor;

/**
 * ZSTD (Zstandard) compressor implementation.
 * <p>
 * This compressor uses the Zstandard compression algorithm via the zstd-jni library.
 * ZSTD provides high compression ratios with fast compression and decompression speeds.
 * </p>
 */
public class ZstdCompressor implements Compressor {

  /**
   * Constructs a new ZSTD compressor.
   */
  public ZstdCompressor() {
  }
  @Override
  public byte[] compress(byte[] uncompressed) throws IOException {
    return Zstd.compress(uncompressed);
  }
}
