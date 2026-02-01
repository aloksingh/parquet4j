package org.parquet.codec;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import org.parquet.Compressor;

/**
 * ZSTD compressor
 */
public class ZstdCompressor implements Compressor {
  @Override
  public byte[] compress(byte[] uncompressed) throws IOException {
    return Zstd.compress(uncompressed);
  }
}
