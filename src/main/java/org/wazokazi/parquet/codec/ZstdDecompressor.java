package org.wazokazi.parquet.codec;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.wazokazi.parquet.Decompressor;
import org.wazokazi.parquet.model.ParquetException;

/**
 * ZSTD (Zstandard) decompressor implementation.
 * <p>
 * This decompressor uses the Zstandard compression algorithm via the zstd-jni library
 * to decompress data compressed with ZSTD.
 * </p>
 */
public class ZstdDecompressor implements Decompressor {

  /**
   * Constructs a new ZSTD decompressor.
   */
  public ZstdDecompressor() {
  }
  @Override
  public ByteBuffer decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
    byte[] compressedBytes = new byte[compressed.remaining()];
    compressed.get(compressedBytes);

    byte[] uncompressed = new byte[uncompressedSize];
    long actualSize = Zstd.decompressByteArray(uncompressed, 0, uncompressedSize,
        compressedBytes, 0, compressedBytes.length);

    if (actualSize != uncompressedSize) {
      throw new ParquetException(String.format(
          "Decompressed size mismatch: expected %d, got %d",
          uncompressedSize, actualSize));
    }

    return ByteBuffer.wrap(uncompressed);
  }
}
