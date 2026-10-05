package io.github.aloksingh.parquet.codec;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import io.github.aloksingh.parquet.Decompressor;
import io.github.aloksingh.parquet.model.ParquetException;

/**
 * ZSTD (Zstandard) decompressor implementation.
 * <p>
 * This decompressor uses the Zstandard compression algorithm via the zstd-jni library
 * to decompress data compressed with ZSTD.
 * </p>
 */
public class ZstdDecompressor extends BoundedDecompressor {

  /**
   * Constructs a new ZSTD decompressor.
   */
  public ZstdDecompressor() {
  }

  /** Constructs a decompressor with explicit page byte limits.
   * @param options the input and output allocation limits
   */
  public ZstdDecompressor(io.github.aloksingh.parquet.PageReadOptions options) {
    super(options);
  }
  @Override
  protected ByteBuffer decompressBounded(ByteBuffer compressed, int uncompressedSize) throws IOException {
    byte[] compressedBytes = new byte[compressed.remaining()];
    compressed.get(compressedBytes);

    byte[] uncompressed = new byte[uncompressedSize];
    long actualSize;
    try {
      actualSize = Zstd.decompressByteArray(uncompressed, 0, uncompressedSize,
          compressedBytes, 0, compressedBytes.length);
    } catch (com.github.luben.zstd.ZstdException error) {
      throw new IOException("ZSTD decompression failed: " + error.getMessage(), error);
    }

    if (Zstd.isError(actualSize)) {
      throw new IOException("ZSTD decompression failed: " + Zstd.getErrorName(actualSize));
    }
    if (actualSize != uncompressedSize) {
      throw new IOException("ZSTD size mismatch: expected " + uncompressedSize + ", got " + actualSize);
    }

    return ByteBuffer.wrap(uncompressed);
  }
}
