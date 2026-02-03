package org.parquet.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.parquet.Decompressor;
import org.parquet.model.ParquetException;
import org.xerial.snappy.Snappy;

/**
 * Snappy decompressor implementation.
 * <p>
 * This decompressor uses the Snappy compression algorithm via the xerial snappy-java library
 * to decompress data compressed with Snappy.
 * </p>
 */
public class SnappyDecompressor implements Decompressor {

  /**
   * Constructs a new Snappy decompressor.
   */
  public SnappyDecompressor() {
  }
  @Override
  public ByteBuffer decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
    byte[] compressedBytes = new byte[compressed.remaining()];
    compressed.get(compressedBytes);

    byte[] uncompressed = new byte[uncompressedSize];
    int actualSize = Snappy.uncompress(compressedBytes, 0, compressedBytes.length,
        uncompressed, 0);

    if (actualSize != uncompressedSize) {
      throw new ParquetException(String.format(
          "Decompressed size mismatch: expected %d, got %d",
          uncompressedSize, actualSize));
    }

    return ByteBuffer.wrap(uncompressed);
  }
}
