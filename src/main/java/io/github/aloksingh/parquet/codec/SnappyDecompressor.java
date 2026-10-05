package io.github.aloksingh.parquet.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import io.github.aloksingh.parquet.Decompressor;
import io.github.aloksingh.parquet.model.ParquetException;
import org.xerial.snappy.Snappy;

/**
 * Snappy decompressor implementation.
 * <p>
 * This decompressor uses the Snappy compression algorithm via the xerial snappy-java library
 * to decompress data compressed with Snappy.
 * </p>
 */
public class SnappyDecompressor extends BoundedDecompressor {

  /**
   * Constructs a new Snappy decompressor.
   */
  public SnappyDecompressor() {
  }

  /** Constructs a decompressor with explicit page byte limits.
   * @param options the input and output allocation limits
   */
  public SnappyDecompressor(io.github.aloksingh.parquet.PageReadOptions options) {
    super(options);
  }
  @Override
  protected ByteBuffer decompressBounded(ByteBuffer compressed, int uncompressedSize) throws IOException {
    byte[] compressedBytes = new byte[compressed.remaining()];
    compressed.get(compressedBytes);

    int encodedSize = Snappy.uncompressedLength(compressedBytes, 0, compressedBytes.length);
    if (encodedSize != uncompressedSize) {
      throw new IOException("Snappy size mismatch: expected " + uncompressedSize + ", got " + encodedSize);
    }
    byte[] uncompressed = new byte[uncompressedSize];
    int actualSize = Snappy.uncompress(compressedBytes, 0, compressedBytes.length,
        uncompressed, 0);

    if (actualSize != uncompressedSize) {
      throw new IOException("Snappy size mismatch: expected " + uncompressedSize + ", got " + actualSize);
    }

    return ByteBuffer.wrap(uncompressed);
  }
}
