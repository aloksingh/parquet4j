package io.github.aloksingh.parquet.codec;

import java.nio.ByteBuffer;
import io.github.aloksingh.parquet.Decompressor;

/**
 * No-op decompressor for uncompressed data.
 *
 * <p>This decompressor is used when the data is already uncompressed and no
 * decompression operation is needed. It simply returns the input buffer as-is.
 *
 * @see Decompressor
 */
public class UncompressedDecompressor extends BoundedDecompressor {

  /**
   * Constructs a new uncompressed decompressor (no-op).
   */
  public UncompressedDecompressor() {
  }

  /** Constructs a decompressor with explicit page byte limits.
   * @param options the input and output allocation limits
   */
  public UncompressedDecompressor(io.github.aloksingh.parquet.PageReadOptions options) {
    super(options);
  }

  /**
   * Returns the input buffer without performing any decompression.
   *
   * <p>Since the data is already uncompressed, this method performs a no-op
   * and returns the compressed buffer directly.
   *
   * @param compressed the input buffer containing uncompressed data
   * @param uncompressedSize the expected size of uncompressed data (ignored)
   * @return the input buffer unchanged
   */
  @Override
  protected ByteBuffer decompressBounded(ByteBuffer compressed, int uncompressedSize) {
    return compressed;
  }
}
