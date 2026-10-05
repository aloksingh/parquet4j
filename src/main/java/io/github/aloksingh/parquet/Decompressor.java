package io.github.aloksingh.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import io.github.aloksingh.parquet.codec.GzipDecompressor;
import io.github.aloksingh.parquet.codec.LZ4Decompressor;
import io.github.aloksingh.parquet.codec.Lz4RawDecompressor;
import io.github.aloksingh.parquet.codec.SnappyDecompressor;
import io.github.aloksingh.parquet.codec.UncompressedDecompressor;
import io.github.aloksingh.parquet.codec.ZstdDecompressor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.ParquetException;

/**
 * Handles decompression of Parquet page data.
 *
 * <p>This interface defines the contract for decompressing data in Parquet files.
 * Implementations provide specific decompression algorithms such as GZIP, Snappy,
 * LZ4, ZSTD, or no decompression for uncompressed data.
 *
 * @see CompressionCodec
 */
public interface Decompressor {

  /**
   * Decompresses the given compressed data.
   *
   * @param compressed the ByteBuffer containing compressed data
   * @param uncompressedSize the expected size of the uncompressed data in bytes
   * @return a ByteBuffer containing the decompressed data
   * @throws IOException if an I/O error occurs during decompression
   */
  ByteBuffer decompress(ByteBuffer compressed, int uncompressedSize) throws IOException;

  /**
   * Creates a decompressor instance for the specified compression codec.
   *
   * @param codec the compression codec to use for decompression
   * @return a decompressor instance that implements the specified codec
   * @throws ParquetException if the codec is not supported
   */
  static Decompressor create(CompressionCodec codec) {
    return create(codec, PageReadOptions.DEFAULT);
  }

  /**
   * Creates a codec with explicit input and output allocation bounds. The checksum
   * and header/count settings are handled by PageReader, not by the codec.
   * @param codec declared column chunk codec
   * @param options input/output byte limits
   * @return a decompressor enforcing the supplied byte limits
   * @throws ParquetException if the codec is unsupported
   */
  static Decompressor create(CompressionCodec codec, PageReadOptions options) {
    java.util.Objects.requireNonNull(options, "options");
    return switch (codec) {
      case UNCOMPRESSED -> new UncompressedDecompressor(options);
      case SNAPPY -> new SnappyDecompressor(options);
      case GZIP -> new GzipDecompressor(options);
      case LZ4 -> new LZ4Decompressor(options);
      case LZ4_RAW -> new Lz4RawDecompressor(options);
      case ZSTD -> new ZstdDecompressor(options);
      default -> throw new ParquetException("Unsupported compression codec: " + codec);
    };
  }

}
