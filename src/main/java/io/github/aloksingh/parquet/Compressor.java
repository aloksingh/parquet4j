package io.github.aloksingh.parquet;

import java.io.IOException;
import io.github.aloksingh.parquet.codec.GzipCompressor;
import io.github.aloksingh.parquet.codec.Lz4Compressor;
import io.github.aloksingh.parquet.codec.SnappyCompressor;
import io.github.aloksingh.parquet.codec.UncompressedCompressor;
import io.github.aloksingh.parquet.codec.ZstdCompressor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.ParquetException;

/**
 * Handles compression of Parquet page data.
 *
 * <p>This interface defines the contract for compressing data in Parquet files.
 * Implementations provide specific compression algorithms such as GZIP, Snappy,
 * LZ4, ZSTD, or no compression.
 *
 * @see CompressionCodec
 */
public interface Compressor {

  /**
   * Compresses the given uncompressed data.
   *
   * @param uncompressed the byte array containing uncompressed data
   * @return a byte array containing the compressed data
   * @throws IOException if an I/O error occurs during compression
   */
  byte[] compress(byte[] uncompressed) throws IOException;

  /**
   * Creates a compressor instance for the specified compression codec.
   *
   * @param codec the compression codec to use
   * @return a compressor instance that implements the specified codec
   * @throws ParquetException if the codec is not supported
   */
  static Compressor create(CompressionCodec codec) {
    return switch (codec) {
      case UNCOMPRESSED -> new UncompressedCompressor();
      case SNAPPY -> new SnappyCompressor();
      case GZIP -> new GzipCompressor();
      case LZ4 -> new Lz4Compressor();
      case ZSTD -> new ZstdCompressor();
      default -> throw new ParquetException("Unsupported compression codec: " + codec);
    };
  }

}
