package org.wazokazi.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.wazokazi.parquet.codec.GzipDecompressor;
import org.wazokazi.parquet.codec.LZ4Decompressor;
import org.wazokazi.parquet.codec.SnappyDecompressor;
import org.wazokazi.parquet.codec.UncompressedDecompressor;
import org.wazokazi.parquet.codec.ZstdDecompressor;
import org.wazokazi.parquet.model.CompressionCodec;
import org.wazokazi.parquet.model.ParquetException;

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
    return switch (codec) {
      case UNCOMPRESSED -> new UncompressedDecompressor();
      case SNAPPY -> new SnappyDecompressor();
      case GZIP -> new GzipDecompressor();
      case LZ4, LZ4_RAW -> new LZ4Decompressor();
      case ZSTD -> new ZstdDecompressor();
      default -> throw new ParquetException("Unsupported compression codec: " + codec);
    };
  }

}
