package org.parquet;

import java.io.IOException;
import org.parquet.codec.GzipCompressor;
import org.parquet.codec.Lz4Compressor;
import org.parquet.codec.SnappyCompressor;
import org.parquet.codec.ZstdCompressor;
import org.parquet.model.CompressionCodec;
import org.parquet.model.ParquetException;

/**
 * Handles compression of Parquet page data
 */
public interface Compressor {

  byte[] compress(byte[] uncompressed) throws IOException;

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

  /**
   * No-op compressor for uncompressed data
   */
  class UncompressedCompressor implements Compressor {
    @Override
    public byte[] compress(byte[] uncompressed) {
      return uncompressed;
    }
  }

}
