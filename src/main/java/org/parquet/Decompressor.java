package org.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.parquet.codec.GzipDecompressor;
import org.parquet.codec.LZ4Decompressor;
import org.parquet.codec.SnappyDecompressor;
import org.parquet.codec.ZstdDecompressor;
import org.parquet.model.CompressionCodec;
import org.parquet.model.ParquetException;

/**
 * Handles decompression of Parquet page data
 */
public interface Decompressor {

  ByteBuffer decompress(ByteBuffer compressed, int uncompressedSize) throws IOException;

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

  /**
   * No-op decompressor for uncompressed data
   */
  class UncompressedDecompressor implements Decompressor {
    @Override
    public ByteBuffer decompress(ByteBuffer compressed, int uncompressedSize) {
      return compressed;
    }
  }

}
