package org.parquet.model;

/**
 * Compression codecs supported by Parquet format.
 * <p>
 * Each codec provides different trade-offs between compression ratio, speed, and CPU usage.
 * The codec values correspond to the Parquet format specification.
 */
public enum CompressionCodec {
  /** No compression applied */
  UNCOMPRESSED(0),

  /** Snappy compression - fast compression with moderate ratio */
  SNAPPY(1),

  /** GZIP compression - high compression ratio, slower than Snappy */
  GZIP(2),

  /** LZO compression - fast compression, requires native libraries */
  LZO(3),

  /** Brotli compression - high compression ratio, good for HTTP compression */
  BROTLI(4),

  /** LZ4 compression - very fast compression and decompression */
  LZ4(5),

  /** Zstandard compression - tunable compression with good ratio and speed */
  ZSTD(6),

  /** LZ4 raw format - LZ4 without frame headers */
  LZ4_RAW(7);

  private final int value;

  /**
   * Constructs a CompressionCodec with the specified value.
   *
   * @param value the numeric value corresponding to the codec in the Parquet specification
   */
  CompressionCodec(int value) {
    this.value = value;
  }

  /**
   * Returns the numeric value of this codec as defined in the Parquet specification.
   *
   * @return the codec's numeric value
   */
  public int getValue() {
    return value;
  }

  /**
   * Returns the CompressionCodec enum constant corresponding to the specified value.
   *
   * @param value the numeric value from the Parquet specification
   * @return the corresponding CompressionCodec enum constant
   * @throws IllegalArgumentException if the value does not correspond to any known codec
   */
  public static CompressionCodec fromValue(int value) {
    return switch (value) {
      case 0 -> UNCOMPRESSED;
      case 1 -> SNAPPY;
      case 2 -> GZIP;
      case 3 -> LZO;
      case 4 -> BROTLI;
      case 5 -> LZ4;
      case 6 -> ZSTD;
      case 7 -> LZ4_RAW;
      default -> throw new IllegalArgumentException("Unknown CompressionCodec value: " + value);
    };
  }
}
