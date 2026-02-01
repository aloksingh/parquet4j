package org.parquet.model;

/**
 * Compression codecs supported by Parquet
 */
public enum CompressionCodec {
  UNCOMPRESSED(0),
  SNAPPY(1),
  GZIP(2),
  LZO(3),
  BROTLI(4),
  LZ4(5),
  ZSTD(6),
  LZ4_RAW(7);

  private final int value;

  CompressionCodec(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

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
