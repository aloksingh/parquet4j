package org.parquet.model;

/**
 * Encoding types supported by Parquet
 */
public enum Encoding {
  PLAIN(0),
  PLAIN_DICTIONARY(2),
  RLE(3),
  BIT_PACKED(4),
  DELTA_BINARY_PACKED(5),
  DELTA_LENGTH_BYTE_ARRAY(6),
  DELTA_BYTE_ARRAY(7),
  RLE_DICTIONARY(8),
  BYTE_STREAM_SPLIT(9);

  private final int value;

  Encoding(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static Encoding fromValue(int value) {
    return switch (value) {
      case 0 -> PLAIN;
      case 2 -> PLAIN_DICTIONARY;
      case 3 -> RLE;
      case 4 -> BIT_PACKED;
      case 5 -> DELTA_BINARY_PACKED;
      case 6 -> DELTA_LENGTH_BYTE_ARRAY;
      case 7 -> DELTA_BYTE_ARRAY;
      case 8 -> RLE_DICTIONARY;
      case 9 -> BYTE_STREAM_SPLIT;
      default -> throw new IllegalArgumentException("Unknown Encoding value: " + value);
    };
  }
}
