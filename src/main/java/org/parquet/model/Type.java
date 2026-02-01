package org.parquet.model;

/**
 * Physical types supported by Parquet.
 * These types are intended to be used in combination with encodings
 * to control the on-disk storage format.
 */
public enum Type {
  BOOLEAN(0),
  INT32(1),
  INT64(2),
  INT96(3),  // deprecated, only used by legacy implementations
  FLOAT(4),
  DOUBLE(5),
  BYTE_ARRAY(6),
  FIXED_LEN_BYTE_ARRAY(7);

  private final int value;

  Type(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static Type fromValue(int value) {
    return switch (value) {
      case 0 -> BOOLEAN;
      case 1 -> INT32;
      case 2 -> INT64;
      case 3 -> INT96;
      case 4 -> FLOAT;
      case 5 -> DOUBLE;
      case 6 -> BYTE_ARRAY;
      case 7 -> FIXED_LEN_BYTE_ARRAY;
      default -> throw new IllegalArgumentException("Unknown Type value: " + value);
    };
  }
}
