package org.parquet.model;

/**
 * Physical types supported by Parquet.
 * These types are intended to be used in combination with encodings
 * to control the on-disk storage format.
 */
public enum Type {
  /** Boolean (true/false) values. */
  BOOLEAN(0),
  /** 32-bit signed integers. */
  INT32(1),
  /** 64-bit signed integers. */
  INT64(2),
  /** 96-bit integers (deprecated, only used by legacy implementations for timestamps). */
  INT96(3),
  /** 32-bit floating point numbers (IEEE 754 single precision). */
  FLOAT(4),
  /** 64-bit floating point numbers (IEEE 754 double precision). */
  DOUBLE(5),
  /** Variable-length byte arrays. */
  BYTE_ARRAY(6),
  /** Fixed-length byte arrays. */
  FIXED_LEN_BYTE_ARRAY(7);

  private final int value;

  Type(int value) {
    this.value = value;
  }

  /**
   * Returns the integer value representing this type in the Parquet format.
   *
   * @return the integer value of this type
   */
  public int getValue() {
    return value;
  }

  /**
   * Converts an integer value to the corresponding Type enum.
   *
   * @param value the integer value from the Parquet format
   * @return the Type enum corresponding to the value
   * @throws IllegalArgumentException if the value does not correspond to a valid Type
   */
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
