package org.wazokazi.parquet.model;

/**
 * Encoding types supported by Parquet format.
 * <p>
 * Each encoding type defines how column data is encoded in Parquet files.
 * The enum values correspond to the encoding types defined in the Parquet specification.
 * </p>
 */
public enum Encoding {
  /** Plain encoding - data is stored in its raw form */
  PLAIN(0),

  /** Plain dictionary encoding - deprecated, use RLE_DICTIONARY instead */
  PLAIN_DICTIONARY(2),

  /** Run Length Encoding - encodes repeated values efficiently */
  RLE(3),

  /** Bit-packed encoding - packs values into minimal number of bits */
  BIT_PACKED(4),

  /** Delta encoding for binary packed integers - stores differences between consecutive values */
  DELTA_BINARY_PACKED(5),

  /** Delta encoding for byte arrays with length - encodes lengths and values separately */
  DELTA_LENGTH_BYTE_ARRAY(6),

  /** Delta encoding for byte arrays - encodes prefixes and suffixes separately */
  DELTA_BYTE_ARRAY(7),

  /** RLE dictionary encoding - combines dictionary encoding with RLE */
  RLE_DICTIONARY(8),

  /** Byte stream split encoding - splits data into separate streams by byte position */
  BYTE_STREAM_SPLIT(9);

  private final int value;

  /**
   * Constructs an Encoding with the specified integer value.
   *
   * @param value the integer value corresponding to this encoding type in the Parquet specification
   */
  Encoding(int value) {
    this.value = value;
  }

  /**
   * Returns the integer value of this encoding type.
   *
   * @return the integer value as defined in the Parquet specification
   */
  public int getValue() {
    return value;
  }

  /**
   * Returns the Encoding enum constant corresponding to the given integer value.
   *
   * @param value the integer value of the encoding type
   * @return the corresponding Encoding enum constant
   * @throws IllegalArgumentException if the value does not correspond to any known encoding type
   */
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
