package io.github.aloksingh.parquet.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Statistical information about a column chunk.
 * These statistics can be used for query optimization and data skipping.
 *
 * @param min           the minimum value in this column chunk (encoded as bytes), or null if not available
 * @param max           the maximum value in this column chunk (encoded as bytes), or null if not available
 * @param nullCount     the count of null values, or null if not tracked
 * @param distinctCount the count of distinct values, or null if not tracked
 * @param minOrder      the minimum bound's order/provenance; unknown means do not use it for pruning
 * @param maxOrder      the maximum bound's order/provenance; unknown means do not use it for pruning
 */
public record ColumnStatistics(byte[] min, byte[] max, Long nullCount, Long distinctCount,
                               BoundsOrder minOrder, BoundsOrder maxOrder) {

  /**
   * The known interpretation of an individual bound, not the ordering of rows.
   */
  public enum BoundsOrder {
    /**
     * No supported order was established.
     */
    UNKNOWN,
    /**
     * Modern min_value/max_value with an explicit TYPE_ORDER.
     */
    TYPE_DEFINED,
    /**
     * Deprecated min/max, whose comparison order is signed.
     */
    LEGACY_SIGNED
  }

  /**
   * Compatibility constructor for caller-supplied signed-order statistics.
   * File readers must instead supply the actual provenance of both bounds.
   *
   * @param min           the signed-order minimum bound
   * @param max           the signed-order maximum bound
   * @param nullCount     the null count, or null if unknown
   * @param distinctCount the distinct count, or null if unknown
   */
  public ColumnStatistics(byte[] min, byte[] max, Long nullCount, Long distinctCount) {
    this(min, max, nullCount, distinctCount, BoundsOrder.LEGACY_SIGNED, BoundsOrder.LEGACY_SIGNED);
  }

  /**
   * Checks if minimum value statistics are available.
   *
   * @return true if min value is present and non-empty
   */
  public boolean hasMin() {
    return min != null && min.length > 0;
  }

  /**
   * Checks if maximum value statistics are available.
   *
   * @return true if max value is present and non-empty
   */
  public boolean hasMax() {
    return max != null && max.length > 0;
  }

  /**
   * Checks if null count statistics are available.
   *
   * @return true if null count is tracked
   */
  public boolean hasNullCount() {
    return nullCount != null;
  }

  /**
   * Checks if distinct count statistics are available.
   *
   * @return true if distinct count is tracked
   */
  public boolean hasDistinctCount() {
    return distinctCount != null;
  }


  /**
   * Decode a statistics value based on its type.
   * <p>
   * Converts byte array representations of statistics values (min/max) to their
   * string representations based on the Parquet physical type. Falls back to
   * hex representation if decoding fails.
   *
   * @param value the byte array containing the encoded value
   * @param type  the Parquet physical type (INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, BOOLEAN)
   * @return string representation of the decoded value, or hex string if decoding fails
   */
  public static Object decodeStatValue(byte[] value, Type type) {
    if (value == null || value.length == 0) {
      return null;
    }

    try {
      switch (type) {
        case INT32:
          if (value.length >= 4) {
            return littleEndianBuffer(value).getInt();
          }
          break;
        case INT64:
          if (value.length >= 8) {
            return littleEndianBuffer(value).getLong();
          }
          break;
        case FLOAT:
          if (value.length >= 4) {
            return littleEndianBuffer(value).getFloat();
          }
          break;
        case DOUBLE:
          if (value.length >= 8) {
            return littleEndianBuffer(value).getDouble();
          }
          break;
        case BYTE_ARRAY:
          return new String(value, StandardCharsets.UTF_8);
        case BOOLEAN:
          if (value.length > 0) {
            return value[0] != 0;
          }
          break;
      }
    } catch (Exception e) {
      // If decoding fails, return hex representation
      return bytesToHex(value);
    }

    return bytesToHex(value);
  }

  /**
   * Convert byte array to hex string.
   * <p>
   * Converts each byte to its two-digit hexadecimal representation and
   * prefixes with "0x".
   *
   * @param bytes the byte array to convert
   * @return hex string representation prefixed with "0x", or empty string if input is null/empty
   */
  private static String bytesToHex(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return "0x" + sb.toString();
  }


  private static ByteBuffer littleEndianBuffer(byte[] value) {
    return ByteBuffer.wrap(value).order(java.nio.ByteOrder.LITTLE_ENDIAN);
  }

}
