package org.wazokazi.parquet.model;

/**
 * Represents the logical type of a column in a Parquet schema.
 * <p>
 * Logical types define how columns are structured and interpreted,
 * ranging from simple primitive values to complex nested structures.
 */
public enum LogicalType {
  /**
   * A single physical column containing primitive values (int, string, etc.).
   */
  PRIMITIVE,

  /**
   * A map structure represented by two physical columns (key + value).
   */
  MAP,

  /**
   * A list structure (future enhancement: one physical column with repetition).
   */
  LIST,

  /**
   * A struct structure (future enhancement: multiple physical columns).
   */
  STRUCT
}
