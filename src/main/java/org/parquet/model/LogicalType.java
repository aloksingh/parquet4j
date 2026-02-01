package org.parquet.model;

public enum LogicalType {
  PRIMITIVE,  // Single physical column (int, string, etc.)
  MAP,        // Two physical columns (key + value)
  LIST,       // Future: one physical column with repetition
  STRUCT      // Future: multiple physical columns
}
