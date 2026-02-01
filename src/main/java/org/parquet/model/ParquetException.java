package org.parquet.model;

/**
 * Exception class for Parquet-related errors
 */
public class ParquetException extends RuntimeException {
  public ParquetException(String message) {
    super(message);
  }

  public ParquetException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParquetException(Throwable cause) {
    super(cause);
  }
}
