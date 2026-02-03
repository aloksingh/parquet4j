package org.parquet.model;

/**
 * Exception class for Parquet-related errors.
 *
 * <p>This exception is thrown when errors occur during Parquet file operations,
 * including reading, writing, schema validation, and data conversion.</p>
 */
public class ParquetException extends RuntimeException {

  /**
   * Constructs a new ParquetException with the specified detail message.
   *
   * @param message the detail message explaining the reason for the exception
   */
  public ParquetException(String message) {
    super(message);
  }

  /**
   * Constructs a new ParquetException with the specified detail message and cause.
   *
   * @param message the detail message explaining the reason for the exception
   * @param cause the underlying cause of this exception
   */
  public ParquetException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new ParquetException with the specified cause.
   *
   * @param cause the underlying cause of this exception
   */
  public ParquetException(Throwable cause) {
    super(cause);
  }
}
