package org.wazokazi.parquet;

import org.wazokazi.parquet.model.RowColumnGroup;

/**
 * Interface for writing data to Parquet files in a row-oriented manner.
 *
 * <p>This interface provides a simple API for writing Parquet data by adding rows
 * one at a time. Each row is represented as a {@link RowColumnGroup} containing
 * the column values for that row.
 *
 * <p>Implementations of this interface must be thread-safe for concurrent writes
 * or document their thread-safety guarantees. Callers should ensure proper resource
 * management by closing the writer when done, either explicitly or via try-with-resources.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (ParquetWriter writer = createWriter()) {
 *     RowColumnGroup row = createRow();
 *     writer.addRow(row);
 * }
 * }</pre>
 *
 * @see RowColumnGroup
 * @see AutoCloseable
 */
public interface ParquetWriter extends AutoCloseable {

  /**
   * Adds a single row of data to the Parquet file.
   *
   * <p>The row data is buffered and written to the underlying file according to
   * the implementation's buffering strategy. Rows are typically written in batches
   * to optimize I/O performance.
   *
   * @param row the row data to write, containing values for all columns; must not be null
   */
  void addRow(RowColumnGroup row);

}
