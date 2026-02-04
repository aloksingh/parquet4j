package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.util.filter.ColumnFilter;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * A filtering iterator for Parquet files that applies column filters during iteration.
 *
 * <p>This iterator extends {@link ParquetRowIterator} and filters rows based on one or more
 * {@link ColumnFilter} predicates. Only rows that match ALL specified filters are returned.
 * The filtering is applied lazily during iteration for memory efficiency.
 *
 * <p>Filters are evaluated against logical columns (user-facing columns) and their values.
 * Multiple filters can be combined using {@link io.github.aloksingh.parquet.util.filter.ColumnFilterSet}
 * for more complex filter logic (AND/OR conditions).
 *
 * <p>Usage example:
 * <pre>{@code
 * // Create a filter for rows where 'age' > 18
 * ColumnFilter ageFilter = new ColumnFilters().createFilter(FilterOperator.gt, 18);
 * ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All,
 *     new ColumnNameFilter("age", ageFilter));
 *
 * try (FilteringParquetRowIterator iterator =
 *         new FilteringParquetRowIterator(fileReader, filterSet)) {
 *   while (iterator.hasNext()) {
 *     RowColumnGroup row = iterator.next();
 *     // Process filtered row...
 *   }
 * }
 * }</pre>
 *
 * @see ParquetRowIterator
 * @see ColumnFilter
 * @see io.github.aloksingh.parquet.util.filter.ColumnFilterSet
 */
public class FilteringParquetRowIterator extends ParquetRowIterator {
  private final ColumnFilter[] filters;
  private RowColumnGroup nextMatchingRow;
  private boolean hasSearchedForNext;

  /**
   * Create a filtering iterator with a single column filter.
   *
   * @param fileReader      The file reader to iterate over
   * @param filter          The column filter to apply
   * @param closeFileReader Whether to close the file reader when done
   */
  public FilteringParquetRowIterator(SerializedFileReader fileReader,
                                     ColumnFilter filter,
                                     boolean closeFileReader) {
    super(fileReader, closeFileReader);
    this.filters = new ColumnFilter[]{filter};
    this.nextMatchingRow = null;
    this.hasSearchedForNext = false;
  }

  /**
   * Create a filtering iterator with multiple column filters.
   * A row must match ALL filters to be included in the results.
   *
   * @param fileReader      The file reader to iterate over
   * @param filters         The column filters to apply (AND semantics)
   * @param closeFileReader Whether to close the file reader when done
   */
  public FilteringParquetRowIterator(SerializedFileReader fileReader,
                                     ColumnFilter[] filters,
                                     boolean closeFileReader) {
    super(fileReader, closeFileReader);
    this.filters = filters != null ? filters : new ColumnFilter[0];
    this.nextMatchingRow = null;
    this.hasSearchedForNext = false;
  }

  /**
   * Create a filtering iterator with a single column filter.
   * The file reader will be closed automatically when {@link #close()} is called.
   *
   * @param fileReader The file reader to iterate over
   * @param filter     The column filter to apply
   */
  public FilteringParquetRowIterator(SerializedFileReader fileReader, ColumnFilter filter) {
    this(fileReader, filter, true);
  }

  /**
   * Create a filtering iterator with multiple column filters.
   * The file reader will be closed automatically when {@link #close()} is called.
   * A row must match ALL filters to be included in the results.
   *
   * @param fileReader The file reader to iterate over
   * @param filters    The column filters to apply (AND semantics)
   */
  public FilteringParquetRowIterator(SerializedFileReader fileReader, ColumnFilter[] filters) {
    this(fileReader, filters, true);
  }

  /**
   * Check if a row matches all filters.
   *
   * @param row The row to check
   * @return true if the row matches all filters, false otherwise
   */
  private boolean matchesFilters(RowColumnGroup row) {
    // If no filters, all rows match
    if (filters.length == 0) {
      return true;
    }

    // Check each filter
    for (ColumnFilter filter : filters) {
      boolean anyColumnMatched = false;

      // Check all columns against this filter
      for (int i = 0; i < row.getColumnCount(); i++) {
        LogicalColumnDescriptor columnDescriptor = row.getSchema().getLogicalColumn(i);
        Object columnValue = row.getColumnValue(i);

        boolean matched = filter.apply(columnDescriptor, columnValue);
        // Debug output
        // System.out.println("Filter check: col=" + columnDescriptor.getName() + ", value=" + columnValue + ", matched=" + matched);

        if (matched) {
          anyColumnMatched = true;
          break;
        }
      }

      // If this filter didn't match any column, the row doesn't match
      if (!anyColumnMatched) {
        return false;
      }
    }

    // All filters matched
    return true;
  }

  /**
   * Find the next row that matches all filters.
   * This method advances the underlying iterator until a matching row is found.
   */
  private void findNextMatchingRow() {
    if (hasSearchedForNext) {
      return;
    }

    nextMatchingRow = null;
    hasSearchedForNext = true;

    // Iterate through rows until we find one that matches
    try {
      while (super.hasNext()) {
        try {
          RowColumnGroup row = super.next();
          if (matchesFilters(row)) {
            nextMatchingRow = row;
            break;
          }
        } catch (NoSuchElementException e) {
          // Parent iterator exhausted unexpectedly, stop searching
          break;
        }
      }
    } catch (Exception e) {
      // Unexpected error during iteration
      nextMatchingRow = null;
    }
  }

  /**
   * Check if there are more matching rows to iterate.
   *
   * @return true if there are more rows that match the filters
   */
  @Override
  public boolean hasNext() {
    findNextMatchingRow();
    return nextMatchingRow != null;
  }

  /**
   * Get the next row that matches all filters.
   *
   * @return A RowColumnGroup containing the next matching row
   * @throws NoSuchElementException If there are no more matching rows
   */
  @Override
  public RowColumnGroup next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more matching rows");
    }

    RowColumnGroup result = nextMatchingRow;
    nextMatchingRow = null;
    hasSearchedForNext = false;
    return result;
  }

  /**
   * Get the number of rows that match the filters.
   * Note: This method will iterate through all remaining rows to count them,
   * which will exhaust the iterator.
   *
   * @return The number of matching rows
   */
  public long getMatchingRowCount() {
    long count = 0;
    while (hasNext()) {
      next();
      count++;
    }
    return count;
  }
}
