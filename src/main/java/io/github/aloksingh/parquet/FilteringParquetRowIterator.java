package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.util.filter.ColumnFilter;
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilter;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilterSet;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * A filtering iterator for Parquet files that applies column filters during iteration.
 *
 * <p>This iterator wraps a {@link ParquetRowIterator} and filters rows based on one or more
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
 * ParquetRowIterator baseIterator = new ParquetRowIterator(fileReader);
 * try (FilteringParquetRowIterator iterator =
 *         new FilteringParquetRowIterator(baseIterator, filterSet)) {
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
public class FilteringParquetRowIterator implements RowColumnGroupIterator, AutoCloseable {
  private final ParquetRowIterator delegate;
  private final RowColumnGroupFilter filter;
  private RowColumnGroup nextMatchingRow;
  private boolean hasSearchedForNext;

  /**
   * Create a filtering iterator with a single column filter.
   *
   * @param delegate The base iterator to filter
   * @param filter   The column filter to apply
   */
  public FilteringParquetRowIterator(ParquetRowIterator delegate, ColumnFilter filter) {
    this(delegate, new RowColumnGroupFilterSet(FilterJoinType.All, filter));
  }

  public FilteringParquetRowIterator(ParquetRowIterator delegate, RowColumnGroupFilter filter) {
    this.delegate = delegate;
    this.filter = filter;
    this.nextMatchingRow = null;
    this.hasSearchedForNext = false;
  }

  /**
   * Check if a row matches all filters.
   *
   * @param row The row to check
   * @return true if the row matches all filters, false otherwise
   */
  private boolean matchesFilters(RowColumnGroup row) {
    // If no filters, all rows match
    return filter == null || filter.apply(row);
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
      while (delegate.hasNext()) {
        try {
          RowColumnGroup row = delegate.next();
          if (matchesFilters(row)) {
            nextMatchingRow = row;
            break;
          }
        } catch (NoSuchElementException e) {
          // Delegate iterator exhausted unexpectedly, stop searching
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

  /**
   * Close the underlying iterator.
   *
   * @throws IOException If closing the iterator fails
   */
  @Override
  public void close() throws IOException {
    delegate.close();
  }

  /**
   * Get the total number of rows in the underlying data (before filtering).
   *
   * @return The total row count from the file metadata
   */
  public long getTotalRowCount() {
    return delegate.getTotalRowCount();
  }

  /**
   * Get the schema for the rows being iterated.
   *
   * @return The schema descriptor containing all logical column definitions
   */
  public io.github.aloksingh.parquet.model.SchemaDescriptor getSchema() {
    return delegate.getSchema();
  }
}
