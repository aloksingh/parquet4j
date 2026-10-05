package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ParquetException;
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
 * The filtering is applied lazily during iteration for memory efficiency. Construction binds
 * metadata only; the first hasNext()/next() performs lookahead. Predicate failures surface as
 * {@link io.github.aloksingh.parquet.model.ParquetException} naming the predicate expression
 * and row position (original failure kept as cause); read failures propagate as-is. Failures
 * are cached and rethrown on subsequent iteration attempts rather than becoming EOF, and only
 * a false delegate.hasNext() counts as exhaustion. The filter also drives the delegate's
 * conservative row-group pruning.
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
  private Throwable iterationFailure;
  private long rowsScanned;

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
    if (delegate == null) throw new IllegalArgumentException("Delegate must not be null");
    this.delegate = delegate;
    this.filter = filter;
    // Binding consults metadata only; do not advance or evaluate any row in this constructor.
    if (filter != null) {
      filter.requiredColumns(delegate.getSchema());
      // Share the delegate's row-group loading path so pruning applies here too.
      delegate.attachPruningFilter(filter);
    }
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
    if (iterationFailure instanceof RuntimeException failure) throw failure;
    if (iterationFailure instanceof Error failure) throw failure;
    if (hasSearchedForNext) return;

    nextMatchingRow = null;
    try {
      while (delegate.hasNext()) {
        RowColumnGroup row = delegate.next();
        boolean matched;
        try {
          matched = matchesFilters(row);
        } catch (RuntimeException predicateFailure) {
          // Predicate failures carry the expression and row position; the original stays
          // the cause. Delegate read failures below propagate unwrapped and unmasked.
          throw new ParquetException("Failed to evaluate filter " + filter.expression()
              + " at row " + rowsScanned + " in " + delegate.getSourceDescription()
              + (predicateFailure.getMessage() == null ? ""
                  : ": " + predicateFailure.getMessage()), predicateFailure);
        }
        rowsScanned++;
        if (matched) {
          nextMatchingRow = row;
          break;
        }
      }
      hasSearchedForNext = true;
    } catch (RuntimeException | Error failure) {
      // Cache and rethrow, including predicate/delegate NoSuchElementException. Only a false
      // delegate.hasNext() is exhaustion. A failed lookahead must never resume at a later row.
      iterationFailure = failure;
      throw failure;
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
