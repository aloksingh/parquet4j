package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public interface ColumnFilter {

  boolean apply(Object colValue);

  boolean isApplicable(LogicalColumnDescriptor columnDescriptor);

  /** Human-readable predicate text for error context; defaults to the class identity. */
  default String expression() {
    return toString();
  }

  /** The bound logical column, or null for opaque custom predicates. */
  default LogicalColumnDescriptor targetColumn() {
    return null;
  }

  /**
   * Returns true only when no value in the chunk can match this predicate's own bound constant.
   * Missing, malformed or unsafe statistics must return false. {@code numValues == -1} means
   * the value count is unknown. The default is conservative for existing custom predicates.
   */
  default boolean canDrop(ColumnStatistics statistics, long numValues) {
    return false;
  }

  /**
   * Compatibility alias for {@link #canDrop(ColumnStatistics, long)} with an unknown count.
   * The supplied value is deliberately ignored: pruning uses the predicate's bound constant.
   *
   * @deprecated use canDrop(statistics, numValues)
   */
  @Deprecated
  default boolean skip(ColumnStatistics statistics, Object colValue) {
    return canDrop(statistics, -1);
  }

}
