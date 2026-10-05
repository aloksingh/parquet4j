package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound gte predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnGreaterThanOrEqualFilter extends TypedColumnFilter {
  public ColumnGreaterThanOrEqualFilter(LogicalColumnDescriptor column, Comparable matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnGreaterThanOrEqualFilter(LogicalColumnDescriptor column, Comparable matchValue,
                            Optional<String> mapKey) {
    this(column, (Object) matchValue, mapKey);
  }

  public ColumnGreaterThanOrEqualFilter(LogicalColumnDescriptor column, Object matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnGreaterThanOrEqualFilter(LogicalColumnDescriptor column, Object matchValue,
                            Optional<String> mapKey) {
    super(column, FilterOperator.gte, matchValue, mapKey);
  }
}
