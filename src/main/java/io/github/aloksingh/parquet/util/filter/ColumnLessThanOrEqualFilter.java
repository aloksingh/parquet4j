package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound lte predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnLessThanOrEqualFilter extends TypedColumnFilter {
  public ColumnLessThanOrEqualFilter(LogicalColumnDescriptor column, Comparable matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnLessThanOrEqualFilter(LogicalColumnDescriptor column, Comparable matchValue,
                            Optional<String> mapKey) {
    this(column, (Object) matchValue, mapKey);
  }

  public ColumnLessThanOrEqualFilter(LogicalColumnDescriptor column, Object matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnLessThanOrEqualFilter(LogicalColumnDescriptor column, Object matchValue,
                            Optional<String> mapKey) {
    super(column, FilterOperator.lte, matchValue, mapKey);
  }
}
