package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound lt predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnLessThanFilter extends TypedColumnFilter {
  public ColumnLessThanFilter(LogicalColumnDescriptor column, Comparable matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnLessThanFilter(LogicalColumnDescriptor column, Comparable matchValue,
                            Optional<String> mapKey) {
    this(column, (Object) matchValue, mapKey);
  }

  public ColumnLessThanFilter(LogicalColumnDescriptor column, Object matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnLessThanFilter(LogicalColumnDescriptor column, Object matchValue,
                            Optional<String> mapKey) {
    super(column, FilterOperator.lt, matchValue, mapKey);
  }
}
