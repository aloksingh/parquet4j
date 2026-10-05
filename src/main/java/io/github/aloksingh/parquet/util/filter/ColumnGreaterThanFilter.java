package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound gt predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnGreaterThanFilter extends TypedColumnFilter {
  public ColumnGreaterThanFilter(LogicalColumnDescriptor column, Comparable matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnGreaterThanFilter(LogicalColumnDescriptor column, Comparable matchValue,
                            Optional<String> mapKey) {
    this(column, (Object) matchValue, mapKey);
  }

  public ColumnGreaterThanFilter(LogicalColumnDescriptor column, Object matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnGreaterThanFilter(LogicalColumnDescriptor column, Object matchValue,
                            Optional<String> mapKey) {
    super(column, FilterOperator.gt, matchValue, mapKey);
  }
}
