package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound neq predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnNotEqualFilter extends TypedColumnFilter {
  public ColumnNotEqualFilter(LogicalColumnDescriptor column, Object matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnNotEqualFilter(LogicalColumnDescriptor column, Object matchValue,
                            Optional<String> mapKey) {
    super(column, FilterOperator.neq, matchValue, mapKey);
  }
}
