package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound eq predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnEqualFilter extends TypedColumnFilter {
  public ColumnEqualFilter(LogicalColumnDescriptor column, Object matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnEqualFilter(LogicalColumnDescriptor column, Object matchValue,
                            Optional<String> mapKey) {
    super(column, FilterOperator.eq, matchValue, mapKey);
  }
}
