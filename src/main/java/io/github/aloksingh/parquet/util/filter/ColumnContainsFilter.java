package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound contains predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnContainsFilter extends TypedColumnFilter {
  public ColumnContainsFilter(LogicalColumnDescriptor column, Object matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnContainsFilter(LogicalColumnDescriptor column, Object matchValue,
                            Optional<String> mapKey) {
    super(column, FilterOperator.contains, matchValue, mapKey);
  }
}
