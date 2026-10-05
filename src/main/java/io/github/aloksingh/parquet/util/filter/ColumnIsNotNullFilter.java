package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound isNotNull predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnIsNotNullFilter extends TypedColumnFilter {
  public ColumnIsNotNullFilter(LogicalColumnDescriptor column) {
    this(column, Optional.empty());
  }

  public ColumnIsNotNullFilter(LogicalColumnDescriptor column, Optional<String> mapKey) {
    super(column, FilterOperator.isNotNull, null, mapKey);
  }
}
