package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound isNull predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnIsNullFilter extends TypedColumnFilter {
  public ColumnIsNullFilter(LogicalColumnDescriptor column) {
    this(column, Optional.empty());
  }

  public ColumnIsNullFilter(LogicalColumnDescriptor column, Optional<String> mapKey) {
    super(column, FilterOperator.isNull, null, mapKey);
  }
}
