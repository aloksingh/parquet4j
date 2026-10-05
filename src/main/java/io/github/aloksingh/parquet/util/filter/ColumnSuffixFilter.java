package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound suffix predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnSuffixFilter extends TypedColumnFilter {
  public ColumnSuffixFilter(LogicalColumnDescriptor column, String matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnSuffixFilter(LogicalColumnDescriptor column, String matchValue,
                            Optional<String> mapKey) {
    super(column, FilterOperator.suffix, matchValue, mapKey);
  }
}
