package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Optional;

/** A bound prefix predicate. See {@link ColumnFilters} for null semantics. */
public class ColumnPrefixFilter extends TypedColumnFilter {
  public ColumnPrefixFilter(LogicalColumnDescriptor column, String matchValue) {
    this(column, matchValue, Optional.empty());
  }

  public ColumnPrefixFilter(LogicalColumnDescriptor column, String matchValue,
                            Optional<String> mapKey) {
    super(column, FilterOperator.prefix, matchValue, mapKey);
  }
}
