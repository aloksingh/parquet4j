package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalType;
import java.util.Optional;

public record ColumnFilterDescriptor(String columnName, LogicalType type,
                                     FilterOperator filterOperator, Object matchValue,
                                     Optional<String> mapKey) {
  public ColumnFilterDescriptor(String columnName, LogicalType type,
                                FilterOperator filterOperator, Object matchValue) {
    this(columnName, type, filterOperator, matchValue, Optional.empty());
  }
}
