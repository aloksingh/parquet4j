package io.github.aloksingh.parquet.util.filter.query;

import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.util.filter.FilterOperator;
import java.util.Optional;

public interface QueryParser {

  ColumnFilterDescriptor parse(String expression);

  record ColumnFilterDescriptor(String columnName, LogicalType type,
                                FilterOperator filterOperator, Object matchValue,
                                Optional<String> mapKey) {
    public ColumnFilterDescriptor(String columnName, LogicalType type,
                                  FilterOperator filterOperator, Object matchValue) {
      this(columnName, type, filterOperator, matchValue, Optional.empty());
    }
  }
}
