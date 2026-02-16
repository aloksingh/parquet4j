package io.github.aloksingh.parquet.util.filter.query;

import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.util.filter.FilterOperator;

public interface QueryParser {

  ColumnFilterDescriptor parse(String expression);

  public record ColumnFilterDescriptor(String columnName, LogicalType type,
                                       FilterOperator filterOperator, Object matchValue) {

  }
}
