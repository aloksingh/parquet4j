package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnIsNotNullFilter implements ColumnFilter {

  public ColumnIsNotNullFilter() {
  }

  @Override
  public boolean apply(LogicalColumnDescriptor columnDescriptor, Object colValue) {
    return colValue != null;
  }
}
