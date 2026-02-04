package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnIsNullFilter implements ColumnFilter {

  public ColumnIsNullFilter() {
  }

  @Override
  public boolean apply(LogicalColumnDescriptor columnDescriptor, Object colValue) {
    return colValue == null;
  }
}
