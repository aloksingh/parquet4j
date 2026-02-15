package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnIsNotNullFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;

  public ColumnIsNotNullFilter(LogicalColumnDescriptor targetColumnDescriptor) {
    this.targetColumnDescriptor = targetColumnDescriptor;
  }

  @Override
  public boolean apply(Object colValue) {
    return colValue != null;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }
}
