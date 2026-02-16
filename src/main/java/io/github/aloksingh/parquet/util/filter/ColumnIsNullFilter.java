package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnIsNullFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;

  public ColumnIsNullFilter(LogicalColumnDescriptor targetColumnDescriptor) {
    this.targetColumnDescriptor = targetColumnDescriptor;
  }

  @Override
  public boolean apply(Object colValue) {
    return colValue == null;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }

  @Override
  public boolean skip(ColumnStatistics statistics, Object colValue) {
    // For IsNull filter, we're looking for null values
    // If statistics show no nulls (nullCount == 0), we can skip this row group
    if (statistics.hasNullCount()) {
      return statistics.nullCount() < 1;
    }
    // If we don't have null count information, conservatively don't skip
    return false;
  }
}
