package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnPrefixFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final String matchValue;

  public ColumnPrefixFilter(LogicalColumnDescriptor targetColumnDescriptor, String matchValue) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.matchValue = matchValue;
  }

  @Override
  public boolean apply(Object colValue) {
    if (colValue == null || matchValue == null) {
      return false;
    }
    if (!targetColumnDescriptor.isPrimitive()) {
      return false;
    }
    if (!(colValue instanceof String)) {
      return false;
    }
    return ((String) colValue).startsWith(matchValue);
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }

  @Override
  public boolean skip(ColumnStatistics statistics, Object colValue) {
    // Conservative approach: don't skip by default
    // TODO: Implement proper statistics-based skipping once value decoding is available
    return false;
  }
}
