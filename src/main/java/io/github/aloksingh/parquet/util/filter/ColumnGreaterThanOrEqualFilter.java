package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnGreaterThanOrEqualFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final Comparable matchValue;

  public ColumnGreaterThanOrEqualFilter(LogicalColumnDescriptor targetColumnDescriptor,
                                        Comparable matchValue) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.matchValue = matchValue;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean apply(Object colValue) {
    if (colValue == null || matchValue == null) {
      return false;
    }
    if (!targetColumnDescriptor.isPrimitive()) {
      return false;
    }
    if (!(colValue instanceof Comparable)) {
      return false;
    }
    try {
      return ((Comparable) colValue).compareTo(matchValue) >= 0;
    } catch (ClassCastException e) {
      return false;
    }
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }
}
