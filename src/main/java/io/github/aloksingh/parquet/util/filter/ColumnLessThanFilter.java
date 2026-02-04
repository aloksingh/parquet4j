package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnLessThanFilter implements ColumnFilter {
  private final Comparable matchValue;

  public ColumnLessThanFilter(Comparable matchValue) {
    this.matchValue = matchValue;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean apply(LogicalColumnDescriptor columnDescriptor, Object colValue) {
    if (colValue == null || matchValue == null) {
      return false;
    }
    if (!columnDescriptor.isPrimitive()) {
      return false;
    }
    if (!(colValue instanceof Comparable)) {
      return false;
    }
    try {
      return ((Comparable) colValue).compareTo(matchValue) < 0;
    } catch (ClassCastException e) {
      return false;
    }
  }
}
