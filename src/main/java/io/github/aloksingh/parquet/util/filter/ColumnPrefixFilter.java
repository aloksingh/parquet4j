package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnPrefixFilter implements ColumnFilter {
  private final String matchValue;

  public ColumnPrefixFilter(String matchValue) {
    this.matchValue = matchValue;
  }

  @Override
  public boolean apply(LogicalColumnDescriptor columnDescriptor, Object colValue) {
    if (colValue == null || matchValue == null) {
      return false;
    }
    if (!columnDescriptor.isPrimitive()) {
      return false;
    }
    if (!(colValue instanceof String)) {
      return false;
    }
    return ((String) colValue).startsWith(matchValue);
  }
}
