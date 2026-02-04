package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.List;

public class ColumnContainsFilter implements ColumnFilter {
  private final Object matchValue;

  public ColumnContainsFilter(Object matchValue) {
    this.matchValue = matchValue;
  }

  @Override
  public boolean apply(LogicalColumnDescriptor columnDescriptor, Object colValue) {
    if (colValue == null) {
      return false;
    }
    if (columnDescriptor.isPrimitive()) {
      if (colValue instanceof String && matchValue instanceof String) {
        return ((String) colValue).contains((String) matchValue);
      }
      return false;
    } else if (columnDescriptor.isList()) {
      List listValues = (List) colValue;
      return listValues.contains(matchValue);
    }
    return false;
  }
}
