package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.List;

public class ColumnContainsFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final Object matchValue;

  public ColumnContainsFilter(LogicalColumnDescriptor targetColumnDescriptor, Object matchValue) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.matchValue = matchValue;
  }

  @Override
  public boolean apply(Object colValue) {
    if (colValue == null) {
      return false;
    }
    if (targetColumnDescriptor.isPrimitive()) {
      if (colValue instanceof String && matchValue instanceof String) {
        return ((String) colValue).contains((String) matchValue);
      }
      return false;
    } else if (targetColumnDescriptor.isList()) {
      List listValues = (List) colValue;
      return listValues.contains(matchValue);
    }
    return false;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }

  @Override
  public boolean skip(ColumnStatistics statistics, Object colValue) {
    return false;
  }
}
