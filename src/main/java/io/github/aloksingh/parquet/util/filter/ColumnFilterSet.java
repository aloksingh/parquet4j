package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.List;

public class ColumnFilterSet implements ColumnFilter{
  private final LogicalColumnDescriptor columnDescriptor;
  private final FilterJoinType type;
  private final List<ColumnFilter> filters;

  public ColumnFilterSet(LogicalColumnDescriptor columnDescriptor, FilterJoinType type,
                         ColumnFilter... filters) {
    this(columnDescriptor, type, List.of(filters));
  }

  public ColumnFilterSet(LogicalColumnDescriptor columnDescriptor, FilterJoinType type,
                         List<ColumnFilter> filters) {
    this.columnDescriptor = columnDescriptor;
    this.type = type;
    this.filters = filters;
  }
  @Override
  public boolean apply(Object colValue) {
    for (ColumnFilter filter : filters) {
      boolean matched = filter.apply(colValue);
      switch (type){
        case All -> {
          if (!matched){
            return false;
          }
        }
        case Any -> {
          if (matched){
            return true;
          }
        }
      }
    }
    switch (type){
      case All -> {
        return true;//all must match. if there are 0 filters, then this is still true
      }
      case Any -> {
        return false; //At least one must match. if there are 0 filters, then this will be false.
      }
    }
    return false;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return this.columnDescriptor.equals(columnDescriptor);
  }
}
