package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
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

  @Override
  public boolean skip(ColumnStatistics statistics, Object colValue) {
    // Combine skip logic from all filters based on join type
    for (ColumnFilter filter : filters) {
      boolean shouldSkip = filter.skip(statistics, colValue);
      switch (type) {
        case All -> {
          // For All join type, if any filter says don't skip, we don't skip
          if (!shouldSkip) {
            return false;
          }
        }
        case Any -> {
          // For Any join type, if any filter says skip, we skip
          if (shouldSkip) {
            return true;
          }
        }
      }
    }
    // If All: all filters say skip, so skip
    // If Any: no filter says skip, so don't skip
    return type == FilterJoinType.All;
  }
}
