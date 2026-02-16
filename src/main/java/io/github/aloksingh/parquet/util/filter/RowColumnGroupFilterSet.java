package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import java.util.List;

public class RowColumnGroupFilterSet implements RowColumnGroupFilter {
  private final FilterJoinType type;
  private final List<ColumnFilter> filters;

  public RowColumnGroupFilterSet(FilterJoinType type, ColumnFilter... filters) {
    this(type, List.of(filters));
  }

  public RowColumnGroupFilterSet(FilterJoinType type, List<ColumnFilter> filters) {
    this.type = type;
    this.filters = filters;
  }

  public boolean apply(RowColumnGroup row) {
    int matchCount = 0;
    for (int i = 0; i < row.getColumnCount(); i++) {
      LogicalColumnDescriptor columnDescriptor = row.getSchema().getLogicalColumn(i);
      Object columnValue = row.getColumnValue(i);
      List<ColumnFilter> columnFilters =
          filters.stream().filter(f -> f.isApplicable(columnDescriptor)).toList();
      boolean columnMatched = false;
      for (ColumnFilter columnFilter : columnFilters) {
        if (columnFilter.apply(columnValue)) {
          matchCount = matchCount + 1;
          columnMatched = true;
          if (type == FilterJoinType.Any) {
            return true;
          }
        }
      }
      if (!columnFilters.isEmpty() && !columnMatched) {
        if (type == FilterJoinType.All) {
          return false;
        }
      }
    }
    return type == FilterJoinType.All && matchCount == filters.size();
  }
}
