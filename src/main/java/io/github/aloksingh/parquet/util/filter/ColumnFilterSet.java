package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.List;

/** Immutable AND/OR predicates over one logical column. */
public class ColumnFilterSet implements ColumnFilter {
  private final LogicalColumnDescriptor columnDescriptor;
  private final FilterJoinType type;
  private final List<ColumnFilter> filters;

  public ColumnFilterSet(LogicalColumnDescriptor columnDescriptor, FilterJoinType type,
                         ColumnFilter... filters) {
    this(columnDescriptor, type, List.of(filters));
  }

  public ColumnFilterSet(LogicalColumnDescriptor columnDescriptor, FilterJoinType type,
                         List<ColumnFilter> filters) {
    if (type == null || filters == null || columnDescriptor == null && !filters.isEmpty()) {
      throw new IllegalArgumentException("A join type and a target column for non-empty filters are required");
    }
    this.columnDescriptor = columnDescriptor;
    this.type = type;
    this.filters = List.copyOf(filters);
    for (ColumnFilter filter : this.filters) {
      var target = filter.targetColumn();
      if (target != null && (!target.getName().equals(columnDescriptor.getName())
          || target.getLogicalType() != columnDescriptor.getLogicalType()
          || target.isPrimitive() && target.getPhysicalType() != columnDescriptor.getPhysicalType())) {
        throw new IllegalArgumentException("Filter for '" + target.getName()
            + "' cannot be combined on column '" + columnDescriptor.getName() + "'");
      }
    }
  }

  public List<ColumnFilter> getFilters() {
    return filters;
  }

  @Override
  public String expression() {
    String joiner = type == FilterJoinType.All ? " AND " : " OR ";
    StringBuilder text = new StringBuilder(columnDescriptor.getName()).append(": (");
    for (int i = 0; i < filters.size(); i++) {
      if (i > 0) text.append(joiner);
      text.append(filters.get(i).expression());
    }
    return text.append(')').toString();
  }

  public FilterJoinType getJoinType() {
    return type;
  }

  @Override
  public boolean apply(Object colValue) {
    for (ColumnFilter filter : filters) {
      boolean matched = filter.apply(colValue);
      if (type == FilterJoinType.All && !matched) return false;
      if (type == FilterJoinType.Any && matched) return true;
    }
    return type == FilterJoinType.All;
  }

  @Override
  public LogicalColumnDescriptor targetColumn() {
    return columnDescriptor;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return this.columnDescriptor != null && this.columnDescriptor.equals(columnDescriptor);
  }

  @Override
  public boolean canDrop(ColumnStatistics statistics, long numValues) {
    for (ColumnFilter filter : filters) {
      boolean impossible = filter.canDrop(statistics, numValues);
      if (type == FilterJoinType.All && impossible) return true;
      if (type == FilterJoinType.Any && !impossible) return false;
    }
    return type == FilterJoinType.Any;
  }
}
