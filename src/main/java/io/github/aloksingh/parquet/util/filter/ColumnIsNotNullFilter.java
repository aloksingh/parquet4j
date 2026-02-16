package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnIsNotNullFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;

  public ColumnIsNotNullFilter(LogicalColumnDescriptor targetColumnDescriptor) {
    this.targetColumnDescriptor = targetColumnDescriptor;
  }

  @Override
  public boolean apply(Object colValue) {
    return colValue != null;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }

  @Override
  public boolean skip(ColumnStatistics statistics, Object colValue) {
    // For IsNotNull filter, we're looking for non-null values
    // We can only skip if we know ALL values in the row group are null
    // However, with just nullCount, we cannot determine if ALL values are null
    // (we would need the total count of values in the row group)
    // Therefore, we conservatively never skip
    //
    // Note: Even if nullCount > 0, there might still be non-null values
    // And if nullCount == 0, all values are non-null, which we want to keep
    return false;
  }
}
