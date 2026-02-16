package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Map;
import java.util.Optional;

public class ColumnIsNullFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final Optional<String> mapKey;

  public ColumnIsNullFilter(LogicalColumnDescriptor targetColumnDescriptor) {
    this(targetColumnDescriptor, Optional.empty());
  }

  public ColumnIsNullFilter(LogicalColumnDescriptor targetColumnDescriptor,
                            Optional<String> mapKey) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.mapKey = mapKey;
  }

  @Override
  public boolean apply(Object colValue) {
    if (targetColumnDescriptor.isMap() && mapKey.isPresent()) {
      // For map columns with a specific key, check if value at that key is null
      if (colValue == null) {
        return false; // The map itself is null, not the value at the key
      }
      Map valueMap = (Map) colValue;
      Object actualValue = valueMap.get(mapKey.get());
      return actualValue == null;
    }
    // For primitive columns or map without key, check if the column value itself is null
    return colValue == null;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }

  @Override
  public boolean skip(ColumnStatistics statistics, Object colValue) {
    // For IsNull filter, we're looking for null values
    // If statistics show no nulls (nullCount == 0), we can skip this row group
    if (statistics.hasNullCount()) {
      return statistics.nullCount() < 1;
    }
    // If we don't have null count information, conservatively don't skip
    return false;
  }
}
