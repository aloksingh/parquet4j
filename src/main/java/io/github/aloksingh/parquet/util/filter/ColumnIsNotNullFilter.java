package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Map;
import java.util.Optional;

public class ColumnIsNotNullFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final Optional<String> mapKey;

  public ColumnIsNotNullFilter(LogicalColumnDescriptor targetColumnDescriptor) {
    this(targetColumnDescriptor, Optional.empty());
  }

  public ColumnIsNotNullFilter(LogicalColumnDescriptor targetColumnDescriptor,
                               Optional<String> mapKey) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.mapKey = mapKey;
  }

  @Override
  public boolean apply(Object colValue) {
    if (targetColumnDescriptor.isMap() && mapKey.isPresent()) {
      // For map columns with a specific key, check if value at that key is not null
      if (colValue == null) {
        return true; // The map itself is null, the key/value is also null
      }
      Map valueMap = (Map) colValue;
      Object actualValue = valueMap.get(mapKey.get());
      return actualValue != null;
    }
    // For primitive columns or map without key, check if the column value itself is not null
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
