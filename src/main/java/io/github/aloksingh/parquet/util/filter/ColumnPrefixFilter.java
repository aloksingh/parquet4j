package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Map;
import java.util.Optional;

public class ColumnPrefixFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final String matchValue;
  private final Optional<String> mapKey;

  public ColumnPrefixFilter(LogicalColumnDescriptor targetColumnDescriptor, String matchValue) {
    this(targetColumnDescriptor, matchValue, Optional.empty());
  }

  public ColumnPrefixFilter(LogicalColumnDescriptor targetColumnDescriptor, String matchValue,
                            Optional<String> mapKey) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.matchValue = matchValue;
    this.mapKey = mapKey;
  }

  @Override
  public boolean apply(Object colValue) {
    if (colValue == null || matchValue == null) {
      return false;
    }
    if (targetColumnDescriptor.isPrimitive()) {
      if (!(colValue instanceof String)) {
        return false;
      }
      return ((String) colValue).startsWith(matchValue);
    } else if (targetColumnDescriptor.isMap()) {
      // If mapKey is present, extract value from colValue map at that key and check prefix
      if (mapKey.isPresent()) {
        Map valueMap = (Map) colValue;
        Object actualValue = valueMap.get(mapKey.get());
        if (actualValue == null) {
          return false;
        }
        if (!(actualValue instanceof String)) {
          return false;
        }
        return ((String) actualValue).startsWith(matchValue);
      }
    }
    return false;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }

  @Override
  public boolean skip(ColumnStatistics statistics, Object colValue) {
    // Conservative approach: don't skip by default
    // TODO: Implement proper statistics-based skipping once value decoding is available
    return false;
  }
}
