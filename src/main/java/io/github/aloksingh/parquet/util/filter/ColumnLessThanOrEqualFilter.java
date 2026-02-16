package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Map;
import java.util.Optional;

public class ColumnLessThanOrEqualFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final Comparable matchValue;
  private final Optional<String> mapKey;

  public ColumnLessThanOrEqualFilter(LogicalColumnDescriptor targetColumnDescriptor,
                                     Comparable matchValue) {
    this(targetColumnDescriptor, matchValue, Optional.empty());
  }

  public ColumnLessThanOrEqualFilter(LogicalColumnDescriptor targetColumnDescriptor,
                                     Comparable matchValue,
                                     Optional<String> mapKey) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.matchValue = matchValue;
    this.mapKey = mapKey;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean apply(Object colValue) {
    if (colValue == null || matchValue == null) {
      return false;
    }
    if (targetColumnDescriptor.isPrimitive()) {
      if (!(colValue instanceof Comparable)) {
        return false;
      }
      try {
        return ((Comparable) colValue).compareTo(matchValue) <= 0;
      } catch (ClassCastException e) {
        return false;
      }
    } else if (targetColumnDescriptor.isMap()) {
      // If mapKey is present, extract value from colValue map at that key and compare with matchValue
      if (mapKey.isPresent()) {
        Map valueMap = (Map) colValue;
        Object actualValue = valueMap.get(mapKey.get());
        if (actualValue == null) {
          return false;
        }
        if (!(actualValue instanceof Comparable)) {
          return false;
        }
        try {
          return ((Comparable) actualValue).compareTo(matchValue) <= 0;
        } catch (ClassCastException e) {
          return false;
        }
      }
    }
    return false;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean skip(ColumnStatistics statistics, Object colValue) {
    if (targetColumnDescriptor.isPrimitive()) {
      if (colValue == null) {
        if (statistics.hasNullCount()) {
          return statistics.nullCount() < 1;
        }
      }
      Object min = ColumnStatistics.decodeStatValue(statistics.min(),
          targetColumnDescriptor.getPhysicalType());
      Object max = ColumnStatistics.decodeStatValue(statistics.max(),
          targetColumnDescriptor.getPhysicalType());
      switch (targetColumnDescriptor.getPhysicalType()) {
        case BOOLEAN -> {
          if (colValue != null && colValue instanceof Boolean) {
            Boolean boolMin = (Boolean) min;
            Boolean v = (Boolean) colValue;
            // For boolean, only false <= true and false <= false
            // If min is true and v is false, no values can be <= false
            try {
              return ((Comparable) min).compareTo(v) <= 0;
            } catch (Exception e) {
              return false;
            }
          }
        }
        case INT32 -> {
          if (colValue != null) {
            Integer v = (Integer) colValue;
            Integer minVal = (Integer) min;
            return minVal <= v;
          }
        }
        case INT64 -> {
          if (colValue != null) {
            Long v = (Long) colValue;
            Long minVal = (Long) min;
            return minVal <= v;
          }
        }
        case FLOAT -> {
          if (colValue != null) {
            Float v = (Float) colValue;
            Float minVal = (Float) min;
            return minVal <= v;
          }
        }
        case DOUBLE -> {
          if (colValue != null) {
            Double v = (Double) colValue;
            Double minVal = (Double) min;
            return minVal <= v;
          }
        }
        case BYTE_ARRAY -> {
        }
        case FIXED_LEN_BYTE_ARRAY -> {
        }
      }
    }
    return false;
  }
}
