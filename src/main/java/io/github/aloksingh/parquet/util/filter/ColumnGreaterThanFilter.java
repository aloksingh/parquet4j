package io.github.aloksingh.parquet.util.filter;

import static io.github.aloksingh.parquet.util.filter.ColumnFilterHelper.CFH;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.Map;
import java.util.Optional;

public class ColumnGreaterThanFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final Comparable matchValue;
  private final Optional<String> mapKey;

  public ColumnGreaterThanFilter(LogicalColumnDescriptor targetColumnDescriptor,
                                 Comparable matchValue) {
    this(targetColumnDescriptor, matchValue, Optional.empty());
  }

  public ColumnGreaterThanFilter(LogicalColumnDescriptor targetColumnDescriptor,
                                 Comparable matchValue,
                                 Optional<String> mapKey) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.matchValue = mapKey.isPresent() ? matchValue :
        (Comparable) CFH.convertToColumnType(targetColumnDescriptor, matchValue);
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
        return ((Comparable) colValue).compareTo(matchValue) > 0;
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
          return ((Comparable) actualValue).compareTo(
              CFH.convertToClassType(actualValue.getClass(), matchValue)) > 0;
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
            Boolean boolMax = (Boolean) max;
            Boolean v = (Boolean) colValue;
            // For boolean, only true > false
            // If max is false, no values can be > false
            // If max is true and v is false, then true > false, so keep
            try {
              return ((Comparable) max).compareTo(v) > 0;
            } catch (Exception e) {
              return false;
            }
          }
        }
        case INT32 -> {
          if (colValue != null) {
            Integer v = (Integer) colValue;
            Integer maxVal = (Integer) max;
            return maxVal > v;
          }
        }
        case INT64 -> {
          if (colValue != null) {
            Long v = (Long) colValue;
            Long maxVal = (Long) max;
            return maxVal > v;
          }
        }
        case FLOAT -> {
          if (colValue != null) {
            Float v = (Float) colValue;
            Float maxVal = (Float) max;
            return maxVal > v;
          }
        }
        case DOUBLE -> {
          if (colValue != null) {
            Double v = (Double) colValue;
            Double maxVal = (Double) max;
            return maxVal > v;
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
