package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ColumnContainsFilter implements ColumnFilter {
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final Object matchValue;

  public ColumnContainsFilter(LogicalColumnDescriptor targetColumnDescriptor, Object matchValue) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.matchValue = matchValue;
  }

  @Override
  public boolean apply(Object colValue) {
    if (colValue == null) {
      return false;
    }
    if (targetColumnDescriptor.isPrimitive()) {
      if (colValue instanceof String && matchValue instanceof String) {
        return ((String) colValue).contains((String) matchValue);
      }
      return false;
    } else if (targetColumnDescriptor.isList()) {
      List listValues = (List) colValue;
      return listValues.contains(matchValue);
    } else if (targetColumnDescriptor.isMap()) {
      Map<?, ?> mapValues = (Map<?, ?>) colValue;
      return mapValues.containsValue(matchValue);
    }
    return false;
  }

  @Override
  public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
    return targetColumnDescriptor.equals(columnDescriptor);
  }

  @Override
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
          if (colValue != null) {
            return (Objects.equals(min, colValue) || Objects.equals(max, colValue));
          }
        }
        case INT32 -> {
          if (colValue != null) {
            Integer v = (Integer) colValue;
            Integer i1 = (Integer) min;
            Integer i2 = (Integer) max;
            return i1 <= v && i2 >= v;
          }
        }
        case INT64 -> {
          if (colValue != null) {
            Long v = (Long) colValue;
            Long i1 = (Long) min;
            Long i2 = (Long) max;
            return i1 <= v && i2 >= v;
          }
        }
        case FLOAT -> {
          if (colValue != null) {
            Float v = (Float) colValue;
            Float i1 = (Float) min;
            Float i2 = (Float) max;
            return i1 <= v && i2 >= v;
          }
        }
        case DOUBLE -> {
          if (colValue != null) {
            Double v = (Double) colValue;
            Double i1 = (Double) min;
            Double i2 = (Double) max;
            return i1 <= v && i2 >= v;
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
