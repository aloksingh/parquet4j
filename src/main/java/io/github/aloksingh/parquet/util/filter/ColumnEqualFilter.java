package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ColumnEqualFilter implements ColumnFilter{
  private final LogicalColumnDescriptor targetColumnDescriptor;
  private final Object matchValue;
  private final Optional<String> mapKey;

  public ColumnEqualFilter(LogicalColumnDescriptor targetColumnDescriptor, Object matchValue) {
    this(targetColumnDescriptor, matchValue, Optional.empty());
  }

  public ColumnEqualFilter(LogicalColumnDescriptor targetColumnDescriptor, Object matchValue,
                           Optional<String> mapKey) {
    this.targetColumnDescriptor = targetColumnDescriptor;
    this.matchValue = mapKey.isPresent() ? matchValue :
        ColumnFilterHelper.CFH.convertToColumnType(targetColumnDescriptor, matchValue);
    this.mapKey = mapKey;
  }

  @Override
  public boolean apply(Object colValue) {
    if (colValue == null) {
      return false;
    }
    if (targetColumnDescriptor.isPrimitive()) {
      return java.util.Objects.equals(matchValue, colValue);
    } else {
      if (targetColumnDescriptor.isList()) {
        List listValues = (List) colValue;
        List matches = (List) matchValue;
        if (listValues.size() != matches.size()){
          return false;
        }
        for (int i = 0; i < matches.size(); i++) {
          Object m = matches.get(i);
          Object v = listValues.get(i);
          if (!Objects.equals(m, v)) {
            return false;
          }
        }
        return true;
      }
      if (targetColumnDescriptor.isMap()) {
        // If mapKey is present, extract value from colValue map at that key and compare with matchValue
        if (mapKey.isPresent()) {
          Map valueMap = (Map) colValue;
          Object actualValue = valueMap.get(mapKey.get());
          if (actualValue != null) {
            return Objects.equals(
                ColumnFilterHelper.CFH.convertToClassType(actualValue.getClass(), matchValue),
                actualValue);
          } else {
            return matchValue == null;
          }
        }

        // Otherwise, compare entire maps
        Map valueMap = (Map) colValue;
        Map matchMap = (Map) matchValue;
        if (valueMap.size() != matchMap.size()){
          return false;
        }
        for (Object key : matchMap.keySet()) {
          if (!valueMap.containsKey(key)){
            return false;
          }
          Object v = valueMap.get(key);
          Object m = matchMap.get(key);
          if (!java.util.Objects.equals(v, m)){
            return false;
          }
        }
        return true;
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
