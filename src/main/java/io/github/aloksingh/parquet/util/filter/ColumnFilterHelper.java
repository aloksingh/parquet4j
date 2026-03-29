package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.math.BigDecimal;

public class ColumnFilterHelper {
  public static final ColumnFilterHelper CFH = new ColumnFilterHelper();

  public ColumnFilterHelper() {

  }

  public Object convertToColumnType(LogicalColumnDescriptor targetColumnDescriptor,
                                    Object matchValue) {
    if (!targetColumnDescriptor.isPrimitive()) {
      return matchValue;
    }
    Type physicalType = targetColumnDescriptor.getPhysicalType();
    if (physicalType == null) {
      return matchValue;
    }
    switch (physicalType) {
      case BOOLEAN -> {
        if (matchValue instanceof Boolean) {
          return matchValue;
        }
        if (matchValue instanceof String) {
          return Boolean.parseBoolean(String.valueOf(matchValue));
        }
        return null;
      }
      case INT32 -> {
        if (matchValue instanceof Number) {
          return ((Number) matchValue).intValue();
        }
        if (matchValue instanceof String) {
          return new BigDecimal(String.valueOf(matchValue)).intValue();
        }
      }
      case INT64 -> {
        if (matchValue instanceof Number) {
          return ((Number) matchValue).longValue();
        }
        if (matchValue instanceof String) {
          return new BigDecimal(String.valueOf(matchValue)).longValue();
        }
      }
      case FLOAT -> {
        if (matchValue instanceof Number) {
          return ((Number) matchValue).floatValue();
        }
        if (matchValue instanceof String) {
          return new BigDecimal(String.valueOf(matchValue)).floatValue();
        }
      }
      case DOUBLE -> {
        if (matchValue instanceof Number) {
          return ((Number) matchValue).doubleValue();
        }
        if (matchValue instanceof String) {
          return new BigDecimal(String.valueOf(matchValue)).doubleValue();
        }
      }
      case BYTE_ARRAY -> {
        return matchValue;
      }
      case FIXED_LEN_BYTE_ARRAY -> {
      }
    }
    return null;
  }

  public Object convertToClassType(Class<?> aClass, Object matchValue) {
    if (matchValue == null) {
      return null;
    }
    String strMatchVal = String.valueOf(matchValue);
    if (aClass.equals(Long.class)) {
      return new BigDecimal(strMatchVal).longValue();
    }
    if (aClass.equals(Integer.class)) {
      return new BigDecimal(strMatchVal).intValue();
    }
    if (aClass.equals(Float.class)) {
      return new BigDecimal(strMatchVal).floatValue();
    }
    if (aClass.equals(Double.class)) {
      return new BigDecimal(strMatchVal).doubleValue();
    }
    if (aClass.equals(Boolean.class)) {
      return Boolean.parseBoolean(strMatchVal);
    }
    return matchValue;
  }
}
