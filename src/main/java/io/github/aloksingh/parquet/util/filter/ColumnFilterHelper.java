package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.math.BigDecimal;

public class ColumnFilterHelper {
  public static final ColumnFilterHelper CFH = new ColumnFilterHelper();

  public ColumnFilterHelper() {

  }

  /** Bind keyed MAP constants to the declared value type, never to a row's runtime class. */
  public Object convertToColumnType(LogicalColumnDescriptor targetColumnDescriptor,
                                    Object matchValue, java.util.Optional<String> mapKey) {
    if (mapKey.isPresent()) {
      if (!targetColumnDescriptor.isMap() || targetColumnDescriptor.getMapMetadata() == null) {
        throw new IllegalArgumentException("A keyed predicate requires MAP metadata for column "
            + targetColumnDescriptor.getName());
      }
      var metadata = targetColumnDescriptor.getMapMetadata();
      return convertToColumnType(new LogicalColumnDescriptor(targetColumnDescriptor.getName(),
          io.github.aloksingh.parquet.model.LogicalType.PRIMITIVE, metadata.valueType(),
          metadata.valueDescriptor()), matchValue);
    }
    return convertToColumnType(targetColumnDescriptor, matchValue);
  }

  public Object convertToColumnType(LogicalColumnDescriptor targetColumnDescriptor,
                                    Object matchValue) {
    if (!targetColumnDescriptor.isPrimitive()) {
      return matchValue;
    }
    Type physicalType = targetColumnDescriptor.getPhysicalType();
    if (physicalType == null) {
      throw new IllegalArgumentException("Column value type is missing: " + targetColumnDescriptor.getName());
    }
    if (matchValue == null) {
      return null;
    }
    switch (physicalType) {
      case BOOLEAN -> {
        return strictBoolean(matchValue);
      }
      case INT32 -> {
        return exactIntegral(matchValue, true);
      }
      case INT64 -> {
        return exactIntegral(matchValue, false);
      }
      case FLOAT -> {
        return floating(matchValue, true);
      }
      case DOUBLE -> {
        return floating(matchValue, false);
      }
      case BYTE_ARRAY -> {
        if (matchValue instanceof String) return matchValue;
        if (matchValue instanceof byte[] bytes) return bytes.clone();
      }
      case FIXED_LEN_BYTE_ARRAY -> {
        if (matchValue instanceof byte[] bytes) {
          var descriptor = targetColumnDescriptor.getPhysicalDescriptor();
          if (descriptor == null || descriptor.typeLength() <= 0) {
            throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY requires a positive typeLength");
          }
          if (bytes.length != descriptor.typeLength()) {
            throw new IllegalArgumentException("Expected " + descriptor.typeLength() + " bytes for column '"
                + targetColumnDescriptor.getName() + "', got " + bytes.length);
          }
          return bytes.clone();
        }
      }
      default -> { }
    }
    throw new IllegalArgumentException("Invalid constant for " + physicalType + " column '"
        + targetColumnDescriptor.getName() + "': " + matchValue.getClass().getName());
  }

  public Object convertToClassType(Class<?> aClass, Object matchValue) {
    if (matchValue == null) {
      return null;
    }
    String strMatchVal = String.valueOf(matchValue);
    if (aClass.equals(Long.class)) {
      return exactIntegral(matchValue, false);
    }
    if (aClass.equals(Integer.class)) {
      return exactIntegral(matchValue, true);
    }
    if (aClass.equals(Float.class)) {
      return floating(matchValue, true);
    }
    if (aClass.equals(Double.class)) {
      return floating(matchValue, false);
    }
    if (aClass.equals(Boolean.class)) {
      return strictBoolean(matchValue);
    }
    return matchValue;
  }

  private Object floating(Object value, boolean singlePrecision) {
    if (!(value instanceof Number) && !(value instanceof String)) {
      throw new IllegalArgumentException("Expected a floating-point constant, got " + value);
    }
    boolean nonFiniteLiteral = value instanceof Float f && !Float.isFinite(f)
        || value instanceof Double d && !Double.isFinite(d);
    Number number;
    if (value instanceof String s) {
      if (s.equals("NaN") || s.equals("Infinity") || s.equals("-Infinity")) {
        number = Double.valueOf(s);
        nonFiniteLiteral = true;
      } else {
        number = new BigDecimal(s);
      }
    } else {
      number = (Number) value;
    }
    if (singlePrecision) {
      float result = number.floatValue();
      if (!nonFiniteLiteral && !Float.isFinite(result)) {
        throw new IllegalArgumentException("FLOAT constant overflows: " + value);
      }
      return result;
    }
    double result = number.doubleValue();
    if (!nonFiniteLiteral && !Double.isFinite(result)) {
      throw new IllegalArgumentException("DOUBLE constant overflows: " + value);
    }
    return result;
  }

  private Object strictBoolean(Object value) {
    if (value == null || value instanceof Boolean) {
      return value;
    }
    if (value instanceof String s) {
      if (s.equalsIgnoreCase("true")) {
        return true;
      }
      if (s.equalsIgnoreCase("false")) {
        return false;
      }
    }
    throw new IllegalArgumentException("Expected true or false, got " + value);
  }

  private Object exactIntegral(Object value, boolean int32) {
    if (value == null) {
      return null;
    }
    if (!(value instanceof Number) && !(value instanceof String)) {
      throw new IllegalArgumentException("Expected an integral constant, got " + value.getClass().getName());
    }
    try {
      BigDecimal decimal = value instanceof BigDecimal d ? d : new BigDecimal(value.toString());
      if (int32) {
        return decimal.intValueExact();
      }
      return decimal.longValueExact();
    } catch (ArithmeticException | NumberFormatException e) {
      throw new IllegalArgumentException("Constant is not an exact " + (int32 ? "INT32" : "INT64") + ": " + value, e);
    }
  }
}
