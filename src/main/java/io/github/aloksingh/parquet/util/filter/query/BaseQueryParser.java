package io.github.aloksingh.parquet.util.filter.query;

import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.util.filter.ColumnFilterDescriptor;
import io.github.aloksingh.parquet.util.filter.FilterOperator;
import java.util.Optional;

public class BaseQueryParser implements QueryParser {

  private String stripQuotes(String value) {
    if (value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    } else if (value.startsWith("'") && value.endsWith("'")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  @Override
  public ColumnFilterDescriptor parse(String expression) {
    // Split on first '=' to get column and value
    int eqIndex = expression.indexOf('=');
    if (eqIndex == -1) {
      throw new IllegalArgumentException("Invalid expression: " + expression);
    }

    String columnPart = expression.substring(0, eqIndex);
    String valuePart = expression.substring(eqIndex + 1);

    // Handle map key syntax: column["key"] or quoted column names
    String columnName;
    Optional<String> mapKey = Optional.empty();

    int bracketStart = columnPart.indexOf('[');
    if (bracketStart != -1) {
      // Map key syntax detected
      int bracketEnd = columnPart.indexOf(']');
      if (bracketEnd == -1 || bracketEnd < bracketStart) {
        throw new IllegalArgumentException("Invalid map key syntax: " + expression);
      }

      // Extract the key part (between brackets)
      String keyPart = columnPart.substring(bracketStart + 1, bracketEnd);

      // Handle quoted keys
      String key = keyPart;
      if (keyPart.startsWith("\"") && keyPart.endsWith("\"")) {
        key = keyPart.substring(1, keyPart.length() - 1);
      }

      columnName = columnPart.substring(0, bracketStart);
      mapKey = Optional.of(key);
    } else {
      // Handle quoted column names
      columnName = columnPart;
      if (columnPart.startsWith("\"") && columnPart.endsWith("\"")) {
        columnName = columnPart.substring(1, columnPart.length() - 1);
      }
    }

    // Parse the value part to determine operator and value
    FilterOperator operator;
    Object matchValue;

    if (valuePart.equals("isNull()")) {
      operator = FilterOperator.isNull;
      matchValue = null;
    } else if (valuePart.equals("isNotNull()")) {
      operator = FilterOperator.isNotNull;
      matchValue = null;
    } else if (valuePart.startsWith("lt(") && valuePart.endsWith(")")) {
      operator = FilterOperator.lt;
      matchValue = stripQuotes(valuePart.substring(3, valuePart.length() - 1));
    } else if (valuePart.startsWith("lte(") && valuePart.endsWith(")")) {
      operator = FilterOperator.lte;
      matchValue = stripQuotes(valuePart.substring(4, valuePart.length() - 1));
    } else if (valuePart.startsWith("gt(") && valuePart.endsWith(")")) {
      operator = FilterOperator.gt;
      matchValue = stripQuotes(valuePart.substring(3, valuePart.length() - 1));
    } else if (valuePart.startsWith("gte(") && valuePart.endsWith(")")) {
      operator = FilterOperator.gte;
      matchValue = stripQuotes(valuePart.substring(4, valuePart.length() - 1));
    } else if (valuePart.startsWith("*") && valuePart.endsWith("*") && valuePart.length() > 1) {
      operator = FilterOperator.contains;
      matchValue = valuePart.substring(1, valuePart.length() - 1);
    } else if (valuePart.startsWith("*")) {
      operator = FilterOperator.suffix;
      matchValue = valuePart.substring(1);
    } else if (valuePart.endsWith("*")) {
      operator = FilterOperator.prefix;
      matchValue = valuePart.substring(0, valuePart.length() - 1);
    } else {
      operator = FilterOperator.eq;
      // Strip quotes if present (both double and single quotes)
      if (valuePart.startsWith("\"") && valuePart.endsWith("\"")) {
        matchValue = valuePart.substring(1, valuePart.length() - 1);
      } else if (valuePart.startsWith("'") && valuePart.endsWith("'")) {
        matchValue = valuePart.substring(1, valuePart.length() - 1);
      } else {
        matchValue = valuePart;
      }
    }

    if (mapKey.isPresent()) {
      return new ColumnFilterDescriptor(columnName, LogicalType.PRIMITIVE, operator, matchValue,
          mapKey);
    } else {
      return new ColumnFilterDescriptor(columnName, LogicalType.PRIMITIVE, operator, matchValue);
    }
  }
}
