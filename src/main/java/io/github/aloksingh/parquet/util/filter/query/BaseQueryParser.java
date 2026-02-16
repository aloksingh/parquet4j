package io.github.aloksingh.parquet.util.filter.query;

import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.util.filter.FilterOperator;

public class BaseQueryParser implements QueryParser {
  @Override
  public ColumnFilterDescriptor parse(String expression) {
    // Split on first '=' to get column and value
    int eqIndex = expression.indexOf('=');
    if (eqIndex == -1) {
      throw new IllegalArgumentException("Invalid expression: " + expression);
    }

    String columnPart = expression.substring(0, eqIndex);
    String valuePart = expression.substring(eqIndex + 1);

    // Handle quoted column names
    String columnName = columnPart;
    if (columnPart.startsWith("\"") && columnPart.endsWith("\"")) {
      columnName = columnPart.substring(1, columnPart.length() - 1);
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
      matchValue = valuePart.substring(3, valuePart.length() - 1);
    } else if (valuePart.startsWith("lte(") && valuePart.endsWith(")")) {
      operator = FilterOperator.lte;
      matchValue = valuePart.substring(4, valuePart.length() - 1);
    } else if (valuePart.startsWith("gt(") && valuePart.endsWith(")")) {
      operator = FilterOperator.gt;
      matchValue = valuePart.substring(3, valuePart.length() - 1);
    } else if (valuePart.startsWith("gte(") && valuePart.endsWith(")")) {
      operator = FilterOperator.gte;
      matchValue = valuePart.substring(4, valuePart.length() - 1);
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
      // Strip quotes if present
      if (valuePart.startsWith("\"") && valuePart.endsWith("\"")) {
        matchValue = valuePart.substring(1, valuePart.length() - 1);
      } else {
        matchValue = valuePart;
      }
    }

    return new ColumnFilterDescriptor(columnName, LogicalType.PRIMITIVE, operator, matchValue);
  }
}
