package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public class ColumnFilters {

  public ColumnFilter createFilter(LogicalColumnDescriptor columnDescriptor,
                                   FilterOperator operator, Object matchValue) {
    switch (operator) {
      case eq:
        return new ColumnEqualFilter(columnDescriptor, matchValue);
      case neq:
        return new ColumnNotEqualFilter(columnDescriptor, matchValue);
      case lt:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for lt operator");
        }
        return new ColumnLessThanFilter(columnDescriptor, (Comparable) matchValue);
      case lte:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for lte operator");
        }
        return new ColumnLessThanOrEqualFilter(columnDescriptor, (Comparable) matchValue);
      case gt:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for gt operator");
        }
        return new ColumnGreaterThanFilter(columnDescriptor, (Comparable) matchValue);
      case gte:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for gte operator");
        }
        return new ColumnGreaterThanOrEqualFilter(columnDescriptor, (Comparable) matchValue);
      case contains:
        return new ColumnContainsFilter(columnDescriptor, matchValue);
      case prefix:
        if (!(matchValue instanceof String)) {
          throw new IllegalArgumentException("matchValue must be String for prefix operator");
        }
        return new ColumnPrefixFilter(columnDescriptor, (String) matchValue);
      case suffix:
        if (!(matchValue instanceof String)) {
          throw new IllegalArgumentException("matchValue must be String for suffix operator");
        }
        return new ColumnSuffixFilter(columnDescriptor, (String) matchValue);
      case isNull:
        return new ColumnIsNullFilter(columnDescriptor);
      case isNotNull:
        return new ColumnIsNotNullFilter(columnDescriptor);
      default:
        throw new IllegalArgumentException("Unsupported operator: " + operator);
    }
  }
}
