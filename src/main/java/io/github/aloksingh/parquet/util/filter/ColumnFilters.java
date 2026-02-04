package io.github.aloksingh.parquet.util.filter;

public class ColumnFilters {

  public ColumnFilter createFilter(FilterOperator operator, Object matchValue) {
    switch (operator) {
      case eq:
        return new ColumnEqualFilter(matchValue);
      case neq:
        return new ColumnNotEqualFilter(matchValue);
      case lt:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for lt operator");
        }
        return new ColumnLessThanFilter((Comparable) matchValue);
      case lte:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for lte operator");
        }
        return new ColumnLessThanOrEqualFilter((Comparable) matchValue);
      case gt:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for gt operator");
        }
        return new ColumnGreaterThanFilter((Comparable) matchValue);
      case gte:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for gte operator");
        }
        return new ColumnGreaterThanOrEqualFilter((Comparable) matchValue);
      case contains:
        return new ColumnContainsFilter(matchValue);
      case prefix:
        if (!(matchValue instanceof String)) {
          throw new IllegalArgumentException("matchValue must be String for prefix operator");
        }
        return new ColumnPrefixFilter((String) matchValue);
      case suffix:
        if (!(matchValue instanceof String)) {
          throw new IllegalArgumentException("matchValue must be String for suffix operator");
        }
        return new ColumnSuffixFilter((String) matchValue);
      case isNull:
        return new ColumnIsNullFilter();
      case isNotNull:
        return new ColumnIsNotNullFilter();
      default:
        throw new IllegalArgumentException("Unsupported operator: " + operator);
    }
  }
}
