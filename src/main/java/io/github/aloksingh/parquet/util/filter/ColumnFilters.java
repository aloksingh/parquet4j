package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.util.List;
import java.util.Optional;

public class ColumnFilters {

  public ColumnFilter createFilter(SchemaDescriptor schemaDescriptor,
                                   ColumnFilterDescriptor descriptor) {
    List<LogicalColumnDescriptor> logicalColumnDescriptors = schemaDescriptor.logicalColumns();
    for (LogicalColumnDescriptor logicalColumnDescriptor : logicalColumnDescriptors) {
      if (logicalColumnDescriptor.getName().equalsIgnoreCase(descriptor.columnName())) {
        return createFilter(logicalColumnDescriptor, descriptor.filterOperator(),
            descriptor.matchValue(), descriptor.mapKey());
      }
    }
    return null;
  }

  public ColumnFilter createFilter(LogicalColumnDescriptor columnDescriptor,
                                   FilterOperator operator, Object matchValue) {
    return createFilter(columnDescriptor, operator, matchValue, Optional.empty());
  }

  public ColumnFilter createFilter(LogicalColumnDescriptor columnDescriptor,
                                   FilterOperator operator, Object matchValue,
                                   Optional<String> mapKey) {
    switch (operator) {
      case eq:
        return new ColumnEqualFilter(columnDescriptor, matchValue, mapKey);
      case neq:
        return new ColumnNotEqualFilter(columnDescriptor, matchValue, mapKey);
      case lt:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for lt operator");
        }
        return new ColumnLessThanFilter(columnDescriptor, (Comparable) matchValue, mapKey);
      case lte:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for lte operator");
        }
        return new ColumnLessThanOrEqualFilter(columnDescriptor, (Comparable) matchValue, mapKey);
      case gt:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for gt operator");
        }
        return new ColumnGreaterThanFilter(columnDescriptor, (Comparable) matchValue, mapKey);
      case gte:
        if (!(matchValue instanceof Comparable)) {
          throw new IllegalArgumentException("matchValue must be Comparable for gte operator");
        }
        return new ColumnGreaterThanOrEqualFilter(columnDescriptor, (Comparable) matchValue,
            mapKey);
      case contains:
        return new ColumnContainsFilter(columnDescriptor, matchValue, mapKey);
      case prefix:
        if (!(matchValue instanceof String)) {
          throw new IllegalArgumentException("matchValue must be String for prefix operator");
        }
        return new ColumnPrefixFilter(columnDescriptor, (String) matchValue, mapKey);
      case suffix:
        if (!(matchValue instanceof String)) {
          throw new IllegalArgumentException("matchValue must be String for suffix operator");
        }
        return new ColumnSuffixFilter(columnDescriptor, (String) matchValue, mapKey);
      case isNull:
        return new ColumnIsNullFilter(columnDescriptor, mapKey);
      case isNotNull:
        return new ColumnIsNotNullFilter(columnDescriptor, mapKey);
      default:
        throw new IllegalArgumentException("Unsupported operator: " + operator);
    }
  }
}
