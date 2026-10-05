package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.util.List;
import java.util.Optional;

/**
 * Creates eagerly bound, typed predicates. Unknown or case-ambiguous names and incompatible
 * constants/operators are errors. Ordinary comparisons exclude null values; eq(null) aliases
 * isNull and neq(null) aliases isNotNull. Keyed MAP nulls include null maps and absent keys.
 * Floating comparisons use exact IEEE values (NaN unordered; signed zeros equal), binary
 * comparisons use content/unsigned byte order, and strings use Java String order. Binary
 * statistics are never pruned without known logical ordering.
 */
public class ColumnFilters {

  public ColumnFilter createFilter(SchemaDescriptor schemaDescriptor,
                                   ColumnFilterDescriptor descriptor) {
    if (schemaDescriptor == null || descriptor == null || descriptor.columnName() == null
        || descriptor.columnName().isEmpty()) {
      throw new IllegalArgumentException("Schema and a non-empty column name are required");
    }
    List<LogicalColumnDescriptor> matches = schemaDescriptor.logicalColumns().stream()
        .filter(c -> c.getName().equalsIgnoreCase(descriptor.columnName())).toList();
    if (matches.isEmpty()) {
      throw new IllegalArgumentException("Unknown column '" + descriptor.columnName()
          + "'; available columns: " + schemaDescriptor.logicalColumns().stream()
          .map(LogicalColumnDescriptor::getName).toList());
    }
    if (matches.size() != 1) {
      throw new IllegalArgumentException("Ambiguous column '" + descriptor.columnName()
          + "': " + matches.stream().map(LogicalColumnDescriptor::getName).toList());
    }
    return createFilter(matches.get(0), descriptor.filterOperator(), descriptor.matchValue(),
        descriptor.mapKey());
  }

  public ColumnFilter createFilter(LogicalColumnDescriptor columnDescriptor,
                                   FilterOperator operator, Object matchValue) {
    return createFilter(columnDescriptor, operator, matchValue, Optional.empty());
  }

  public ColumnFilter createFilter(LogicalColumnDescriptor columnDescriptor,
                                   FilterOperator operator, Object matchValue,
                                   Optional<String> mapKey) {
    if (operator == null) {
      throw new IllegalArgumentException("Filter operator must not be null");
    }
    switch (operator) {
      case eq:
        return new ColumnEqualFilter(columnDescriptor, matchValue, mapKey);
      case neq:
        return new ColumnNotEqualFilter(columnDescriptor, matchValue, mapKey);
      case lt:
        return new ColumnLessThanFilter(columnDescriptor, matchValue, mapKey);
      case lte:
        return new ColumnLessThanOrEqualFilter(columnDescriptor, matchValue, mapKey);
      case gt:
        return new ColumnGreaterThanFilter(columnDescriptor, matchValue, mapKey);
      case gte:
        return new ColumnGreaterThanOrEqualFilter(columnDescriptor, matchValue, mapKey);
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
