package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Immutable AND/OR composition of column predicates. Known targets bind to logical indices once
 * per schema. Opaque predicates keep all columns and match if any applicable column matches.
 * Metadata pruning is per primitive physical leaf; repeated and keyed MAP leaves keep the group.
 */
public class RowColumnGroupFilterSet implements RowColumnGroupFilter {
  private final FilterJoinType type;
  private final List<ColumnFilter> filters;
  private final Map<SchemaDescriptor, List<BoundFilter>> bindings = new IdentityHashMap<>();

  public RowColumnGroupFilterSet(FilterJoinType type, ColumnFilter... filters) {
    this(type, List.of(filters));
  }

  public RowColumnGroupFilterSet(FilterJoinType type, List<ColumnFilter> filters) {
    if (type == null || filters == null) {
      throw new IllegalArgumentException("Join type and filters must not be null");
    }
    this.type = type;
    this.filters = List.copyOf(filters);
  }

  public List<ColumnFilter> getFilters() {
    return filters;
  }

  public FilterJoinType getJoinType() {
    return type;
  }

  private static boolean constant(ColumnFilter filter) {
    return filter instanceof ColumnFilterSet set && set.getFilters().isEmpty();
  }

  private synchronized List<BoundFilter> bind(SchemaDescriptor schema) {
    if (schema == null) throw new IllegalArgumentException("Schema must not be null");
    var cached = bindings.get(schema);
    if (cached != null) return cached;
    var bound = new ArrayList<BoundFilter>();
    for (var filter : filters) {
      var indices = new ArrayList<Integer>();
      var target = filter.targetColumn();
      if (!constant(filter)) {
        if (target != null) {
          for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
            if (schema.getLogicalColumn(i).getName().equalsIgnoreCase(target.getName())) indices.add(i);
          }
          if (indices.isEmpty()) {
            throw new IllegalArgumentException("Unknown bound column '" + target.getName() + "'");
          }
          if (indices.size() != 1) {
            throw new IllegalArgumentException("Ambiguous bound column '" + target.getName() + "'");
          }
          if (!sameValueType(target, schema.getLogicalColumn(indices.getFirst()))) {
            throw new IllegalArgumentException("Incompatible schema type for bound column '"
                + target.getName() + "'");
          }
        } else {
          for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
            if (filter.isApplicable(schema.getLogicalColumn(i))) indices.add(i);
          }
        }
      }
      bound.add(new BoundFilter(filter, List.copyOf(indices)));
    }
    cached = List.copyOf(bound);
    bindings.put(schema, cached);
    return cached;
  }

  private static boolean sameValueType(LogicalColumnDescriptor target, LogicalColumnDescriptor actual) {
    if (target.getLogicalType() != actual.getLogicalType()) return false;
    if (target.isPrimitive()) return target.getPhysicalType() == actual.getPhysicalType();
    if (target.isMap() && target.getMapMetadata() != null && actual.getMapMetadata() != null) {
      return target.getMapMetadata().keyType() == actual.getMapMetadata().keyType()
          && target.getMapMetadata().valueType() == actual.getMapMetadata().valueType();
    }
    if (target.isList() && target.getListMetadata() != null && actual.getListMetadata() != null) {
      return target.getListMetadata().elementType() == actual.getListMetadata().elementType();
    }
    return true;
  }

  @Override
  public Set<String> requiredColumns(SchemaDescriptor schema) {
    var required = new LinkedHashSet<String>();
    for (var bound : bind(schema)) {
      if (constant(bound.filter())) continue;
      if (bound.filter().targetColumn() == null) {
        for (var column : schema.logicalColumns()) required.add(column.getName());
      } else {
        for (int index : bound.indices()) required.add(schema.getLogicalColumn(index).getName());
      }
    }
    return Collections.unmodifiableSet(required);
  }

  @Override
  public boolean apply(RowColumnGroup row) {
    for (var bound : bind(row.getSchema())) {
      boolean matched = constant(bound.filter()) && bound.filter().apply(null);
      if (!constant(bound.filter())) {
        for (int index : bound.indices()) {
          if (bound.filter().apply(row.getColumnValue(index))) {
            matched = true;
            break;
          }
        }
      }
      if (type == FilterJoinType.All && !matched) return false;
      if (type == FilterJoinType.Any && matched) return true;
    }
    return type == FilterJoinType.All;
  }

  @Override
  public boolean canDrop(ParquetMetadata.RowGroupMetadata group, SchemaDescriptor schema) {
    for (var bound : bind(schema)) {
      boolean impossible = canDropLeaf(bound, group, schema);
      if (type == FilterJoinType.All && impossible) return true;
      if (type == FilterJoinType.Any && !impossible) return false;
    }
    return type == FilterJoinType.Any;
  }

  @Override
  public String expression() {
    String joiner = type == FilterJoinType.All ? " AND " : " OR ";
    StringBuilder text = new StringBuilder("(");
    for (int i = 0; i < filters.size(); i++) {
      if (i > 0) text.append(joiner);
      text.append(filters.get(i).expression());
    }
    return text.append(')').toString();
  }

  private static boolean canDropLeaf(BoundFilter bound, ParquetMetadata.RowGroupMetadata group,
                                     SchemaDescriptor schema) {
    if (constant(bound.filter())) return bound.filter().canDrop(null, -1);
    if (bound.filter().targetColumn() == null || bound.indices().size() != 1 || group == null
        || group.numRows() < 0 || group.columns() == null) return false;
    var logical = schema.getLogicalColumn(bound.indices().getFirst());
    if (!logical.isPrimitive()) return false;
    ColumnDescriptor physical = logical.getPhysicalDescriptor();
    if (physical == null || physical.maxRepetitionLevel() != 0) return false;
    // Resolve by exact path, never by logical index or by decoding bytes as a string.
    long schemaMatches = schema.columns().stream()
        .filter(c -> Arrays.equals(c.path(), physical.path()) && c.physicalType() == physical.physicalType())
        .count();
    if (schemaMatches != 1) return false;
    ParquetMetadata.ColumnChunkMetadata candidate = null;
    for (var chunk : group.columns()) {
      if (chunk == null) return false;
      if (Arrays.equals(chunk.path(), physical.path())) {
        if (candidate != null) return false;
        candidate = chunk;
      }
    }
    if (candidate == null || candidate.type() != physical.physicalType()
        || candidate.numValues() != group.numRows()) return false;
    return bound.filter().canDrop(candidate.statistics(), candidate.numValues());
  }

  private record BoundFilter(ColumnFilter filter, List<Integer> indices) { }
}
