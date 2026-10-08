package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.bloom.SplitBlockBloomFilter;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@FunctionalInterface
public interface RowColumnGroupFilter {
  boolean apply(RowColumnGroup row);

  /** Opaque predicates may inspect any logical column, so the default requires all of them. */
  default Set<String> requiredColumns(SchemaDescriptor schema) {
    return schema.logicalColumns().stream().map(c -> c.getName()).collect(Collectors.toUnmodifiableSet());
  }

  /** Human-readable predicate text for error context; defaults to the class identity. */
  default String expression() {
    return toString();
  }

  /** True only if metadata proves no row in the group can match; opaque filters keep the group. */
  default boolean canDrop(ParquetMetadata.RowGroupMetadata group, SchemaDescriptor schema) {
    return false;
  }

  /**
   * Bloom-filter-aware pruning: the {@code bloomLookup} function returns a bloom filter
   * for a physical column path string, or null if unavailable. The default delegates
   * to {@link #canDrop(ParquetMetadata.RowGroupMetadata, SchemaDescriptor)}.
   */
  default boolean canDrop(ParquetMetadata.RowGroupMetadata group, SchemaDescriptor schema,
                          Function<String, SplitBlockBloomFilter> bloomLookup) {
    return canDrop(group, schema);
  }
}
