package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ListMetadata;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;

/**
 * Real declared types/physical paths for predicate fixtures, rather than untyped descriptors.
 */
final class FilterTestSupport {
    private FilterTestSupport() {
    }

    static LogicalColumnDescriptor primitive(String name, Type type) {
        return new LogicalColumnDescriptor(name, LogicalType.PRIMITIVE, type,
                new ColumnDescriptor(type, new String[]{name}, 1, 0,
                        type == Type.FIXED_LEN_BYTE_ARRAY ? 4 : 0));
    }

    static LogicalColumnDescriptor map(Type valueType) {
        return SchemaDescriptor.createMapColumn("col", Type.BYTE_ARRAY, valueType, true, true);
    }

    static LogicalColumnDescriptor list(Type elementType) {
        var physical = new ColumnDescriptor(elementType, new String[]{"col", "list", "element"},
                3, 1, elementType == Type.FIXED_LEN_BYTE_ARRAY ? 4 : 0);
        return new LogicalColumnDescriptor("col", LogicalType.LIST, new ListMetadata(0, elementType, physical));
    }
}
