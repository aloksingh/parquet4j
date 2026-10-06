package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class RowColumnGroupFilterSetTest {
    private static LogicalColumnDescriptor primitive(String name, Type type) {
        return new LogicalColumnDescriptor(name, LogicalType.PRIMITIVE, type,
                new ColumnDescriptor(type, new String[]{name}, 1, 0, 0));
    }

    private static ParquetMetadata.ColumnChunkMetadata chunk(LogicalColumnDescriptor column,
                                                             List<Object> values) {
        return new ParquetMetadata.ColumnChunkMetadata(column.getPhysicalType(),
                column.getPhysicalDescriptor().path(), CompressionCodec.UNCOMPRESSED, 4, 0, 0, 0,
                values.size(), StatisticsPruningTest.actualStatistics(values, column.getPhysicalType()));
    }

    @Test
    void enumeratedTwoColumnRowsProveCompoundPruningNeverLosesAMatchingRow() {
        var a = primitive("a", Type.INT32);
        var b = primitive("b", Type.INT32);
        var schema = SchemaDescriptor.fromLogicalColumns("schema", List.of(a, b));
        var combinations = new ArrayList<RowColumnGroupFilterSet>();
        for (var first : propertyFilters(a)) {
            for (var second : propertyFilters(b)) {
                combinations.add(new RowColumnGroupFilterSet(FilterJoinType.All, first, second));
                combinations.add(new RowColumnGroupFilterSet(FilterJoinType.Any, first, second));
            }
        }
        var domain = Arrays.asList(null, -1, 0, 1);
        int checked = 0;
        int dropped = 0;
        for (Object a1 : domain)
            for (Object a2 : domain) {
                for (Object b1 : domain)
                    for (Object b2 : domain) {
                        var rows = List.of(new SimpleRowColumnGroup(schema, new Object[]{a1, b1}),
                                new SimpleRowColumnGroup(schema, new Object[]{a2, b2}));
                        var group = new ParquetMetadata.RowGroupMetadata(List.of(
                                chunk(a, Arrays.asList(a1, a2)), chunk(b, Arrays.asList(b1, b2))), 0, 2);
                        for (var filter : combinations) {
                            if (filter.canDrop(group, schema)) {
                                assertEquals(0L, rows.stream().filter(filter::apply).count());
                                dropped++;
                            }
                            checked++;
                        }
                    }
            }
        assertEquals(100352, checked, "every declared row/operator/join combination must execute");
        assertTrue(dropped > 0);
    }

    private static List<ColumnFilter> propertyFilters(LogicalColumnDescriptor column) {
        return List.of(new ColumnEqualFilter(column, -1), new ColumnEqualFilter(column, 0),
                new ColumnEqualFilter(column, 1), new ColumnNotEqualFilter(column, 0),
                new ColumnLessThanFilter(column, 0), new ColumnLessThanOrEqualFilter(column, 0),
                new ColumnGreaterThanFilter(column, 0), new ColumnGreaterThanOrEqualFilter(column, 0),
                new ColumnIsNullFilter(column), new ColumnIsNotNullFilter(column),
                new ColumnFilterSet(column, FilterJoinType.All,
                        new ColumnGreaterThanFilter(column, -1), new ColumnLessThanFilter(column, 1)),
                new ColumnFilterSet(column, FilterJoinType.Any,
                        new ColumnEqualFilter(column, -1), new ColumnEqualFilter(column, 1)),
                new ColumnFilterSet(null, FilterJoinType.All), new ColumnFilterSet(null, FilterJoinType.Any));
    }

    @Test
    void boundPredicatesRequireOnlyTheirTargetAndNeverReadOtherColumns() {
        var a = primitive("a", Type.INT32);
        var b = primitive("b", Type.INT32);
        var schema = SchemaDescriptor.fromLogicalColumns("schema", List.of(b, a));
        var children = new ArrayList<ColumnFilter>(List.of(new ColumnEqualFilter(a, 12)));
        var filter = new RowColumnGroupFilterSet(FilterJoinType.All, children);
        children.clear();
        assertEquals(Set.of("a"), filter.requiredColumns(schema));
        assertThrows(UnsupportedOperationException.class, () -> filter.requiredColumns(schema).clear());
        var row = new SimpleRowColumnGroup(schema, new Object[]{99, 12});
        RowColumnGroup onlyTargetReadable = new RowColumnGroup() {
            @Override
            public SchemaDescriptor getSchema() {
                return row.getSchema();
            }

            @Override
            public List<LogicalColumnDescriptor> getColumns() {
                return row.getColumns();
            }

            @Override
            public List<ColumnDescriptor> getPhysicalColumns() {
                return row.getPhysicalColumns();
            }

            @Override
            public Object getColumnValue(int index) {
                assertEquals(1, index, "unneeded columns must not be touched");
                return row.getColumnValue(index);
            }

            @Override
            public <T> T getColumnValue(ColumnDescriptor column, Class<T> type) {
                return row.getColumnValue(column, type);
            }

            @Override
            public Object getColumnValue(String name) {
                return row.getColumnValue(name);
            }

            @Override
            public int getColumnCount() {
                return row.getColumnCount();
            }
        };
        assertTrue(filter.apply(onlyTargetReadable));
        assertTrue(filter.apply(onlyTargetReadable));
        var copy = SchemaDescriptor.fromLogicalColumns("copy", List.of(primitive("a", Type.INT32)));
        assertEquals(Set.of("a"), filter.requiredColumns(copy));
        assertTrue(filter.apply(new SimpleRowColumnGroup(copy, new Object[]{12})));
    }

    @Test
    void opaquePredicatesKeepAllColumnsAndCacheImmutableApplicability() {
        var schema = SchemaDescriptor.fromLogicalColumns("schema",
                List.of(primitive("a", Type.INT32), primitive("b", Type.INT32)));
        AtomicInteger applicabilityChecks = new AtomicInteger();
        ColumnFilter opaque = new ColumnFilter() {
            @Override
            public boolean apply(Object value) {
                return value instanceof Integer n && n > 0;
            }

            @Override
            public boolean isApplicable(LogicalColumnDescriptor column) {
                applicabilityChecks.incrementAndGet();
                return true;
            }
        };
        var filter = new RowColumnGroupFilterSet(FilterJoinType.All, opaque);
        assertEquals(Set.of("a", "b"), filter.requiredColumns(schema));
        assertTrue(filter.apply(new SimpleRowColumnGroup(schema, new Object[]{1, 2})),
                "one predicate matching multiple columns is still one AND branch");
        assertTrue(filter.apply(new SimpleRowColumnGroup(schema, new Object[]{1, -1})));
        assertFalse(filter.apply(new SimpleRowColumnGroup(schema, new Object[]{-1, -2})));
        assertEquals(2, applicabilityChecks.get(), "applicability must bind once per schema");
        var group = new ParquetMetadata.RowGroupMetadata(List.of(
                chunk(schema.getLogicalColumn(0), List.of(1, 2)),
                chunk(schema.getLogicalColumn(1), List.of(1, 2))), 0, 2);
        assertFalse(filter.canDrop(group, schema));
    }

    @Test
    void rowGroupPruningUsesEachLogicalLeafsOwnPhysicalChunkAndJoinIdentity() {
        var map = SchemaDescriptor.createMapColumn("map", Type.BYTE_ARRAY, Type.INT32, true, true);
        var a = primitive("a", Type.INT32);
        var b = primitive("b", Type.INT32);
        var schema = SchemaDescriptor.fromLogicalColumns("schema", List.of(map, a, b));
        var aChunk = chunk(a, Arrays.asList(10, 15, 20, null));
        var bChunk = chunk(b, Arrays.asList(30, 35, 40, null));
        // Chunk positions deliberately differ from logical indices and physical schema positions.
        var group = new ParquetMetadata.RowGroupMetadata(List.of(bChunk, aChunk), 0, 4);
        var impossible = new ColumnEqualFilter(a, 5);
        var possible = new ColumnEqualFilter(b, 35);
        assertTrue(new RowColumnGroupFilterSet(FilterJoinType.All, impossible, possible).canDrop(group, schema));
        assertFalse(new RowColumnGroupFilterSet(FilterJoinType.Any, impossible, possible).canDrop(group, schema));
        assertTrue(new RowColumnGroupFilterSet(FilterJoinType.Any, impossible,
                new ColumnGreaterThanFilter(b, 40)).canDrop(group, schema));
        assertFalse(new RowColumnGroupFilterSet(FilterJoinType.All).canDrop(group, schema));
        assertTrue(new RowColumnGroupFilterSet(FilterJoinType.Any).canDrop(group, schema));
        assertTrue(new RowColumnGroupFilterSet(FilterJoinType.All)
                .apply(new SimpleRowColumnGroup(schema, new Object[]{null, 15, 35})));
        assertFalse(new RowColumnGroupFilterSet(FilterJoinType.Any)
                .apply(new SimpleRowColumnGroup(schema, new Object[]{null, 15, 35})));
        assertEquals(Set.of(), new RowColumnGroupFilterSet(FilterJoinType.All,
                new ColumnFilterSet(null, FilterJoinType.All)).requiredColumns(schema));
        assertTrue(new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnFilterSet(null, FilterJoinType.All))
                .apply(new SimpleRowColumnGroup(schema, new Object[]{null, 15, 35})));
        assertTrue(new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnFilterSet(null, FilterJoinType.Any))
                .canDrop(group, schema));
        var keyed = new ColumnIsNullFilter(schema.getLogicalColumn(0), Optional.of("absent"));
        assertFalse(new RowColumnGroupFilterSet(FilterJoinType.All, keyed).canDrop(group, schema));
        var missing = new ParquetMetadata.RowGroupMetadata(List.of(bChunk), 0, 4);
        assertFalse(new RowColumnGroupFilterSet(FilterJoinType.Any, impossible,
                new ColumnGreaterThanFilter(b, 40)).canDrop(missing, schema));
        assertTrue(new RowColumnGroupFilterSet(FilterJoinType.All, impossible,
                new ColumnGreaterThanFilter(b, 40)).canDrop(missing, schema));
        assertFalse(new RowColumnGroupFilterSet(FilterJoinType.All, impossible)
                .canDrop(new ParquetMetadata.RowGroupMetadata(List.of(aChunk, aChunk), 0, 4), schema));
        var wrongCount = new ParquetMetadata.ColumnChunkMetadata(Type.INT32, new String[]{"a"},
                CompressionCodec.UNCOMPRESSED, 4, 0, 0, 0, 3, aChunk.statistics());
        assertFalse(new RowColumnGroupFilterSet(FilterJoinType.All, impossible)
                .canDrop(new ParquetMetadata.RowGroupMetadata(List.of(wrongCount), 0, 4), schema));
    }

    @Test
    void missingAmbiguousAndIncompatibleBoundTargetsFailBeforeReadingRows() {
        var schema = SchemaDescriptor.fromLogicalColumns("schema", List.of(primitive("a", Type.INT32)));
        var missing = new RowColumnGroupFilterSet(FilterJoinType.All,
                new ColumnEqualFilter(primitive("missing", Type.INT32), 1));
        var error = assertThrows(IllegalArgumentException.class, () -> missing.requiredColumns(schema));
        assertTrue(error.getMessage().contains("missing"));
        var ambiguous = SchemaDescriptor.fromLogicalColumns("schema",
                List.of(primitive("a", Type.INT32), primitive("A", Type.INT32)));
        var bound = new RowColumnGroupFilterSet(FilterJoinType.All,
                new ColumnEqualFilter(schema.getLogicalColumn(0), 1));
        assertThrows(IllegalArgumentException.class, () -> bound.requiredColumns(ambiguous));
        assertThrows(IllegalArgumentException.class,
                () -> new RowColumnGroupFilterSet(FilterJoinType.All,
                        new ColumnEqualFilter(primitive("a", Type.INT64), 1L)).requiredColumns(schema));
    }
}
