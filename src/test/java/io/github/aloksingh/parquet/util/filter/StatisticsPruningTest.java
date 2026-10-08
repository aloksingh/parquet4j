package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.*;
import io.github.aloksingh.parquet.model.ColumnStatistics.BoundsOrder;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class StatisticsPruningTest {
    private static LogicalColumnDescriptor primitive(String name, Type type) {
        return new LogicalColumnDescriptor(name, LogicalType.PRIMITIVE, type,
                new ColumnDescriptor(type, new String[]{name}, 1, 0, 0));
    }

    @Test
    void unsupportedLogicalComparisonsCannotUsePhysicalBounds() {
        for (var annotation : List.of(PrimitiveLogicalType.date(), PrimitiveLogicalType.decimal(6, 2),
                PrimitiveLogicalType.unknown())) {
            var column = new LogicalColumnDescriptor("value", LogicalType.PRIMITIVE, Type.INT32,
                    new ColumnDescriptor(Type.INT32, new String[]{"value"}, 1, 0, 0, annotation));
            var statistics = new ColumnStatistics(intBytes(10), intBytes(20), 0L, null,
                    BoundsOrder.TYPE_DEFINED, BoundsOrder.TYPE_DEFINED);
            assertFalse(new ColumnEqualFilter(column, 5).canDrop(statistics, 2), annotation.kind().toString());
        }
    }

    @Test
    void deprecatedSignedBoundsCannotPruneUnsignedIntegerColumns() {
        for (var type : List.of(Type.INT32, Type.INT64)) {
            var column = new LogicalColumnDescriptor("value", LogicalType.PRIMITIVE, type,
                    new ColumnDescriptor(type, new String[]{"value"}, 1, 0, 0,
                            PrimitiveLogicalType.integer(type == Type.INT32 ? 32 : 64, false)));
            Object minimum = type == Type.INT32 ? (Object) 10 : 10L;
            Object maximum = type == Type.INT32 ? (Object) 20 : 20L;
            var statistics = new ColumnStatistics(encode(minimum, type), encode(maximum, type), 0L, null);
            assertFalse(new ColumnEqualFilter(column, 5).canDrop(statistics, 2));
            assertTrue(new ColumnIsNullFilter(column).canDrop(statistics, 2), "counts are independent of bound order");
        }
    }

    @Test
    void unknownBoundOrderDoesNotDisableNullCountPruning() {
        var column = primitive("value", Type.INT32);
        var statistics = new ColumnStatistics(intBytes(10), intBytes(20), 0L, null,
                BoundsOrder.UNKNOWN, BoundsOrder.UNKNOWN);
        assertTrue(new ColumnIsNullFilter(column).canDrop(statistics, 2));
        assertFalse(new ColumnEqualFilter(column, 5).canDrop(statistics, 2));
    }

    @Test
    void finiteFloatingBoundsCannotProveTheAbsenceOfHiddenNaNs() {
        for (Type type : List.of(Type.FLOAT, Type.DOUBLE)) {
            var column = primitive("value", type);
            Object one = type == Type.FLOAT ? (Object) 1.0f : 1.0;
            Object two = type == Type.FLOAT ? (Object) 2.0f : 2.0;
            Object nan = type == Type.FLOAT ? (Object) Float.NaN : Double.NaN;
            var pair = List.of(two, nan);
            var equalBounds = actualStatistics(pair, type);
            var neq = new ColumnNotEqualFilter(column, two);
            assertEquals(1L, pair.stream().filter(neq::apply).count());
            assertFalse(neq.canDrop(equalBounds, pair.size()));
            var rows = List.of(one, two, nan);
            var finiteBounds = actualStatistics(rows, type);
            for (var operator : List.of(FilterOperator.lt, FilterOperator.lte, FilterOperator.gt, FilterOperator.gte)) {
                var filter = new ColumnFilters().createFilter(column, operator,
                        operator == FilterOperator.lt || operator == FilterOperator.lte ? "0" : "3");
                assertEquals(0L, rows.stream().filter(filter::apply).count(), "NaN is unordered under the residual policy");
                assertFalse(filter.canDrop(finiteBounds, rows.size()), "finite bounds provide no independent no-NaN proof");
            }
        }
    }

    @Test
    void compoundPruningUsesAnyImpossibleAndBranchButEveryOrBranch() {
        var column = primitive("value", Type.INT32);
        var statistics = new ColumnStatistics(intBytes(10), intBytes(20), 0L, null);
        var impossible = new ColumnEqualFilter(column, 5);
        var possible = new ColumnEqualFilter(column, 15);
        assertTrue(new ColumnFilterSet(column, FilterJoinType.All, impossible, possible)
                .canDrop(statistics, 4));
        assertFalse(new ColumnFilterSet(column, FilterJoinType.Any, impossible, possible)
                .canDrop(statistics, 4));
        assertTrue(new ColumnFilterSet(column, FilterJoinType.Any, impossible,
                new ColumnGreaterThanFilter(column, 20)).canDrop(statistics, 4));
        var emptyAnd = new ColumnFilterSet(null, FilterJoinType.All);
        var emptyOr = new ColumnFilterSet(null, FilterJoinType.Any);
        assertTrue(emptyAnd.apply(null));
        assertFalse(emptyAnd.canDrop(null, 4));
        assertFalse(emptyOr.apply(null));
        assertTrue(emptyOr.canDrop(null, 4));
        assertTrue(emptyOr.skip(null, "irrelevant"));
        assertSame(column, new ColumnFilterSet(column, FilterJoinType.All, possible).targetColumn());
        var children = new ArrayList<ColumnFilter>(List.of(impossible));
        var snapshot = new ColumnFilterSet(column, FilterJoinType.All, children);
        children.clear();
        assertTrue(snapshot.canDrop(statistics, 4), "constructor must snapshot mutable child lists");
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnFilterSet(column, FilterJoinType.All,
                        new ColumnEqualFilter(primitive("other", Type.INT32), 5)));
    }

    @Test
    void enumeratedActualRowsProveEveryDropIsSoundForSignedPrimitives() {
        Map<Type, List<Object>> domains = Map.of(
                Type.BOOLEAN, Arrays.asList(null, false, true),
                Type.INT32, Arrays.asList(null, Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE),
                Type.INT64, Arrays.asList(null, Long.MIN_VALUE, -1L, 0L, 1L,
                        9007199254740992L, 9007199254740993L, Long.MAX_VALUE),
                Type.FLOAT, Arrays.asList(null, Float.NEGATIVE_INFINITY, -1.0f, -0.0f, 0.0f,
                        1.0f, Math.nextUp(1.0f), Float.POSITIVE_INFINITY, Float.NaN),
                Type.DOUBLE, Arrays.asList(null, Double.NEGATIVE_INFINITY, -1.0, -0.0, 0.0,
                        1.0, Math.nextUp(1.0), Double.POSITIVE_INFINITY, Double.NaN));
        int checked = 0;
        int dropped = 0;
        for (var entry : domains.entrySet()) {
            var column = primitive("value", entry.getKey());
            var filters = new ArrayList<ColumnFilter>();
            for (Object constant : entry.getValue()) {
                for (var operator : List.of(FilterOperator.eq, FilterOperator.neq, FilterOperator.lt,
                        FilterOperator.lte, FilterOperator.gt, FilterOperator.gte)) {
                    if (constant == null && operator != FilterOperator.eq && operator != FilterOperator.neq) continue;
                    filters.add(new ColumnFilters().createFilter(column, operator, constant));
                }
            }
            filters.add(new ColumnIsNullFilter(column));
            filters.add(new ColumnIsNotNullFilter(column));
            for (Object first : entry.getValue()) {
                for (Object second : entry.getValue()) {
                    var rows = Arrays.asList(first, second);
                    var statistics = actualStatistics(rows, entry.getKey());
                    for (var filter : filters) {
                        if (filter.canDrop(statistics, rows.size())) {
                            assertEquals(0L, rows.stream().filter(filter::apply).count(),
                                    entry.getKey() + " rows " + rows + " statistics " + statistics);
                            dropped++;
                        }
                        checked++;
                    }
                }
            }
        }
        assertEquals(12736, checked, "all declared row/constant/operator combinations must execute");
        assertTrue(dropped > 0, "an always-keep implementation is not adequate");
    }

    @SuppressWarnings("unchecked")
    static ColumnStatistics actualStatistics(List<Object> rows, Type type) {
        Object min = null;
        Object max = null;
        long nulls = 0;
        for (Object value : rows) {
            if (value == null) {
                nulls++;
                continue;
            }
            if (value instanceof Float f && Float.isNaN(f)
                    || value instanceof Double d && Double.isNaN(d)) continue;
            if (min == null || ((Comparable<Object>) value).compareTo(min) < 0) min = value;
            if (max == null || ((Comparable<Object>) value).compareTo(max) > 0) max = value;
        }
        return new ColumnStatistics(encode(min, type), encode(max, type), nulls, null);
    }

    private static byte[] encode(Object value, Type type) {
        if (value == null) return null;
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        return switch (type) {
            case BOOLEAN -> new byte[]{(byte) ((Boolean) value ? 1 : 0)};
            case INT32 -> Arrays.copyOf(buffer.putInt((Integer) value).array(), 4);
            case INT64 -> buffer.putLong((Long) value).array();
            case FLOAT -> Arrays.copyOf(buffer.putFloat((Float) value).array(), 4);
            case DOUBLE -> buffer.putDouble((Double) value).array();
            default -> throw new IllegalArgumentException(type.toString());
        };
    }

    @Test
    void boundConstantsAndInclusiveBoundariesDetermineWhetherAChunkCanBeDropped() {
        var column = primitive("value", Type.INT32);
        var statistics = new ColumnStatistics(intBytes(10), intBytes(20), 1L, null);
        var cases = List.of(
                new Object[]{FilterOperator.eq, 5, true}, new Object[]{FilterOperator.eq, 10, false},
                new Object[]{FilterOperator.eq, 20, false}, new Object[]{FilterOperator.eq, 25, true},
                new Object[]{FilterOperator.neq, 15, false},
                new Object[]{FilterOperator.lt, 10, true}, new Object[]{FilterOperator.lt, 11, false},
                new Object[]{FilterOperator.lte, 9, true}, new Object[]{FilterOperator.lte, 10, false},
                new Object[]{FilterOperator.gt, 20, true}, new Object[]{FilterOperator.gt, 19, false},
                new Object[]{FilterOperator.gte, 21, true}, new Object[]{FilterOperator.gte, 20, false});
        for (var test : cases) {
            var filter = new ColumnFilters().createFilter(column, (FilterOperator) test[0], test[1]);
            assertSame(column, filter.targetColumn());
            assertEquals(test[2], filter.canDrop(statistics, 4), test[0] + " " + test[1]);
            assertEquals(test[2], filter.skip(statistics, -1000), "skip must ignore external values");
        }
        assertTrue(new ColumnNotEqualFilter(column, 15)
                .canDrop(new ColumnStatistics(intBytes(15), intBytes(15), 1L, null), 4));
        var noNulls = new ColumnStatistics(intBytes(10), intBytes(20), 0L, null);
        var allNulls = new ColumnStatistics(null, null, 4L, null);
        assertTrue(new ColumnIsNullFilter(column).canDrop(noNulls, 4));
        assertFalse(new ColumnIsNullFilter(column).canDrop(statistics, 4));
        assertFalse(new ColumnIsNullFilter(column).canDrop(allNulls, 4));
        assertTrue(new ColumnIsNotNullFilter(column).canDrop(allNulls, 4));
        assertTrue(new ColumnEqualFilter(column, 15).canDrop(allNulls, 4));
        assertTrue(new ColumnNotEqualFilter(column, 15).canDrop(allNulls, 4));
        assertTrue(new ColumnEqualFilter(column, null).canDrop(noNulls, 4));
        assertTrue(new ColumnNotEqualFilter(column, null).canDrop(allNulls, 4));
        assertFalse(new ColumnIsNotNullFilter(column).skip(allNulls, 4));
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    @Test
    void unknownMalformedNanAndUnsafeBinaryStatisticsAlwaysKeepTheChunk() {
        var column = primitive("value", Type.INT32);
        var unknown = List.of(new ColumnStatistics(null, null, null, null),
                new ColumnStatistics(new byte[]{1}, intBytes(20), 0L, null),
                new ColumnStatistics(intBytes(10), new byte[8], 0L, null),
                new ColumnStatistics(intBytes(20), intBytes(10), 0L, null),
                new ColumnStatistics(intBytes(10), null, 0L, null),
                new ColumnStatistics(intBytes(10), intBytes(20), -1L, null),
                new ColumnStatistics(intBytes(10), intBytes(20), 5L, null),
                new ColumnStatistics(intBytes(10), intBytes(20), 0L, -1L),
                new ColumnStatistics(intBytes(10), intBytes(20), 0L, 5L),
                new ColumnStatistics(intBytes(10), intBytes(20), 4L, null));
        for (var operator : List.of(FilterOperator.eq, FilterOperator.neq, FilterOperator.lt,
                FilterOperator.lte, FilterOperator.gt, FilterOperator.gte, FilterOperator.isNull, FilterOperator.isNotNull)) {
            var filter = new ColumnFilters().createFilter(column, operator,
                    operator == FilterOperator.isNull || operator == FilterOperator.isNotNull ? null : 100);
            assertFalse(filter.canDrop(null, 4));
            for (var statistics : unknown) assertFalse(filter.canDrop(statistics, 4), operator.toString());
        }
        var bool = primitive("value", Type.BOOLEAN);
        assertFalse(new ColumnEqualFilter(bool, false)
                .canDrop(new ColumnStatistics(new byte[]{2}, new byte[]{3}, 0L, null), 4));
        for (Type type : List.of(Type.BYTE_ARRAY, Type.FIXED_LEN_BYTE_ARRAY)) {
            var binary = new LogicalColumnDescriptor("value", LogicalType.PRIMITIVE, type,
                    new ColumnDescriptor(type, new String[]{"value"}, 1, 0, type == Type.FIXED_LEN_BYTE_ARRAY ? 1 : 0));
            var filter = new ColumnEqualFilter(binary, new byte[]{3});
            assertFalse(filter.canDrop(new ColumnStatistics(new byte[]{1}, new byte[]{2}, 0L, null), 4));
            assertFalse(new ColumnIsNullFilter(binary)
                    .canDrop(new ColumnStatistics(new byte[]{1}, new byte[]{2}, 0L, null), 4));
        }
        var map = SchemaDescriptor.createMapColumn("map", Type.BYTE_ARRAY, Type.INT32, true, true);
        assertFalse(new ColumnIsNullFilter(map, Optional.of("missing"))
                .canDrop(new ColumnStatistics(intBytes(10), intBytes(20), 0L, null), 4));
        assertFalse(new ColumnEqualFilter(map, 100, Optional.of("key"))
                .canDrop(new ColumnStatistics(intBytes(10), intBytes(20), 0L, null), 4));
        var floating = primitive("value", Type.DOUBLE);
        var nanBytes = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(Double.NaN).array();
        assertFalse(new ColumnEqualFilter(floating, 100.0)
                .canDrop(new ColumnStatistics(nanBytes, nanBytes, 0L, null), 4));
        var fifteen = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(15.0).array();
        assertFalse(new ColumnNotEqualFilter(floating, 15.0)
                .canDrop(new ColumnStatistics(fifteen, fifteen, 0L, null), 4), "finite stats may omit NaNs");
        assertFalse(new ColumnEqualFilter(floating, Double.NaN)
                .canDrop(new ColumnStatistics(fifteen, fifteen, 0L, null), 4));
    }

    @Test
    void customPredicatesHaveConservativeSourceCompatibleDefaultContracts() throws Exception {
        var target = ColumnFilter.class.getMethod("targetColumn");
        var columnDrop = ColumnFilter.class.getMethod("canDrop", ColumnStatistics.class, long.class);
        assertTrue(target.isDefault());
        assertTrue(columnDrop.isDefault());
        ColumnFilter opaqueColumn = new ColumnFilter() {
            @Override
            public boolean apply(Object value) {
                return true;
            }

            @Override
            public boolean isApplicable(LogicalColumnDescriptor column) {
                return true;
            }

            @Override
            public boolean skip(ColumnStatistics statistics, Object value) {
                return true;
            }
        };
        assertNull(target.invoke(opaqueColumn));
        assertFalse((Boolean) columnDrop.invoke(opaqueColumn, null, 10L));
        var schema = SchemaDescriptor.fromLogicalColumns("schema",
                List.of(primitive("a", Type.INT32), primitive("b", Type.INT64)));
        RowColumnGroupFilter opaqueRow = row -> true;
        var required = RowColumnGroupFilter.class.getMethod("requiredColumns", SchemaDescriptor.class);
        var rowDrop = RowColumnGroupFilter.class.getMethod("canDrop",
                ParquetMetadata.RowGroupMetadata.class, SchemaDescriptor.class);
        assertTrue(required.isDefault());
        assertTrue(rowDrop.isDefault());
        assertEquals(Set.of("a", "b"), required.invoke(opaqueRow, schema));
        assertFalse((Boolean) rowDrop.invoke(opaqueRow, null, schema));
    }
}
