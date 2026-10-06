package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class ColumnFilterBindingTest {
    private static LogicalColumnDescriptor primitive(Type type) {
        return new LogicalColumnDescriptor("value", LogicalType.PRIMITIVE, type,
                new ColumnDescriptor(type, new String[]{"value"}, 1, 0, 0));
    }

    @Test
    void unknownAndCaseAmbiguousColumnNamesFailDescriptivelyDuringBinding() {
        var column = primitive(Type.INT32);
        var schema = SchemaDescriptor.fromLogicalColumns("schema", List.of(column));
        var factory = new ColumnFilters();
        var unknown = assertThrows(IllegalArgumentException.class,
                () -> factory.createFilter(schema,
                        new ColumnFilterDescriptor("missing", LogicalType.PRIMITIVE, FilterOperator.eq, 1)));
        assertTrue(unknown.getMessage().contains("missing"));
        assertTrue(unknown.getMessage().toLowerCase().contains("unknown"));
        var other = new LogicalColumnDescriptor("VALUE", LogicalType.PRIMITIVE, Type.INT32,
                new ColumnDescriptor(Type.INT32, new String[]{"VALUE"}, 1, 0, 0));
        var ambiguousSchema = SchemaDescriptor.fromLogicalColumns("schema", List.of(column, other));
        var ambiguous = assertThrows(IllegalArgumentException.class,
                () -> factory.createFilter(ambiguousSchema,
                        new ColumnFilterDescriptor("value", LogicalType.PRIMITIVE, FilterOperator.eq, 1)));
        assertTrue(ambiguous.getMessage().toLowerCase().contains("ambiguous"));
        assertTrue(ambiguous.getMessage().contains("value"));
        assertTrue(factory.createFilter(schema,
                new ColumnFilterDescriptor("VALUE", LogicalType.PRIMITIVE, FilterOperator.eq, "12")).apply(12));
    }

    @Test
    void floatingComparisonsAreExactWithIeeeNaNsAndNumericallyEqualSignedZeros() {
        for (Type type : List.of(Type.FLOAT, Type.DOUBLE)) {
            var column = primitive(type);
            Object nan = type == Type.FLOAT ? (Object) Float.NaN : Double.NaN;
            Object negativeZero = type == Type.FLOAT ? (Object) (-0.0f) : -0.0;
            Object zero = type == Type.FLOAT ? (Object) 0.0f : 0.0;
            Object one = type == Type.FLOAT ? (Object) 1.0f : 1.0;
            Object next = type == Type.FLOAT ? (Object) Math.nextUp(1.0f) : Math.nextUp(1.0);
            var rows = List.of(nan, negativeZero, zero, one, next);
            var expected = Map.of(FilterOperator.eq, List.of(1, 2), FilterOperator.neq, List.of(0, 3, 4),
                    FilterOperator.lt, List.of(), FilterOperator.lte, List.of(1, 2),
                    FilterOperator.gt, List.of(3, 4), FilterOperator.gte, List.of(1, 2, 3, 4));
            for (var operator : List.of(FilterOperator.eq, FilterOperator.neq, FilterOperator.lt,
                    FilterOperator.lte, FilterOperator.gt, FilterOperator.gte)) {
                var filter = new ColumnFilters().createFilter(column, operator, zero);
                assertEquals(expected.get(operator),
                        IntStream.range(0, rows.size()).filter(i -> filter.apply(rows.get(i))).boxed().toList(),
                        type + " " + operator + " signed zero");
                var nanFilter = new ColumnFilters().createFilter(column, operator, nan);
                assertEquals(operator == FilterOperator.neq ? 5L : 0L,
                        rows.stream().filter(nanFilter::apply).count(), type + " " + operator + " NaN");
            }
            assertTrue(new ColumnEqualFilter(column, one).apply(one));
            assertFalse(new ColumnEqualFilter(column, one).apply(next));
            assertTrue(new ColumnLessThanFilter(column, (Comparable) next).apply(one));
            assertEquals(nan, ColumnFilterHelper.CFH.convertToColumnType(column, "NaN"));
            assertThrows(IllegalArgumentException.class, () -> new ColumnEqualFilter(column, "1e1000"));
            assertThrows(IllegalArgumentException.class,
                    () -> new ColumnEqualFilter(column, new BigDecimal("1e1000")));
        }
        assertTrue(new ColumnEqualFilter(primitive(Type.DOUBLE), Double.POSITIVE_INFINITY)
                .apply(Double.POSITIVE_INFINITY));
    }

    @Test
    void binaryPredicatesUseContentUnsignedOrderingAndSnapshotTheirConstant() {
        var expected = Map.of(FilterOperator.eq, List.of(1), FilterOperator.neq, List.of(0, 2),
                FilterOperator.lt, List.of(0), FilterOperator.lte, List.of(0, 1),
                FilterOperator.gt, List.of(2), FilterOperator.gte, List.of(1, 2));
        var rows = List.of(new byte[]{0, 0x7f}, new byte[]{0, (byte) 0xff},
                new byte[]{(byte) 0x80, 0});
        for (Type type : List.of(Type.BYTE_ARRAY, Type.FIXED_LEN_BYTE_ARRAY)) {
            var column = new LogicalColumnDescriptor("value", LogicalType.PRIMITIVE, type,
                    new ColumnDescriptor(type, new String[]{"value"}, 1, 0, type == Type.FIXED_LEN_BYTE_ARRAY ? 2 : 0));
            for (var operator : expected.keySet()) {
                byte[] constant = {0, (byte) 0xff};
                var filter = new ColumnFilters().createFilter(column, operator, constant);
                constant[0] = 42;
                assertEquals(expected.get(operator),
                        IntStream.range(0, rows.size()).filter(i -> filter.apply(rows.get(i))).boxed().toList(),
                        type + " " + operator);
            }
            if (type == Type.FIXED_LEN_BYTE_ARRAY) {
                assertThrows(IllegalArgumentException.class, () -> new ColumnEqualFilter(column, new byte[]{1}));
                var filter = new ColumnEqualFilter(column, new byte[]{1, 0});
                assertThrows(IllegalArgumentException.class, () -> filter.apply(new byte[]{1}));
            }
        }
    }

    @Test
    void wrongRuntimeTypesAreErrorsRatherThanNonMatchingRows() {
        var column = primitive(Type.INT32);
        for (var operator : List.of(FilterOperator.eq, FilterOperator.neq, FilterOperator.lt,
                FilterOperator.lte, FilterOperator.gt, FilterOperator.gte)) {
            var filter = new ColumnFilters().createFilter(column, operator, 12);
            assertThrows(IllegalArgumentException.class, () -> filter.apply("wrong"), operator.toString());
            assertThrows(IllegalArgumentException.class, () -> filter.apply(12L), operator.toString());
        }
        var map = SchemaDescriptor.createMapColumn("value", Type.BYTE_ARRAY, Type.INT64, true, true);
        var filter = new ColumnLessThanFilter(map, 12L, Optional.of("key"));
        assertThrows(IllegalArgumentException.class, () -> filter.apply(Map.of("key", "wrong")));
        assertThrows(IllegalArgumentException.class, () -> filter.apply(12L));
    }

    @Test
    void unsupportedTypesAndOperatorsFailAtConstruction() {
        var factory = new ColumnFilters();
        for (Type type : List.of(Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE, Type.BOOLEAN,
                Type.BYTE_ARRAY, Type.FIXED_LEN_BYTE_ARRAY, Type.INT96)) {
            var column = primitive(type);
            assertThrows(IllegalArgumentException.class,
                    () -> factory.createFilter(column, FilterOperator.eq, new Object()), type.toString());
        }
        for (var operator : List.of(FilterOperator.contains, FilterOperator.prefix, FilterOperator.suffix)) {
            assertThrows(IllegalArgumentException.class,
                    () -> factory.createFilter(primitive(Type.INT32), operator, "1"), operator.toString());
        }
        for (var operator : List.of(FilterOperator.lt, FilterOperator.lte, FilterOperator.gt, FilterOperator.gte)) {
            assertThrows(IllegalArgumentException.class,
                    () -> factory.createFilter(primitive(Type.INT32), operator, null));
            assertThrows(IllegalArgumentException.class,
                    () -> factory.createFilter(SchemaDescriptor.createStringMapColumn("value", true), operator, 1));
        }
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnPrefixFilter(primitive(Type.INT32), "1"));
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnEqualFilter(primitive(Type.BYTE_ARRAY), 1));
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnEqualFilter(primitive(Type.INT32), 1, Optional.of("key")));
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnIsNullFilter(primitive(Type.INT32), Optional.of("key")));
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnEqualFilter(new LogicalColumnDescriptor("untyped", LogicalType.PRIMITIVE, (Type) null, null), 1));
        assertThrows(IllegalArgumentException.class,
                () -> factory.createFilter(primitive(Type.INT32), null, 1));
    }

    @Test
    void keyedNullMapsMissingKeysAndPresentNullHaveTheSameNullSemantics() {
        var column = SchemaDescriptor.createMapColumn("value", Type.BYTE_ARRAY, Type.INT32, true, true);
        Map<String, Integer> presentNull = new HashMap<>();
        presentNull.put("key", null);
        var rows = Arrays.asList(null, Map.of(), presentNull, Map.of("key", 0),
                Map.of("key", 12), Map.of("key", 20));
        var expected = Map.of(FilterOperator.eq, List.of(4), FilterOperator.neq, List.of(3, 5),
                FilterOperator.lt, List.of(3), FilterOperator.lte, List.of(3, 4),
                FilterOperator.gt, List.of(5), FilterOperator.gte, List.of(4, 5),
                FilterOperator.isNull, List.of(0, 1, 2), FilterOperator.isNotNull, List.of(3, 4, 5));
        for (var operator : FilterOperator.values()) {
            if (!expected.containsKey(operator)) continue;
            var filter = new ColumnFilters().createFilter(column, operator,
                    operator == FilterOperator.isNull || operator == FilterOperator.isNotNull ? null : 12,
                    Optional.of("key"));
            assertEquals(expected.get(operator),
                    IntStream.range(0, rows.size()).filter(i -> filter.apply(rows.get(i))).boxed().toList(),
                    operator.toString());
        }
        var eqNull = new ColumnEqualFilter(column, null, Optional.of("key"));
        var neqNull = new ColumnNotEqualFilter(column, null, Optional.of("key"));
        assertEquals(List.of(0, 1, 2),
                IntStream.range(0, rows.size()).filter(i -> eqNull.apply(rows.get(i))).boxed().toList());
        assertEquals(List.of(3, 4, 5),
                IntStream.range(0, rows.size()).filter(i -> neqNull.apply(rows.get(i))).boxed().toList());
        assertTrue(new ColumnEqualFilter(primitive(Type.INT32), null).apply(null));
        assertFalse(new ColumnNotEqualFilter(primitive(Type.INT32), 12).apply(null));
    }

    @Test
    void keyedMapConstantsBindOnceToTheDeclaredValueType() {
        var column = SchemaDescriptor.createMapColumn("value", Type.BYTE_ARRAY, Type.INT64, true, true);
        var rows = List.of(Map.of("key", 11L), Map.of("key", 12L), Map.of("key", 13L));
        var expectedCounts = Map.of(FilterOperator.eq, 1L, FilterOperator.neq, 2L,
                FilterOperator.lt, 1L, FilterOperator.lte, 2L, FilterOperator.gt, 1L, FilterOperator.gte, 2L);
        for (var operator : expectedCounts.keySet()) {
            var constant = new CountingNumber();
            var filter = new ColumnFilters().createFilter(column, operator, constant, Optional.of("key"));
            assertEquals(1, constant.conversions, operator + " must bind during construction");
            assertEquals(expectedCounts.get(operator), rows.stream().filter(filter::apply).count());
            assertEquals(1, constant.conversions, operator + " must not convert per row");
        }
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnEqualFilter(column, "1.5", Optional.of("key")));
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnGreaterThanFilter(column, "9223372036854775808", Optional.of("key")));
    }

    private static final class CountingNumber extends Number implements Comparable<CountingNumber> {
        int conversions;

        @Override
        public String toString() {
            conversions++;
            return "12";
        }

        @Override
        public int intValue() {
            return 12;
        }

        @Override
        public long longValue() {
            return 12;
        }

        @Override
        public float floatValue() {
            return 12;
        }

        @Override
        public double doubleValue() {
            return 12;
        }

        @Override
        public int compareTo(CountingNumber other) {
            return 0;
        }
    }

    @Test
    void booleanConstantsAcceptOnlyTrueOrFalse() {
        var column = primitive(Type.BOOLEAN);
        for (Object invalid : new Object[]{"garbage", "yes", "0", "", 1, new Object()}) {
            assertThrows(IllegalArgumentException.class,
                    () -> ColumnFilterHelper.CFH.convertToColumnType(column, invalid));
            assertThrows(IllegalArgumentException.class,
                    () -> ColumnFilterHelper.CFH.convertToClassType(Boolean.class, invalid));
            assertThrows(IllegalArgumentException.class, () -> new ColumnEqualFilter(column, invalid));
        }
        assertEquals(true, ColumnFilterHelper.CFH.convertToColumnType(column, "TRUE"));
        assertEquals(false, ColumnFilterHelper.CFH.convertToColumnType(column, "false"));
        assertEquals(true, ColumnFilterHelper.CFH.convertToColumnType(column, true));
    }

    @Test
    void integralConstantsRejectFractionsAndOverflowWithoutTruncation() {
        var helper = ColumnFilterHelper.CFH;
        var intColumn = primitive(Type.INT32);
        var longColumn = primitive(Type.INT64);
        for (Object invalid : new Object[]{"1.5", new BigDecimal("-2.1"), 1.25,
                "2147483648", "-2147483649", Long.MAX_VALUE, new Object()}) {
            assertThrows(IllegalArgumentException.class,
                    () -> helper.convertToColumnType(intColumn, invalid), String.valueOf(invalid));
            assertThrows(IllegalArgumentException.class,
                    () -> helper.convertToClassType(Integer.class, invalid), String.valueOf(invalid));
        }
        for (Object invalid : new Object[]{"1.5", new BigDecimal("-2.1"), Double.NaN,
                "9223372036854775808", "-9223372036854775809", new BigInteger("18446744073709551615")}) {
            assertThrows(IllegalArgumentException.class,
                    () -> helper.convertToColumnType(longColumn, invalid), String.valueOf(invalid));
            assertThrows(IllegalArgumentException.class,
                    () -> helper.convertToClassType(Long.class, invalid), String.valueOf(invalid));
        }
        assertEquals(Integer.MAX_VALUE, helper.convertToColumnType(intColumn, "2147483647"));
        assertEquals(Integer.MIN_VALUE, helper.convertToColumnType(intColumn, "-2147483648"));
        assertEquals(Long.MAX_VALUE, helper.convertToColumnType(longColumn, "9223372036854775807"));
        assertEquals(Long.MIN_VALUE, helper.convertToColumnType(longColumn, "-9223372036854775808"));
        assertEquals(9007199254740993L, helper.convertToColumnType(longColumn, 9007199254740993L));
        assertEquals(1, helper.convertToColumnType(intColumn, "1.000"));
    }
}
