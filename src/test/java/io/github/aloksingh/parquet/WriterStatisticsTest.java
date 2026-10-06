package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.parquet.format.Statistics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class WriterStatisticsTest {
    @TempDir
    Path directory;

    private Statistics statistics(Type type, Object... values) throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(type, 1, 0);
        Path destination = directory.resolve(type + "-stats.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema)) {
            for (Object value : values) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
        }
        return WriterTestSupport.footer(destination).getRow_groups().getFirst().getColumns().getFirst()
                .getMeta_data().getStatistics();
    }

    @Test
    void booleanBoundsUseFalseBeforeTrue() throws Exception {
        Statistics stats = statistics(Type.BOOLEAN, true, false, true);
        assertArrayEquals(new byte[]{0}, stats.getMin_value());
        assertArrayEquals(new byte[]{1}, stats.getMax_value());
    }

    @Test
    void allNullPrimitiveRetainsExactNullAndDistinctCountsWithoutBounds() throws Exception {
        Statistics stats = statistics(Type.INT32, null, null, null);
        assertNotNull(stats);
        assertEquals(3, stats.getNull_count());
        assertEquals(0, stats.getDistinct_count());
        assertFalse(stats.isSetMin_value());
        assertFalse(stats.isSetMax_value());
    }

    @Test
    void binaryDistinctCountUsesEncodedContentEquality() throws Exception {
        Statistics stats = statistics(Type.BYTE_ARRAY, new byte[]{1, 2}, new byte[]{1, 2},
                ByteBuffer.wrap(new byte[]{0, 1, 2}).position(1), new byte[]{3});
        assertEquals(2, stats.getDistinct_count());
        assertArrayEquals(new byte[]{1, 2}, stats.getMin_value());
        assertArrayEquals(new byte[]{3}, stats.getMax_value());
    }

    @Test
    void integralDistinctCountUsesPhysicalValuesRatherThanJavaWrapperIdentity() throws Exception {
        Statistics stats = statistics(Type.INT32, 1, 1L, (short) 1, (byte) 1);
        assertEquals(1, stats.getDistinct_count());
    }

    @Test
    void footerDeclaresTypeOrdersAndOnlySignedCompatibleDeprecatedBounds() throws Exception {
        List<ColumnDescriptor> physical = List.of(
                new ColumnDescriptor(Type.INT32, new String[]{"number"}, 0, 0, 0),
                new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"binary"}, 0, 0, 0));
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("orders",
                SchemaDescriptor.createLogicalColumnsFromPhysical(physical));
        Path destination = directory.resolve("column-orders.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{-1, new byte[]{(byte) 0x80}}));
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{1, new byte[]{0x7f}}));
        }
        var footer = WriterTestSupport.footer(destination);
        var columns = footer.getRow_groups().getFirst().getColumns();
        var numeric = columns.get(0).getMeta_data().getStatistics();
        var binary = columns.get(1).getMeta_data().getStatistics();
        assertAll(
                () -> assertTrue(footer.isSetColumn_orders()),
                () -> assertFalse(binary.isSetMin(), "Unsigned binary bounds must not use deprecated signed fields"),
                () -> assertFalse(binary.isSetMax()),
                () -> assertArrayEquals(new byte[]{0x7f}, binary.getMin_value()),
                () -> assertArrayEquals(new byte[]{(byte) 0x80}, binary.getMax_value()),
                () -> assertArrayEquals(numeric.getMin_value(), numeric.getMin()),
                () -> assertArrayEquals(numeric.getMax_value(), numeric.getMax()));
        assertEquals(2, footer.getColumn_ordersSize());
        assertTrue(footer.getColumn_orders().stream().allMatch(order -> order.isSetTYPE_ORDER()));
    }

    @Test
    void highCardinalityOmitsDistinctCountInsteadOfPublishingAnEstimate() throws Exception {
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("maps", List.of(
                SchemaDescriptor.createMapColumn("map", Type.INT32, Type.BYTE_ARRAY, false, false)));
        Map<Integer, byte[]> map = new LinkedHashMap<>();
        for (int i = 0; i < 4097; i++) {
            map.put(i, ByteBuffer.allocate(4).putInt(i).array());
        }
        Path destination = directory.resolve("many-distinct.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{map}));
        }
        var columns = WriterTestSupport.footer(destination).getRow_groups().getFirst().getColumns();
        assertFalse(columns.get(0).getMeta_data().getStatistics().isSetDistinct_count());
        assertFalse(columns.get(1).getMeta_data().getStatistics().isSetDistinct_count());
        assertEquals(0, columns.get(1).getMeta_data().getStatistics().getNull_count());
        assertTrue(columns.get(1).getMeta_data().getStatistics().isSetMin_value());
    }

    @Test
    void rowGroupTotalByteSizeSumsUncompressedColumnTotals() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("uncompressed-total.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema,
                CompressionCodec.ZSTD, 1024, 8192)) {
            for (int i = 0; i < 200; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{0}));
            }
        }
        var group = WriterTestSupport.footer(destination).getRow_groups().getFirst();
        var column = group.getColumns().getFirst().getMeta_data();
        assertEquals(org.apache.parquet.format.CompressionCodec.ZSTD, column.getCodec());
        assertTrue(column.getTotal_uncompressed_size() > column.getTotal_compressed_size());
        long expected = group.getColumns().stream().mapToLong(c -> c.getMeta_data().getTotal_uncompressed_size()).sum();
        assertEquals(expected, group.getTotal_byte_size());
    }

    @Test
    void pageAggregateSizesAndFileRowCounterDoNotOverflowInt() throws Exception {
        Class<?> infoClass = Class.forName("io.github.aloksingh.parquet.ParquetFileWriter$PageInfo");
        var constructor = infoClass.getDeclaredConstructor(int.class, int.class, int.class, CompressionCodec.class);
        constructor.setAccessible(true);
        Object info = constructor.newInstance(Integer.MAX_VALUE - 8, Integer.MAX_VALUE - 16, 1024,
                CompressionCodec.UNCOMPRESSED);
        var uncompressed = infoClass.getDeclaredField("total_uncompressed_size");
        var compressed = infoClass.getDeclaredField("total_compressed_size");
        uncompressed.setAccessible(true);
        compressed.setAccessible(true);
        assertEquals((long) Integer.MAX_VALUE - 8 + 1024, ((Number) uncompressed.get(info)).longValue());
        assertEquals((long) Integer.MAX_VALUE - 16 + 1024, ((Number) compressed.get(info)).longValue());
        assertEquals(long.class, ParquetFileWriter.class.getDeclaredField("totalRowCount").getType());
    }

    @Test
    void mapLeafNullCountsIncludeNullAndEmptyDefinitionEvents() throws Exception {
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("maps", List.of(
                SchemaDescriptor.createMapColumn("map", Type.INT32, Type.INT64, true, true)));
        Path destination = directory.resolve("map-stats.parquet");
        Map<Integer, Long> map = new LinkedHashMap<>();
        map.put(1, null);
        map.put(2, 5L);
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{null}));
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{Map.of()}));
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{map}));
        }
        var columns = WriterTestSupport.footer(destination).getRow_groups().getFirst().getColumns();
        assertEquals(4, columns.get(0).getMeta_data().getNum_values());
        assertEquals(4, columns.get(1).getMeta_data().getNum_values());
        assertEquals(2, columns.get(0).getMeta_data().getStatistics().getNull_count());
        assertEquals(3, columns.get(1).getMeta_data().getStatistics().getNull_count());
        assertEquals(2, columns.get(0).getMeta_data().getStatistics().getDistinct_count());
        assertEquals(1, columns.get(1).getMeta_data().getStatistics().getDistinct_count());
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"FLOAT", "DOUBLE"})
    void floatingBoundsIgnoreNaNsAndOrderNegativeZeroBeforePositiveZero(Type type) throws Exception {
        Object negativeZero = type == Type.FLOAT ? (Object) Float.valueOf(-0.0f) : Double.valueOf(-0.0d);
        Object positiveZero = type == Type.FLOAT ? (Object) 0.0f : 0.0d;
        Object nan = type == Type.FLOAT ? (Object) Float.NaN : Double.NaN;
        Statistics stats = statistics(type, nan, negativeZero, positiveZero, nan);
        byte[] min = type == Type.FLOAT
                ? ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(-0.0f).array()
                : ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(-0.0d).array();
        byte[] max = type == Type.FLOAT
                ? ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(0.0f).array()
                : ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(0.0d).array();
        assertArrayEquals(min, stats.getMin_value());
        assertArrayEquals(max, stats.getMax_value());
        assertEquals(0, stats.getNull_count(), "NaN is not null");
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"FLOAT", "DOUBLE"})
    void allNaNValuesOmitBoundsButRetainNullCount(Type type) throws Exception {
        Object nan = type == Type.FLOAT ? (Object) Float.NaN : Double.NaN;
        Statistics stats = statistics(type, nan, nan);
        assertNotNull(stats);
        assertTrue(stats.isSetNull_count());
        assertEquals(0, stats.getNull_count());
        assertFalse(stats.isSetMin_value());
        assertFalse(stats.isSetMax_value());
        assertFalse(stats.isSetMin());
        assertFalse(stats.isSetMax());
    }

    private static Object num(Type type, double value) {
        return switch (type) {
            case INT32 -> (Object) (int) value;
            case INT64 -> (Object) (long) value;
            case FLOAT -> (Object) (float) value;
            case DOUBLE -> (Object) value;
            default -> throw new IllegalArgumentException("Unexpected type " + type);
        };
    }

    private static byte[] bound(Type type, double value) {
        return type == Type.FLOAT
                ? ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat((float) value).array()
                : ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).array();
    }

    private static Object nan(Type type) {
        return type == Type.FLOAT ? (Object) Float.NaN : Double.NaN;
    }

    private Statistics statistics(String name, Type type, Object... values) throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(type, 1, 0);
        Path destination = directory.resolve(name + "-stats.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema)) {
            for (Object value : values) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
        }
        return WriterTestSupport.footer(destination).getRow_groups().getFirst().getColumns().getFirst()
                .getMeta_data().getStatistics();
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"FLOAT", "DOUBLE"})
    void minZeroAlongsidePositiveValuesSerializesNegativeZero(Type type) throws Exception {
        Statistics stats = statistics(type, num(type, 0.0), num(type, 1.0), num(type, 0.0));
        assertArrayEquals(bound(type, -0.0d), stats.getMin_value(), "zero min must serialize as -0.0");
        assertArrayEquals(bound(type, 1.0d), stats.getMax_value());
        assertArrayEquals(stats.getMin_value(), stats.getMin(), "deprecated min must carry the normalized bound");
        assertArrayEquals(stats.getMax_value(), stats.getMax());
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"FLOAT", "DOUBLE"})
    void maxZeroAlongsideNegativeValuesSerializesPositiveZero(Type type) throws Exception {
        Statistics stats = statistics(type, num(type, -2.0), num(type, -0.0), num(type, -1.0));
        assertArrayEquals(bound(type, -2.0d), stats.getMin_value());
        assertArrayEquals(bound(type, 0.0d), stats.getMax_value(), "zero max must serialize as +0.0");
        assertArrayEquals(stats.getMin_value(), stats.getMin());
        assertArrayEquals(stats.getMax_value(), stats.getMax(), "deprecated max must carry the normalized bound");
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"FLOAT", "DOUBLE"})
    void singleElementZeroArraysGetBothSpecBounds(Type type) throws Exception {
        Statistics positive = statistics("single-positive-zero", type, num(type, 0.0));
        assertArrayEquals(bound(type, -0.0d), positive.getMin_value());
        assertArrayEquals(bound(type, 0.0d), positive.getMax_value());
        Statistics negative = statistics("single-negative-zero", type, num(type, -0.0));
        assertArrayEquals(bound(type, -0.0d), negative.getMin_value(), "-0.0 alone must advertise a -0.0 min");
        assertArrayEquals(bound(type, 0.0d), negative.getMax_value(), "-0.0 alone must advertise a +0.0 max");
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"FLOAT", "DOUBLE"})
    void allZeroArraysNormalizeBothBounds(Type type) throws Exception {
        Statistics stats = statistics("all-zero", type,
                num(type, 0.0), num(type, -0.0), num(type, 0.0), num(type, -0.0));
        assertArrayEquals(bound(type, -0.0d), stats.getMin_value());
        assertArrayEquals(bound(type, 0.0d), stats.getMax_value());
        assertArrayEquals(stats.getMin_value(), stats.getMin());
        assertArrayEquals(stats.getMax_value(), stats.getMax());
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"FLOAT", "DOUBLE"})
    void nanMixedWithZerosNormalizesZeroBounds(Type type) throws Exception {
        Statistics stats = statistics("nan-zero", type,
                nan(type), num(type, 0.0), nan(type), num(type, -0.0), nan(type));
        assertArrayEquals(bound(type, -0.0d), stats.getMin_value());
        assertArrayEquals(bound(type, 0.0d), stats.getMax_value());
        assertEquals(0, stats.getNull_count(), "NaN is not null");
        assertArrayEquals(stats.getMin_value(), stats.getMin());
        assertArrayEquals(stats.getMax_value(), stats.getMax());
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64", "FLOAT", "DOUBLE"})
    void generatedArraysKeepEveryValueInsideEmittedBounds(Type type) throws Exception {
        java.util.Random random = new java.util.Random(0x5EED_1005L + type.ordinal());
        for (int iteration = 0; iteration < 120; iteration++) {
            Object[] values = generate(type, random, iteration % 12);
            Statistics stats = statistics(type + "-property-" + iteration, type, values);
            boolean anyComparable = false;
            List<Double> finite = new java.util.ArrayList<>();
            for (Object value : values) {
                if (value == null) continue;
                if (type == Type.FLOAT ? Float.isNaN((Float) value)
                        : type == Type.DOUBLE ? Double.isNaN((Double) value) : false) continue;
                anyComparable = true;
                double numeric = ((Number) value).doubleValue();
                if (Double.isFinite(numeric)) finite.add(numeric);
            }
            String caseInfo = type + " iteration " + iteration + " values " + java.util.Arrays.toString(values);
            if (!anyComparable) {
                assertFalse(stats.isSetMin_value(), "bounds must be omitted: " + caseInfo);
                assertFalse(stats.isSetMax_value(), "bounds must be omitted: " + caseInfo);
                assertFalse(stats.isSetMin(), "deprecated bounds must be omitted: " + caseInfo);
                assertFalse(stats.isSetMax(), "deprecated bounds must be omitted: " + caseInfo);
                continue;
            }
            assertTrue(stats.isSetMin_value(), "bounds must be present: " + caseInfo);
            assertTrue(stats.isSetMax_value(), "bounds must be present: " + caseInfo);
            assertArrayEquals(stats.getMin_value(), stats.getMin(), "deprecated min must mirror min_value: " + caseInfo);
            assertArrayEquals(stats.getMax_value(), stats.getMax(), "deprecated max must mirror max_value: " + caseInfo);
            double min = decode(type, stats.getMin_value());
            double max = decode(type, stats.getMax_value());
            assertTrue(Double.compare(min, max) <= 0, "min must not exceed max: " + caseInfo);
            for (double value : finite) {
                assertTrue(Double.compare(min, value) <= 0 && Double.compare(value, max) <= 0,
                        value + " outside [" + min + ", " + max + "]: " + caseInfo);
            }
            if (type == Type.FLOAT || type == Type.DOUBLE) {
                if (min == 0.0d) {
                    assertArrayEquals(bound(type, -0.0d), stats.getMin_value(), "zero min must be -0.0: " + caseInfo);
                }
                if (max == 0.0d) {
                    assertArrayEquals(bound(type, 0.0d), stats.getMax_value(), "zero max must be +0.0: " + caseInfo);
                }
            }
        }
    }

    private static double decode(Type type, byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        return switch (type) {
            case INT32 -> buffer.getInt();
            case INT64 -> buffer.getLong();
            case FLOAT -> buffer.getFloat();
            case DOUBLE -> buffer.getDouble();
            default -> throw new IllegalArgumentException("Unexpected type " + type);
        };
    }

    /**
     * Deterministic shapes: randoms, nulls, NaNs, zero-sign mixes, singles, duplicates.
     */
    private static Object[] generate(Type type, java.util.Random random, int shape) {
        boolean floating = type == Type.FLOAT || type == Type.DOUBLE;
        int length = switch (shape) {
            case 0, 7 -> 1 + random.nextInt(8);   // all-null, all-NaN
            case 1 -> 1;                          // single value
            default -> 2 + random.nextInt(7);
        };
        Object[] values = new Object[length];
        switch (shape) {
            case 0 -> java.util.Arrays.fill(values, null);                       // all null
            case 1 -> values[0] = anyValue(type, random);                        // single value
            case 2 -> {                                                          // duplicates
                Object repeated = anyValue(type, random);
                java.util.Arrays.fill(values, repeated);
            }
            case 3 -> {                                                          // +/-0.0 mixes
                for (int i = 0; i < length; i++) values[i] = num(type, random.nextBoolean() ? 0.0d : -0.0d);
            }
            case 4 -> {                                                          // NaN mixed with zeros
                for (int i = 0; i < length; i++) {
                    values[i] = !floating || random.nextBoolean()
                            ? num(type, random.nextBoolean() ? 0.0d : -0.0d) : nan(type);
                }
            }
            case 5 -> {                                                          // zero min, positive values
                for (int i = 0; i < length; i++) {
                    values[i] = i == 0 ? num(type, random.nextBoolean() ? 0.0d : -0.0d) : finiteValue(type, random);
                    if (i > 0 && ((Number) values[i]).doubleValue() <= 0)
                        values[i] = num(type, Math.abs(((Number) values[i]).doubleValue()) + 1);
                }
            }
            case 6 -> {                                                          // zero max, negative values
                for (int i = 0; i < length; i++) {
                    values[i] = i == 0 ? num(type, random.nextBoolean() ? 0.0d : -0.0d) : finiteValue(type, random);
                    if (i > 0 && ((Number) values[i]).doubleValue() >= 0)
                        values[i] = num(type, -Math.abs(((Number) values[i]).doubleValue()) - 1);
                }
            }
            case 7 -> {                                                          // all NaN (all null for ints)
                Object filler = floating ? nan(type) : null;
                java.util.Arrays.fill(values, filler);
            }
            case 8 -> {                                                          // small range, many duplicates
                for (int i = 0; i < length; i++) values[i] = num(type, random.nextInt(4) - 2);
            }
            case 9 -> {                                                          // wide-range randoms
                for (int i = 0; i < length; i++) values[i] = anyValue(type, random);
            }
            case 10 -> {                                                         // null + NaN + zeros + finite mix
                for (int i = 0; i < length; i++) {
                    values[i] = switch (random.nextInt(4)) {
                        case 0 -> null;
                        case 1 -> floating ? nan(type) : num(type, 0.0d);
                        case 2 -> num(type, random.nextBoolean() ? 0.0d : -0.0d);
                        default -> finiteValue(type, random);
                    };
                }
            }
            default -> {                                                         // zeros only, one sign each
                double zero = random.nextBoolean() ? 0.0d : -0.0d;
                java.util.Arrays.fill(values, num(type, zero));
            }
        }
        return values;
    }

    private static Object anyValue(Type type, java.util.Random random) {
        return switch (type) {
            case INT32 -> random.nextInt();
            case INT64 -> random.nextLong();
            case FLOAT -> random.nextFloat() * (random.nextBoolean() ? 1 : 1_000_000);
            case DOUBLE -> random.nextDouble() * (random.nextBoolean() ? 1 : 1_000_000);
            default -> throw new IllegalArgumentException("Unexpected type " + type);
        };
    }

    private static Object finiteValue(Type type, java.util.Random random) {
        return anyValue(type, random);
    }
}
