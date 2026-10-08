package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.*;
import io.github.aloksingh.parquet.writer.WriterStatistics;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Statistics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class WriterLogicalStatisticsTest {
    @TempDir
    Path directory;

    private static ColumnDescriptor column(Type type, int length, PrimitiveLogicalType annotation) {
        return new ColumnDescriptor(type, new String[]{"value"}, 1, 0, length, annotation);
    }

    private Path write(ColumnDescriptor descriptor, int pageTarget, Object... values) throws Exception {
        return writeWithTargets(descriptor, pageTarget, 8192, values);
    }

    private Path writeWithTargets(ColumnDescriptor descriptor, int pageTarget, int groupTarget, Object... values)
            throws Exception {
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("logical_statistics",
                SchemaDescriptor.createLogicalColumnsFromPhysical(List.of(descriptor)));
        Path destination = directory.resolve("logical-statistics.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema,
                CompressionCodec.UNCOMPRESSED, pageTarget, groupTarget)) {
            for (Object value : values) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
        }
        return destination;
    }

    private static Statistics chunkStatistics(Path path) throws Exception {
        return WriterTestSupport.footer(path).getRow_groups().getFirst().getColumns().getFirst()
                .getMeta_data().getStatistics();
    }

    private static byte[] integralBytes(Type type, long value) {
        return type == Type.INT32
                ? ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt((int) value).array()
                : ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    private static void bounds(Statistics statistics, byte[] minimum, byte[] maximum) {
        assertArrayEquals(minimum, statistics.getMin_value(), "minimum physical bytes");
        assertArrayEquals(maximum, statistics.getMax_value(), "maximum physical bytes");
    }

    private static Stream<ColumnDescriptor> unsignedBinaryColumns() {
        return Stream.of(column(Type.BYTE_ARRAY, 0, PrimitiveLogicalType.none()),
                column(Type.FIXED_LEN_BYTE_ARRAY, 16, PrimitiveLogicalType.none()),
                column(Type.BYTE_ARRAY, 0, PrimitiveLogicalType.string()),
                column(Type.FIXED_LEN_BYTE_ARRAY, 16, PrimitiveLogicalType.uuid()));
    }

    @ParameterizedTest
    @MethodSource("unsignedBinaryColumns")
    void nonDecimalBinaryBoundsRetainUnsignedLexicographicOrder(ColumnDescriptor descriptor) throws Exception {
        byte[] minimum;
        byte[] maximum;
        if (descriptor.physicalType() == Type.FIXED_LEN_BYTE_ARRAY) {
            minimum = new byte[16];
            maximum = new byte[16];
            minimum[0] = 0x7f;
            maximum[0] = (byte) 0xff;
        } else {
            minimum = new byte[]{0x7a};
            maximum = new byte[]{(byte) 0xc3, (byte) 0xa9};
        }
        Path path = write(descriptor, 1024, maximum, minimum);
        bounds(chunkStatistics(path), minimum, maximum);
        bounds(WriterTestSupport.pages(path, 0, 0).getFirst().header().getData_page_header_v2()
                .getStatistics(), minimum, maximum);
        assertFalse(chunkStatistics(path).isSetMin());
        assertFalse(chunkStatistics(path).isSetMax());
    }

    @Test
    void variableDecimalBoundsCompareDifferentLengthsAndPreserveSignExtension() throws Exception {
        byte[] negative = {(byte) 0xff, 0x7f};
        byte[] positive = {0, (byte) 0x80};
        Path path = write(column(Type.BYTE_ARRAY, 0, PrimitiveLogicalType.decimal(6, 2)), 1024,
                new byte[]{0x7f}, positive, new byte[]{(byte) 0xff}, negative,
                new byte[]{(byte) 0xff, (byte) 0xff}, null);
        for (Statistics statistics : List.of(chunkStatistics(path), WriterTestSupport.pages(path, 0, 0)
                .getFirst().header().getData_page_header_v2().getStatistics())) {
            bounds(statistics, negative, positive);
            assertEquals(1, statistics.getNull_count());
            assertFalse(statistics.isSetMin());
            assertFalse(statistics.isSetMax());
        }
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.prepareStatement(
                     "SELECT min(\"value\"), max(\"value\"), count(*) FROM read_parquet(?) WHERE \"value\" < 0")) {
            statement.setString(1, path.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals("-1.29", rows.getString(1));
                assertEquals("-0.01", rows.getString(2));
                assertEquals(3, rows.getLong(3));
                assertFalse(rows.next());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64"})
    void unsignedBoundsAndPhysicalPayloadSurvivePageSplits(Type type) throws Exception {
        int width = type == Type.INT32 ? 32 : 64;
        Object[] values = {-1, 1, null, 0, -2};
        Path path = write(column(type, 0, PrimitiveLogicalType.integer(width, false)), 1, values);
        var pages = WriterTestSupport.pages(path, 0, 0);
        assertEquals(values.length, pages.size(), "each whole row must occupy its own page");
        for (int index = 0; index < values.length; index++) {
            var page = pages.get(index);
            var header = page.header().getData_page_header_v2();
            Statistics statistics = header.getStatistics();
            int levels = header.getDefinition_levels_byte_length() + header.getRepetition_levels_byte_length();
            if (values[index] == null) {
                assertFalse(statistics.isSetMin_value());
                assertFalse(statistics.isSetMax_value());
                assertEquals(1, statistics.getNull_count());
                assertEquals(levels, page.payload().length, "null has no physical value payload");
            } else {
                byte[] physical = integralBytes(type, ((Number) values[index]).longValue());
                bounds(statistics, physical, physical);
                assertArrayEquals(physical, Arrays.copyOfRange(page.payload(), levels, page.payload().length));
            }
            assertFalse(statistics.isSetMin());
            assertFalse(statistics.isSetMax());
        }
        bounds(chunkStatistics(path), integralBytes(type, 0), integralBytes(type, -1));
        assertEquals(1, chunkStatistics(path).getNull_count());
        assertEquals(values.length - 1, chunkStatistics(path).getDistinct_count());
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"})
    void decimalBoundsAndPhysicalPayloadSurvivePageSplits(Type type) throws Exception {
        byte[] minimum = {(byte) 0xff, 0x7f};
        byte[] maximum = {0, (byte) 0x80};
        Object[] values = {maximum, new byte[]{(byte) 0xff, (byte) 0xff}, null, minimum};
        var descriptor = column(type, type == Type.FIXED_LEN_BYTE_ARRAY ? 2 : 0,
                PrimitiveLogicalType.decimal(4, 2));
        Path path = write(descriptor, 1, values);
        var pages = WriterTestSupport.pages(path, 0, 0);
        assertEquals(values.length, pages.size());
        for (int index = 0; index < values.length; index++) {
            var page = pages.get(index);
            var header = page.header().getData_page_header_v2();
            int levels = header.getDefinition_levels_byte_length() + header.getRepetition_levels_byte_length();
            if (values[index] == null) {
                assertFalse(header.getStatistics().isSetMin_value());
                assertFalse(header.getStatistics().isSetMax_value());
                assertEquals(levels, page.payload().length);
            } else {
                byte[] physical = (byte[]) values[index];
                bounds(header.getStatistics(), physical, physical);
                byte[] payload = Arrays.copyOfRange(page.payload(), levels, page.payload().length);
                if (type == Type.BYTE_ARRAY) {
                    assertEquals(physical.length, ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN).getInt());
                    payload = Arrays.copyOfRange(payload, 4, payload.length);
                }
                assertArrayEquals(physical, payload, "decimal value bytes must stay big-endian");
            }
        }
        bounds(chunkStatistics(path), minimum, maximum);
        assertEquals(1, chunkStatistics(path).getNull_count());
        var annotation = PrimitiveLogicalType.fromSchemaElement(WriterTestSupport.footer(path).getSchema().get(1));
        assertEquals(descriptor.annotation(), annotation, "decimal precision/scale must not change");
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64"})
    void unsignedStatisticsResetBetweenRowGroups(Type type) throws Exception {
        int width = type == Type.INT32 ? 32 : 64;
        Path path = writeWithTargets(column(type, 0, PrimitiveLogicalType.integer(width, false)), 1, 1, -1, 1);
        var footer = WriterTestSupport.footer(path);
        assertEquals(2, footer.getRow_groupsSize());
        bounds(footer.getRow_groups().get(0).getColumns().getFirst().getMeta_data().getStatistics(),
                integralBytes(type, -1), integralBytes(type, -1));
        bounds(footer.getRow_groups().get(1).getColumns().getFirst().getMeta_data().getStatistics(),
                integralBytes(type, 1), integralBytes(type, 1));
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.prepareStatement("SELECT count(*) FROM read_parquet(?) WHERE \"value\" = 1")) {
            statement.setString(1, path.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals(1, rows.getLong(1));
                assertFalse(rows.next());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64"})
    void integralDecimalsKeepSignedPhysicalBounds(Type type) throws Exception {
        Path path = write(column(type, 0, PrimitiveLogicalType.decimal(6, 2)), 1024, -1, 1);
        bounds(chunkStatistics(path), integralBytes(type, -1), integralBytes(type, 1));
        assertArrayEquals(chunkStatistics(path).getMin_value(), chunkStatistics(path).getMin());
        assertArrayEquals(chunkStatistics(path).getMax_value(), chunkStatistics(path).getMax());
    }

    @Test
    void equivalentModernAndLegacyIntegerAnnotationsCanMerge() {
        var legacy = PrimitiveLogicalType.fromSchemaElement(new SchemaElement().setConverted_type(ConvertedType.UINT_32));
        WriterStatistics destination = new WriterStatistics(column(Type.INT32, 0, PrimitiveLogicalType.integer(32, false)));
        WriterStatistics incoming = new WriterStatistics(column(Type.INT32, 0, legacy));
        destination.add(1);
        incoming.add(-1);
        incoming.addNull();
        Statistics incomingBefore = incoming.toParquet();
        destination.merge(incoming);
        bounds(destination.toParquet(), integralBytes(Type.INT32, 1), integralBytes(Type.INT32, -1));
        assertEquals(1, destination.nullCount());
        assertEquals(2, destination.toParquet().getDistinct_count());
        assertEquals(incomingBefore, incoming.toParquet());
    }

    @Test
    void physicalOrderCompatibilityConstructorCanMergeUnannotatedDescriptors() {
        WriterStatistics destination = new WriterStatistics(Type.INT32);
        WriterStatistics incoming = new WriterStatistics(column(Type.INT32, 0, PrimitiveLogicalType.none()));
        destination.add(1);
        incoming.add(-1);
        destination.merge(incoming);
        bounds(destination.toParquet(), integralBytes(Type.INT32, -1), integralBytes(Type.INT32, 1));
    }

    private static Stream<Arguments> incompatibleColumns() {
        return Stream.of(
                Arguments.of(column(Type.INT32, 0, PrimitiveLogicalType.none()),
                        column(Type.INT64, 0, PrimitiveLogicalType.none())),
                Arguments.of(column(Type.INT64, 0, PrimitiveLogicalType.timestamp(PrimitiveLogicalType.TimeUnit.MICROS, true)),
                        column(Type.INT64, 0, PrimitiveLogicalType.timestamp(PrimitiveLogicalType.TimeUnit.NANOS, true))),
                Arguments.of(column(Type.INT32, 0, PrimitiveLogicalType.integer(32, true)),
                        column(Type.INT32, 0, PrimitiveLogicalType.integer(32, false))),
                Arguments.of(column(Type.INT32, 0, PrimitiveLogicalType.integer(8, false)),
                        column(Type.INT32, 0, PrimitiveLogicalType.integer(16, false))),
                Arguments.of(column(Type.FIXED_LEN_BYTE_ARRAY, 4, PrimitiveLogicalType.decimal(6, 1)),
                        column(Type.FIXED_LEN_BYTE_ARRAY, 4, PrimitiveLogicalType.decimal(6, 2))),
                Arguments.of(column(Type.FIXED_LEN_BYTE_ARRAY, 4, PrimitiveLogicalType.decimal(6, 1)),
                        column(Type.FIXED_LEN_BYTE_ARRAY, 4, PrimitiveLogicalType.decimal(7, 1))),
                Arguments.of(column(Type.FIXED_LEN_BYTE_ARRAY, 3, PrimitiveLogicalType.decimal(6, 1)),
                        column(Type.FIXED_LEN_BYTE_ARRAY, 4, PrimitiveLogicalType.decimal(6, 1))),
                Arguments.of(column(Type.INT32, 0, PrimitiveLogicalType.none()),
                        column(Type.INT32, 0, PrimitiveLogicalType.unknown())));
    }

    private static Object sample(ColumnDescriptor descriptor, int value) {
        if (descriptor.physicalType() == Type.INT32) return value;
        if (descriptor.physicalType() == Type.INT64) return (long) value;
        byte[] bytes = new byte[descriptor.typeLength()];
        bytes[bytes.length - 1] = (byte) value;
        return bytes;
    }

    @ParameterizedTest
    @MethodSource("incompatibleColumns")
    void incompatibleStatisticsMergeIsRejectedBeforeMutation(ColumnDescriptor first, ColumnDescriptor second) {
        WriterStatistics destination = new WriterStatistics(first);
        WriterStatistics incoming = new WriterStatistics(second);
        destination.add(sample(first, 1));
        destination.addNull();
        incoming.add(sample(second, 2));
        incoming.addNull();
        Statistics before = destination.toParquet();
        Statistics incomingBefore = incoming.toParquet();
        assertThrows(IllegalArgumentException.class, () -> destination.merge(incoming));
        assertEquals(before, destination.toParquet(), "rejected merge must not mutate destination");
        assertEquals(incomingBefore, incoming.toParquet(), "rejected merge must not mutate source");
    }

    @ParameterizedTest
    @ValueSource(ints = {8, 16, 32, 64})
    void unsignedAnnotationsDoNotEmitDeprecatedSignedBounds(int width) throws Exception {
        Type type = width == 64 ? Type.INT64 : Type.INT32;
        Path path = write(column(type, 0, PrimitiveLogicalType.integer(width, false)), 1024, 1, 2);
        for (Statistics statistics : List.of(chunkStatistics(path), WriterTestSupport.pages(path, 0, 0)
                .getFirst().header().getData_page_header_v2().getStatistics())) {
            bounds(statistics, integralBytes(type, 1), integralBytes(type, 2));
            assertFalse(statistics.isSetMin(), "unsigned bound is not a deprecated signed minimum");
            assertFalse(statistics.isSetMax(), "unsigned bound is not a deprecated signed maximum");
        }
    }

    @Test
    void unknownOrderingOmitsBoundsButRetainsCounts() {
        WriterStatistics statistics = new WriterStatistics(column(Type.INT32, 0, PrimitiveLogicalType.unknown()));
        statistics.add(7);
        statistics.add(null);
        var result = statistics.toParquet();
        assertFalse(result.isSetMin_value());
        assertFalse(result.isSetMax_value());
        assertFalse(result.isSetMin());
        assertFalse(result.isSetMax());
        assertEquals(1, result.getNull_count());
        assertEquals(1, result.getDistinct_count());
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"})
    void binaryDecimalBoundsUseSignedNumericOrder(Type type) throws Exception {
        byte[] negative = {(byte) 0xff};
        byte[] positive = {1};
        Path path = write(column(type, type == Type.FIXED_LEN_BYTE_ARRAY ? 1 : 0,
                PrimitiveLogicalType.decimal(2, 0)), 1024, negative, positive);
        bounds(WriterTestSupport.pages(path, 0, 0).getFirst().header()
                .getData_page_header_v2().getStatistics(), negative, positive);
        bounds(chunkStatistics(path), negative, positive);
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64"})
    void unsignedBoundsUseLogicalOrderOnPagesAndChunks(Type type) throws Exception {
        int width = type == Type.INT32 ? 32 : 64;
        Path path = write(column(type, 0, PrimitiveLogicalType.integer(width, false)), 1024, -1, 1);
        var pages = WriterTestSupport.pages(path, 0, 0);
        assertEquals(1, pages.size());
        bounds(pages.getFirst().header().getData_page_header_v2().getStatistics(),
                integralBytes(type, 1), integralBytes(type, -1));
        bounds(chunkStatistics(path), integralBytes(type, 1), integralBytes(type, -1));
        assertTrue(WriterTestSupport.footer(path).getColumn_orders().getFirst().isSetTYPE_ORDER());
    }
}
