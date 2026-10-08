package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.*;
import io.github.aloksingh.parquet.model.ColumnStatistics.BoundsOrder;
import io.github.aloksingh.parquet.util.filter.ColumnFilters;
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.FilterOperator;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilterSet;
import org.apache.parquet.format.FileMetaData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class ReaderPruningOrderTest {
    @TempDir
    Path directory;

    private Path write(Type type, PrimitiveLogicalType annotation, Object... values) throws Exception {
        var descriptor = new ColumnDescriptor(type, new String[]{"value"}, 1, 0, 0, annotation);
        var schema = SchemaDescriptor.fromLogicalColumns("pruning_order",
                SchemaDescriptor.createLogicalColumnsFromPhysical(List.of(descriptor)));
        Path path = directory.resolve("pruning-order.parquet");
        try (var writer = new ParquetFileWriter(path, schema, CompressionCodec.UNCOMPRESSED, 1024, 1)) {
            for (Object value : values) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
        }
        return path;
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    private static void rewriteFooter(Path path, Consumer<FileMetaData> change) throws Exception {
        FileMetaData footer = WriterTestSupport.footer(path);
        change.accept(footer);
        var encoded = new ByteArrayOutputStream();
        footer.write(new TCompactProtocol(new TIOStreamTransport(encoded)));
        replaceFooter(path, encoded.toByteArray());
    }

    private static void replaceFooter(Path path, byte[] encoded) throws Exception {
        byte[] file = Files.readAllBytes(path);
        int oldLength = ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        var output = new ByteArrayOutputStream();
        output.write(file, 0, file.length - 8 - oldLength);
        output.writeBytes(encoded);
        output.writeBytes(intBytes(encoded.length));
        output.write(file, file.length - 4, 4);
        Files.write(path, output.toByteArray());
    }

    private record Scan(List<Object> values, long droppedGroups) {
    }

    private static Scan scan(Path path, FilterOperator operator, Object constant, boolean pruning) throws Exception {
        try (var reader = new ParquetFileReader(path)) {
            var column = reader.getSchema().getLogicalColumn(0);
            var predicate = new ColumnFilters().createFilter(column, operator, constant);
            var filter = new RowColumnGroupFilterSet(FilterJoinType.All, predicate);
            var options = ReadOptions.builder().filter(filter).pruning(pruning).build();
            try (var rows = reader.rowIterator(options)) {
                var values = new ArrayList<Object>();
                while (rows.hasNext()) values.add(rows.next().getColumnValue(0));
                return new Scan(values, rows.getDroppedRowGroupCount());
            }
        }
    }

    @Test
    void physicalLeafColumnOrdersRemainAlignedAfterMapColumns() throws Exception {
        var key = new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"map", "key_value", "key"}, 2, 1, 0,
                PrimitiveLogicalType.string());
        var value = new ColumnDescriptor(Type.INT32, new String[]{"map", "key_value", "value"}, 3, 1, 0,
                PrimitiveLogicalType.integer(32, false));
        var map = new LogicalColumnDescriptor("map", LogicalType.MAP,
                new MapMetadata(0, 1, Type.BYTE_ARRAY, Type.INT32, key, value));
        var idDescriptor = new ColumnDescriptor(Type.INT32, new String[]{"id"}, 1, 0, 0,
                PrimitiveLogicalType.integer(32, false));
        var id = new LogicalColumnDescriptor("id", LogicalType.PRIMITIVE, Type.INT32, idDescriptor);
        var schema = SchemaDescriptor.fromLogicalColumns("mapped_pruning", List.of(map, id));
        Path path = directory.resolve("mapped-pruning.parquet");
        try (var writer = new ParquetFileWriter(path, schema, CompressionCodec.UNCOMPRESSED, 1024, 1)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{Map.of("key", -1), -1}));
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{Map.of("key", 1), 1}));
        }
        for (boolean pruning : List.of(false, true)) {
            try (var reader = new ParquetFileReader(path)) {
                var actualSchema = reader.getSchema();
                var keyed = new ColumnFilters().createFilter(actualSchema.getLogicalColumn(0),
                        FilterOperator.eq, "4294967295", Optional.of("key"));
                var scalar = new ColumnFilters().createFilter(actualSchema.getLogicalColumn(1),
                        FilterOperator.eq, "4294967295");
                var filter = new RowColumnGroupFilterSet(FilterJoinType.All, keyed, scalar);
                try (var rows = reader.rowIterator(ReadOptions.builder().filter(filter).pruning(pruning).build())) {
                    assertTrue(rows.hasNext());
                    assertEquals(4294967295L, rows.next().getColumnValue(1));
                    assertFalse(rows.hasNext());
                    assertEquals(pruning ? 1 : 0, rows.getDroppedRowGroupCount());
                }
                assertEquals(BoundsOrder.TYPE_DEFINED, reader.getMetadata().rowGroups().getFirst()
                        .columns().get(2).statistics().minOrder());
            }
        }
    }

    @Test
    void unknownColumnOrderUnionIsIgnoredForPruning() throws Exception {
        Path path = write(Type.INT32, PrimitiveLogicalType.none(), 5);
        rewriteFooter(path, footer -> {
            var statistics = footer.getRow_groups().getFirst().getColumns().getFirst().getMeta_data().getStatistics();
            statistics.setMin_value(intBytes(10));
            statistics.setMax_value(intBytes(20));
        });
        var encoded = new ByteArrayOutputStream();
        WriterTestSupport.footer(path).write(new TCompactProtocol(new TIOStreamTransport(encoded)));
        byte[] bytes = encoded.toByteArray();
        // Footer field 7: LIST[1 STRUCT], ColumnOrder field 1: empty TYPE_ORDER,
        // STOP union, STOP footer. Replace field 1 with unknown struct field 2.
        assertArrayEquals(new byte[]{0x19, 0x1c, 0x1c, 0, 0, 0}, Arrays.copyOfRange(bytes, bytes.length - 6, bytes.length));
        bytes[bytes.length - 4] = 0x2c;
        replaceFooter(path, bytes);
        assertEquals(List.of(5), scan(path, FilterOperator.eq, 5, true).values());
        try (var reader = new ParquetFileReader(path)) {
            assertEquals(BoundsOrder.UNKNOWN, reader.getMetadata().rowGroups().getFirst()
                    .columns().getFirst().statistics().minOrder());
        }
    }

    @Test
    void incorrectColumnOrderCardinalityCannotHideRows() throws Exception {
        Path path = write(Type.INT32, PrimitiveLogicalType.none(), 5);
        rewriteFooter(path, footer -> {
            var order = footer.getColumn_orders().getFirst();
            footer.setColumn_orders(List.of(order, order.deepCopy()));
            var statistics = footer.getRow_groups().getFirst().getColumns().getFirst().getMeta_data().getStatistics();
            statistics.setMin_value(intBytes(10));
            statistics.setMax_value(intBytes(20));
        });
        assertEquals(List.of(5), scan(path, FilterOperator.eq, 5, true).values());
    }

    @Test
    void mixedModernAndLegacyBoundsCannotHideRows() throws Exception {
        Path path = write(Type.INT32, PrimitiveLogicalType.none(), 5);
        rewriteFooter(path, footer -> {
            var statistics = footer.getRow_groups().getFirst().getColumns().getFirst().getMeta_data().getStatistics();
            statistics.setMin_value(intBytes(10));
            statistics.unsetMax_value();
            statistics.setMax(intBytes(20));
        });
        try (var reader = new ParquetFileReader(path)) {
            var statistics = reader.getMetadata().rowGroups().getFirst().columns().getFirst().statistics();
            assertEquals(BoundsOrder.TYPE_DEFINED, statistics.minOrder());
            assertEquals(BoundsOrder.LEGACY_SIGNED, statistics.maxOrder());
        }
        assertEquals(List.of(5), scan(path, FilterOperator.eq, 5, true).values());
    }

    @Test
    void signedLegacyBoundsStillPruneWithoutColumnOrder() throws Exception {
        Path path = write(Type.INT32, PrimitiveLogicalType.none(), 5);
        rewriteFooter(path, footer -> {
            footer.unsetColumn_orders();
            var statistics = footer.getRow_groups().getFirst().getColumns().getFirst().getMeta_data().getStatistics();
            statistics.unsetMin_value();
            statistics.unsetMax_value();
        });
        assertEquals(List.of(5), scan(path, FilterOperator.eq, 5, true).values());
        var excluded = scan(path, FilterOperator.eq, 1, true);
        assertEquals(List.of(), excluded.values());
        assertEquals(1, excluded.droppedGroups());
        try (var reader = new ParquetFileReader(path)) {
            assertEquals(BoundsOrder.LEGACY_SIGNED, reader.getMetadata().rowGroups().getFirst()
                    .columns().getFirst().statistics().minOrder());
        }
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64"})
    void unsignedFiltersHaveIdenticalResultsWithPruningEnabled(Type type) throws Exception {
        int width = type == Type.INT32 ? 32 : 64;
        Object[] physical = type == Type.INT32
                ? new Object[]{-1, 1, 0, Integer.MIN_VALUE, null}
                : new Object[]{-1L, 1L, 0L, Long.MIN_VALUE, null};
        Path path = write(type, PrimitiveLogicalType.integer(width, false), physical);
        List<Object> logical = new ArrayList<>();
        for (Object value : physical) {
            if (value != null) logical.add(type == Type.INT32
                    ? (Object) Integer.toUnsignedLong((Integer) value)
                    : new BigInteger(Long.toUnsignedString((Long) value)));
        }
        for (BigInteger constant : List.of(BigInteger.ZERO, BigInteger.ONE,
                BigInteger.ONE.shiftLeft(width - 1), BigInteger.ONE.shiftLeft(width).subtract(BigInteger.ONE))) {
            for (var operator : List.of(FilterOperator.eq, FilterOperator.neq, FilterOperator.lt,
                    FilterOperator.lte, FilterOperator.gt, FilterOperator.gte)) {
                var expected = logical.stream().filter(value -> {
                    int comparison = new BigInteger(value.toString()).compareTo(constant);
                    return switch (operator) {
                        case eq -> comparison == 0;
                        case neq -> comparison != 0;
                        case lt -> comparison < 0;
                        case lte -> comparison <= 0;
                        case gt -> comparison > 0;
                        case gte -> comparison >= 0;
                        default -> throw new AssertionError(operator);
                    };
                }).toList();
                assertEquals(expected, assertDoesNotThrow(() -> scan(path, operator, constant, false)).values());
                assertEquals(expected, assertDoesNotThrow(() -> scan(path, operator, constant, true)).values());
            }
        }
        assertTrue(scan(path, FilterOperator.eq, BigInteger.ONE, true).droppedGroups() > 0,
                "correct unsigned pruning must not be disabled globally");
    }

    @Test
    void modernBoundsWithoutColumnOrderCannotHideMatchingRows() throws Exception {
        Path path = write(Type.INT32, PrimitiveLogicalType.none(), 5);
        rewriteFooter(path, footer -> {
            footer.unsetColumn_orders();
            // Deliberately untrusted modern bounds: without ColumnOrder they cannot
            // establish a signed numeric range. Keep the real legacy bounds intact.
            var statistics = footer.getRow_groups().getFirst().getColumns().getFirst()
                    .getMeta_data().getStatistics();
            statistics.setMin_value(intBytes(10));
            statistics.setMax_value(intBytes(20));
        });
        assertEquals(List.of(5), scan(path, FilterOperator.eq, 5, false).values());
        assertEquals(List.of(5), scan(path, FilterOperator.eq, 5, true).values());
        assertEquals(0, scan(path, FilterOperator.eq, 5, true).droppedGroups());
    }
}
