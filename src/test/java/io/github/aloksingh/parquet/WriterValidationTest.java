package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ListMetadata;
import io.github.aloksingh.parquet.model.MapMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class WriterValidationTest {
    @TempDir
    Path directory;

    static Stream<Arguments> duplicatePhysicalKeys() {
        Map<Object, Long> integers = new LinkedHashMap<>();
        integers.put(1, 10L);
        integers.put(1L, 20L);
        Map<Object, Long> binaries = new LinkedHashMap<>();
        binaries.put(new byte[]{'a', 'b'}, 10L);
        binaries.put(new byte[]{'a', 'b'}, 20L);
        Map<Object, Long> representations = new LinkedHashMap<>();
        representations.put("ab", 10L);
        representations.put(ByteBuffer.wrap(new byte[]{'a', 'b'}), 20L);
        return Stream.of(Arguments.of(Type.INT32, integers), Arguments.of(Type.BYTE_ARRAY, binaries),
                Arguments.of(Type.BYTE_ARRAY, representations));
    }

    @ParameterizedTest
    @MethodSource("duplicatePhysicalKeys")
    void duplicateEncodedMapKeysAreRejectedBeforeOpening(Type keyType, Map<Object, Long> map) throws Exception {
        var schema = SchemaDescriptor.fromLogicalColumns("duplicates", List.of(
                SchemaDescriptor.createMapColumn("map", keyType, Type.INT64, true, false)));
        Path destination = directory.resolve("duplicate-keys.parquet");
        byte[] sentinel = {9, 1, 9};
        Files.write(destination, sentinel);
        int[] opened = {0};
        var writer = new ParquetFileWriter(destination, schema) {
            @Override
            java.io.OutputStream openSink(Path path) throws java.io.IOException {
                opened[0]++;
                return super.openSink(path);
            }
        };
        assertThrows(IllegalArgumentException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{map})));
        assertEquals(0, opened[0]);
        assertDoesNotThrow(writer::close);
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
    }

    static SchemaDescriptor scalar(Type type, int definition, int length) {
        ColumnDescriptor descriptor = new ColumnDescriptor(type, new String[]{"value"}, definition, 0, length);
        return SchemaDescriptor.fromLogicalColumns("same_name", List.of(
                new LogicalColumnDescriptor("value", LogicalType.PRIMITIVE, type, descriptor)));
    }

    static Stream<Arguments> invalidPrimitiveValues() {
        return Stream.of(
                Arguments.of(Type.BOOLEAN, 0, null),
                Arguments.of(Type.BOOLEAN, 0, 1),
                Arguments.of(Type.INT32, 0, 2147483648L),
                Arguments.of(Type.INT32, 0, -2147483649L),
                Arguments.of(Type.INT32, 0, 1.25d),
                Arguments.of(Type.INT32, 0, new BigDecimal("1.25")),
                Arguments.of(Type.INT32, 0, "5"),
                Arguments.of(Type.INT64, 0, new BigInteger("9223372036854775808")),
                Arguments.of(Type.INT64, 0, 1.5f),
                Arguments.of(Type.FLOAT, 0, Double.MAX_VALUE),
                Arguments.of(Type.FLOAT, 0, Double.MIN_VALUE),
                Arguments.of(Type.FLOAT, 0, new BigDecimal("1e-1000")),
                Arguments.of(Type.DOUBLE, 0, new BigDecimal("1e-1000")),
                Arguments.of(Type.FLOAT, 0, "1.2"),
                Arguments.of(Type.DOUBLE, 0, new BigDecimal("1e10000")),
                Arguments.of(Type.BYTE_ARRAY, 0, new Object()),
                Arguments.of(Type.BYTE_ARRAY, 1, 17),
                Arguments.of(Type.FIXED_LEN_BYTE_ARRAY, 0, new byte[]{1}),
                Arguments.of(Type.FIXED_LEN_BYTE_ARRAY, 0, ByteBuffer.wrap(new byte[]{1, 2, 3})));
    }

    static Stream<Arguments> unsupportedSchemas() {
        return Stream.of(
                bad("INT96", () -> scalar(Type.INT96, 0, 0)),
                bad("nested path", () -> physical(new ColumnDescriptor(Type.INT32,
                        new String[]{"outer", "value"}, 1, 0, 0))),
                bad("repeated primitive", () -> physical(new ColumnDescriptor(Type.INT32,
                        new String[]{"value"}, 1, 1, 0))),
                bad("nested definition level", () -> physical(new ColumnDescriptor(Type.INT32,
                        new String[]{"value"}, 2, 0, 0))),
                bad("missing fixed size", () -> scalar(Type.FIXED_LEN_BYTE_ARRAY, 0, 0)),
                bad("logical/physical disagreement", () -> SchemaDescriptor.fromLogicalColumns("bad", List.of(
                        new LogicalColumnDescriptor("value", LogicalType.PRIMITIVE, Type.INT64,
                                new ColumnDescriptor(Type.INT32, new String[]{"value"}, 0, 0, 0))))),
                bad("LIST", () -> {
                    ColumnDescriptor leaf = new ColumnDescriptor(Type.INT32,
                            new String[]{"values", "list", "element"}, 3, 1, 0);
                    return SchemaDescriptor.fromLogicalColumns("bad", List.of(new LogicalColumnDescriptor(
                            "values", LogicalType.LIST, new ListMetadata(0, Type.INT32, leaf))));
                }),
                bad("STRUCT", () -> {
                    ColumnDescriptor leaf = new ColumnDescriptor(Type.INT32, new String[]{"value"}, 0, 0, 0);
                    return new SchemaDescriptor("bad", List.of(leaf), List.of(new LogicalColumnDescriptor(
                            "value", LogicalType.STRUCT, Type.INT32, leaf)));
                }),
                bad("noncanonical MAP levels", () -> malformedMap(3, "key_value", 0)),
                bad("noncanonical MAP path", () -> malformedMap(2, "entries", 0)),
                bad("wrong MAP indexes", () -> malformedMap(2, "key_value", 1)),
                bad("empty schema", () -> new SchemaDescriptor("empty", List.of(), List.of())),
                bad("duplicate column", () -> {
                    ColumnDescriptor first = new ColumnDescriptor(Type.INT32, new String[]{"value"}, 0, 0, 0);
                    ColumnDescriptor second = new ColumnDescriptor(Type.INT64, new String[]{"value"}, 0, 0, 0);
                    return new SchemaDescriptor("bad", List.of(first, second), List.of());
                }));
    }

    private static Arguments bad(String name, Supplier<SchemaDescriptor> schema) {
        return Arguments.of(name, schema);
    }

    private static SchemaDescriptor physical(ColumnDescriptor column) {
        return new SchemaDescriptor("bad", List.of(column), List.of());
    }

    private static SchemaDescriptor malformedMap(int keyDefinition, String repeatedName, int keyIndex) {
        ColumnDescriptor key = new ColumnDescriptor(Type.INT32,
                new String[]{"mapping", repeatedName, "key"}, keyDefinition, 1, 0);
        ColumnDescriptor value = new ColumnDescriptor(Type.INT64,
                new String[]{"mapping", repeatedName, "value"}, keyDefinition + 1, 1, 0);
        MapMetadata metadata = new MapMetadata(keyIndex, keyIndex + 1, Type.INT32, Type.INT64, key, value);
        return new SchemaDescriptor("bad", List.of(key, value), List.of(
                new LogicalColumnDescriptor("mapping", LogicalType.MAP, metadata)));
    }

    static Stream<Arguments> incompatibleRows() {
        return Stream.of(
                Arguments.of(scalar(Type.INT64, 0, 0), new Object[]{2147483648L}),
                Arguments.of(scalar(Type.INT64, 0, 0), new Object[]{42L}),
                Arguments.of(scalar(Type.INT32, 1, 0), new Object[]{42}),
                Arguments.of(scalar(Type.INT32, 0, 0), new Object[]{}),
                Arguments.of(scalar(Type.INT32, 0, 0), new Object[]{42, 43}));
    }

    @ParameterizedTest
    @MethodSource("incompatibleRows")
    void sameNamedIncompatibleRowIsRejectedBeforeOpening(SchemaDescriptor rowSchema, Object[] values)
            throws Exception {
        Path destination = directory.resolve("incompatible.parquet");
        byte[] sentinel = new byte[]{3, 3, 1};
        Files.write(destination, sentinel);
        int[] opens = {0};
        ParquetFileWriter writer = new ParquetFileWriter(destination, scalar(Type.INT32, 0, 0)) {
            @Override
            OutputStream openSink(Path path) throws IOException {
                opens[0]++;
                return super.openSink(path);
            }
        };
        assertThrows(IllegalArgumentException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(rowSchema, values)));
        assertEquals(0, opens[0]);
        writer.close();
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
    }

    static Stream<Arguments> invalidMapRows() {
        Map<Object, Object> nullKey = new LinkedHashMap<>();
        nullKey.put(null, 3L);
        Map<Object, Object> nullValue = new LinkedHashMap<>();
        nullValue.put(1, null);
        return Stream.of(
                Arguments.of(true, true, "not a map"),
                Arguments.of(true, true, new Object()),
                Arguments.of(false, true, null),
                Arguments.of(true, true, nullKey),
                Arguments.of(true, false, nullValue),
                Arguments.of(true, true, Map.of(2147483648L, 3L)),
                Arguments.of(true, true, Map.of(1, "3")),
                Arguments.of(true, true, Map.of(1, Map.of(2, 3L))));
    }

    @ParameterizedTest
    @MethodSource("invalidMapRows")
    void invalidMapIsRejectedBeforeOpening(boolean optional, boolean valuesOptional, Object map)
            throws Exception {
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("map_schema", List.of(
                SchemaDescriptor.createMapColumn("mapping", Type.INT32, Type.INT64, optional, valuesOptional)));
        Path destination = directory.resolve("invalid-map.parquet");
        byte[] sentinel = new byte[]{4, 4, 3};
        Files.write(destination, sentinel);
        int[] opens = {0};
        ParquetFileWriter writer = new ParquetFileWriter(destination, schema) {
            @Override
            OutputStream openSink(Path path) throws IOException {
                opens[0]++;
                return super.openSink(path);
            }
        };
        assertThrows(IllegalArgumentException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{map})));
        assertEquals(0, opens[0]);
        writer.close();
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
    }

    @Test
    void mapGetterFailurePropagatesInsteadOfBecomingNull() throws Exception {
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("map_schema", List.of(
                SchemaDescriptor.createMapColumn("mapping", Type.INT32, Type.INT64, true, true)));
        Path destination = directory.resolve("getter.parquet");
        IllegalStateException original = new IllegalStateException("injected getter failure");
        SimpleRowColumnGroup row = new SimpleRowColumnGroup(schema, new Object[]{Map.of(1, 3L)}) {
            @Override
            public Object getColumnValue(int index) {
                throw original;
            }

            @Override
            public Object getColumnValue(String name) {
                throw original;
            }
        };
        ParquetFileWriter writer = new ParquetFileWriter(destination, schema);
        assertSame(original, assertThrows(IllegalStateException.class, () -> writer.addRow(row)));
        writer.close();
        assertFalse(Files.exists(destination));
    }

    @Test
    void nullRowIsRejectedBeforeOpening() throws Exception {
        Path destination = directory.resolve("null-row.parquet");
        ParquetFileWriter writer = new ParquetFileWriter(destination, scalar(Type.INT32, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> writer.addRow(null));
        writer.close();
        assertFalse(Files.exists(destination));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedSchemas")
    void unsupportedWritableShapeIsRejectedBeforeCreatingAnyFile(String name,
                                                                 Supplier<SchemaDescriptor> schema) throws Exception {
        Path destination = directory.resolve("unsupported.parquet");
        byte[] sentinel = new byte[]{1, 7, 2};
        Files.write(destination, sentinel);
        assertThrows(IllegalArgumentException.class,
                () -> new ParquetFileWriter(destination, schema.get()), name);
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        try (var paths = Files.list(directory)) {
            assertEquals(List.of(destination), paths.toList());
        }
    }

    @ParameterizedTest
    @MethodSource("invalidPrimitiveValues")
    void invalidPrimitiveIsRejectedBeforeOpeningOrBuffering(Type type, int definition, Object value)
            throws Exception {
        SchemaDescriptor schema = scalar(type, definition, type == Type.FIXED_LEN_BYTE_ARRAY ? 2 : 0);
        Path destination = directory.resolve("sentinel.parquet");
        byte[] sentinel = new byte[]{4, 2, 1};
        Files.write(destination, sentinel);
        int[] opens = {0};
        ParquetFileWriter writer = new ParquetFileWriter(destination, schema) {
            @Override
            OutputStream openSink(Path path) throws IOException {
                opens[0]++;
                return super.openSink(path);
            }
        };
        assertThrows(IllegalArgumentException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value})));
        assertEquals(0, opens[0]);
        writer.close();
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        try (var paths = Files.list(directory)) {
            assertEquals(List.of(destination), paths.toList());
        }
    }
}
