package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.sql.DriverManager;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class WriterEncodingTest {
    @TempDir
    Path directory;

    static Stream<Arguments> booleanPages() {
        return Stream.of(Arguments.of(8, new byte[]{0x4d}), Arguments.of(9, new byte[]{0x4d, 0x01}));
    }

    @ParameterizedTest
    @MethodSource("booleanPages")
    void plainBooleanUsesExactLeastSignificantBitPackedBytes(int count, byte[] expected) throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.BOOLEAN, 0, 0);
        Path destination = directory.resolve("booleans.parquet");
        boolean[] values = {true, false, true, true, false, false, true, false, true};
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema)) {
            for (int i = 0; i < count; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{values[i]}));
            }
        }
        assertArrayEquals(expected, WriterTestSupport.firstPageValues(destination, 0, 0, 0));
        assertEquals(count, WriterTestSupport.footer(destination).getNum_rows());
    }

    @Test
    void primitiveAfterMapUsesItsLogicalIndexRatherThanPhysicalIndex() throws Exception {
        ColumnDescriptor before = new ColumnDescriptor(Type.INT32, new String[]{"before"}, 0, 0, 0);
        ColumnDescriptor after = new ColumnDescriptor(Type.INT64, new String[]{"after"}, 0, 0, 0);
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("mapping", List.of(
                new LogicalColumnDescriptor("before", LogicalType.PRIMITIVE, Type.INT32, before),
                SchemaDescriptor.createMapColumn("map", Type.BYTE_ARRAY, Type.INT32, true, true),
                new LogicalColumnDescriptor("after", LogicalType.PRIMITIVE, Type.INT64, after)));
        Path destination = directory.resolve("logical-index.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{1, Map.of("x", 7), 10001L}));
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{2, null, 10002L}));
        }
        byte[] expected = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
                .putLong(10001L).putLong(10002L).array();
        assertArrayEquals(expected, WriterTestSupport.firstPageValues(destination, 3, 0, 0));
        // V2 MAP support in the local reader is a separate implementation workstream.
        // An independent engine verifies every field rather than mirroring writer internals.
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.prepareStatement(
                     "SELECT before, after, map['x'] FROM read_parquet(?) ORDER BY before")) {
            statement.setString(1, destination.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals(1, rows.getInt(1));
                assertEquals(10001L, rows.getLong(2));
                assertEquals(7, rows.getInt(3));
                assertFalse(rows.wasNull());
                assertTrue(rows.next());
                assertEquals(2, rows.getInt(1));
                assertEquals(10002L, rows.getLong(2));
                assertNull(rows.getObject(3));
                assertFalse(rows.next());
            }
        }
    }
}
