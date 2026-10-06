package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class WriterTransactionalTest {
    @TempDir
    Path directory;

    static SchemaDescriptor intSchema(String name) {
        ColumnDescriptor column = new ColumnDescriptor(Type.INT32, new String[]{"id"}, 0, 0, 0);
        return SchemaDescriptor.fromLogicalColumns(name, List.of(
                new LogicalColumnDescriptor("id", LogicalType.PRIMITIVE, Type.INT32, column)));
    }

    enum ErrorPoint {WRITE, CLOSE, GETTER}

    @ParameterizedTest
    @EnumSource(ErrorPoint.class)
    void fatalErrorsAbortWithoutAnyImplicitWriteOrCloseRetry(ErrorPoint point) throws Exception {
        Path destination = directory.resolve("fatal-error.parquet");
        byte[] sentinel = {8, 6, 8};
        Files.write(destination, sentinel);
        int[] writes = {0};
        int[] closes = {0};
        AssertionError injected = new AssertionError("injected " + point);
        var schema = intSchema("fatal");
        var writer = new ParquetFileWriter(destination, schema, CompressionCodec.UNCOMPRESSED, 64, 4) {
            @Override
            OutputStream openSink(Path path) throws IOException {
                return new FilterOutputStream(super.openSink(path)) {
                    @Override
                    public void write(byte[] bytes, int offset, int length) throws IOException {
                        writes[0]++;
                        if (point == ErrorPoint.WRITE && writes[0] > 1) throw injected;
                        out.write(bytes, offset, length);
                    }

                    @Override
                    public void close() throws IOException {
                        closes[0]++;
                        out.close();
                        if (point == ErrorPoint.CLOSE) throw injected;
                    }
                };
            }
        };
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{1}));
        AssertionError failure = assertThrows(AssertionError.class, () -> {
            if (point == ErrorPoint.CLOSE) writer.close();
            else if (point == ErrorPoint.WRITE) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{2}));
            else writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{2}) {
                    @Override
                    public Object getColumnValue(int index) {
                        throw injected;
                    }
                });
        });
        assertSame(injected, failure);
        int failedAt = writes[0];
        assertDoesNotThrow(writer::close);
        assertDoesNotThrow(writer::close);
        assertEquals(failedAt, writes[0]);
        assertEquals(1, closes[0]);
        assertThrows(IllegalStateException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{3})));
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        try (var files = Files.list(directory)) {
            assertEquals(List.of(destination), files.toList());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cleanupFailureDoesNotReplaceTheOriginalWriteFailure(boolean sameException) throws Exception {
        Path destination = directory.resolve("cleanup-failure.parquet");
        byte[] sentinel = {3, 8, 3};
        Files.write(destination, sentinel);
        IOException original = new IOException("original write failure");
        IOException cleanup = sameException ? original : new IOException("cleanup close failure");
        var schema = intSchema("cleanup");
        int[] writes = {0};
        int[] closes = {0};
        var writer = new ParquetFileWriter(destination, schema, CompressionCodec.UNCOMPRESSED, 64, 4) {
            @Override
            OutputStream openSink(Path path) throws IOException {
                return new FilterOutputStream(super.openSink(path)) {
                    @Override
                    public void write(byte[] bytes, int offset, int length) throws IOException {
                        if (++writes[0] > 1) throw original;
                        out.write(bytes, offset, length);
                    }

                    @Override
                    public void close() throws IOException {
                        closes[0]++;
                        out.close();
                        throw cleanup;
                    }
                };
            }
        };
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{1}));
        ParquetException failure = assertThrows(ParquetException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{2})));
        assertSame(original, failure.getCause());
        assertEquals(sameException ? 0 : 1, original.getSuppressed().length);
        if (!sameException) assertSame(cleanup, original.getSuppressed()[0]);
        int failedAt = writes[0];
        assertDoesNotThrow(writer::close);
        assertEquals(failedAt, writes[0]);
        assertEquals(1, closes[0]);
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        try (var files = Files.list(directory)) {
            assertEquals(List.of(destination), files.toList());
        }
    }

    @Test
    void partialPageWriteFailureIsTerminalAndCloseDoesNotRetryFlush() throws Exception {
        Path destination = directory.resolve("partial.parquet");
        byte[] sentinel = new byte[]{11, 12, 13};
        Files.write(destination, sentinel);
        int[] writeCalls = {0};
        int[] closeCalls = {0};
        SchemaDescriptor schema = intSchema("data");
        ParquetFileWriter writer = new ParquetFileWriter(destination, schema,
                CompressionCodec.UNCOMPRESSED, 1024, 4) {
            @Override
            OutputStream openSink(Path path) throws IOException {
                return new FilterOutputStream(super.openSink(path)) {
                    private int written;

                    @Override
                    public void write(byte[] bytes, int offset, int length) throws IOException {
                        writeCalls[0]++;
                        int accepted = Math.min(length, Math.max(0, 12 - written));
                        out.write(bytes, offset, accepted);
                        written += accepted;
                        if (accepted < length) throw new IOException("injected partial page write");
                    }

                    @Override
                    public void close() throws IOException {
                        closeCalls[0]++;
                        out.close();
                    }
                };
            }
        };
        ParquetException failure = assertThrows(ParquetException.class, () -> {
            for (int i = 0; i < 1000; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i}));
            }
        });
        assertEquals("injected partial page write", failure.getCause().getMessage());
        int failedAt = writeCalls[0];
        assertDoesNotThrow(writer::close);
        writer.close();
        assertEquals(failedAt, writeCalls[0], "Close must abort, not retry a failed flush");
        assertEquals(1, closeCalls[0]);
        assertThrows(IllegalStateException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{99})));
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        try (var files = Files.list(directory)) {
            assertEquals(List.of(destination), files.toList());
        }
    }

    @Test
    void sinkCloseFailureAbortsWithoutRetryingCloseOrPublishing() throws Exception {
        Path destination = directory.resolve("close-failure.parquet");
        byte[] sentinel = new byte[]{9, 8, 7};
        Files.write(destination, sentinel);
        int[] closeCalls = {0};
        SchemaDescriptor schema = intSchema("data");
        ParquetFileWriter writer = new ParquetFileWriter(destination, schema) {
            @Override
            OutputStream openSink(Path path) throws IOException {
                return new FilterOutputStream(super.openSink(path)) {
                    @Override
                    public void close() throws IOException {
                        closeCalls[0]++;
                        super.close();
                        throw new IOException("injected close failure");
                    }
                };
            }
        };
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{17}));
        IOException failure = assertThrows(IOException.class, writer::close);
        assertEquals("injected close failure", failure.getMessage());
        assertEquals(1, closeCalls[0], "A failed close must not be retried implicitly");
        writer.close();
        assertEquals(1, closeCalls[0]);
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        try (var files = Files.list(directory)) {
            assertEquals(List.of(destination), files.toList());
        }
    }

    @Test
    void validRowsArePublishedOnlyAfterSuccessfulIdempotentClose() throws Exception {
        Path destination = directory.resolve("published.parquet");
        byte[] sentinel = new byte[]{2, 4, 6};
        Files.write(destination, sentinel);
        SchemaDescriptor schema = intSchema("data");
        ParquetFileWriter writer = new ParquetFileWriter(destination, schema);
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{42}));
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        writer.close();
        byte[] published = Files.readAllBytes(destination);
        try (ParquetFileReader reader = new ParquetFileReader(destination)) {
            assertEquals(1, reader.getMetadata().fileMetadata().numRows());
            assertEquals(42, reader.rowIterator().next().getColumnValue(0));
        }
        writer.close();
        assertArrayEquals(published, Files.readAllBytes(destination));
        assertThrows(IllegalStateException.class, writer::start);
        assertThrows(IllegalStateException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{43})));
        try (var files = Files.list(directory)) {
            assertEquals(List.of(destination), files.toList());
        }
    }

    @Test
    void rejectedFirstRowPreservesExistingDestinationEvenOnClose() throws Exception {
        Path destination = directory.resolve("existing.parquet");
        byte[] sentinel = new byte[]{7, 3, 9, 1};
        Files.write(destination, sentinel);
        ParquetFileWriter writer = new ParquetFileWriter(destination, intSchema("expected"));
        assertThrows(IllegalArgumentException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(intSchema("other"), new Object[]{42})));
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        writer.close();
        writer.close();
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        try (var files = Files.list(directory)) {
            assertEquals(List.of(destination), files.toList(), "Rejected input must not leave staging files");
        }
    }
}
