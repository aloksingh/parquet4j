package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FilteringIteratorFailureTest {
    @TempDir
    Path tempDir;

    private Path fixture() throws IOException {
        var id = new LogicalColumnDescriptor("id", LogicalType.PRIMITIVE, Type.INT32,
                new ColumnDescriptor(Type.INT32, new String[]{"id"}, 0, 0, 0));
        var schema = SchemaDescriptor.fromLogicalColumns("rows", List.of(id));
        Path file = tempDir.resolve("rows.parquet");
        try (var writer = new ParquetFileWriter(file, schema)) {
            for (int value : new int[]{1, 2, 3, 4, 5, 6}) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
            }
        }
        return file;
    }

    @Test
    void invalidBoundTargetFailsDuringConstructionWithoutAdvancingTheDelegate() throws IOException {
        var missing = new LogicalColumnDescriptor("missing", LogicalType.PRIMITIVE, Type.INT32,
                new ColumnDescriptor(Type.INT32, new String[]{"missing"}, 0, 0, 0));
        try (var reader = new ParquetFileReader(fixture()); var base = new CountingDelegate(reader)) {
            assertThrows(IllegalArgumentException.class, () -> new FilteringParquetRowIterator(base,
                    new io.github.aloksingh.parquet.util.filter.ColumnEqualFilter(missing, 1)));
            assertEquals(0, base.nextCalls);
            assertEquals(0, base.hasNextCalls);
        }
    }

    @Test
    void constructionIsLazyAndPredicateNoSuchElementFailureIsStickyNotEndOfFile() throws IOException {
        AtomicInteger evaluated = new AtomicInteger();
        var failure = new NoSuchElementException("predicate failure, not EOF");
        try (var reader = new ParquetFileReader(fixture());
             var base = new CountingDelegate(reader)) {
            RowColumnGroupFilter predicate = row -> {
                evaluated.incrementAndGet();
                throw failure;
            };
            var iterator = assertDoesNotThrow(() -> new FilteringParquetRowIterator(base, predicate));
            assertEquals(0, evaluated.get());
            assertEquals(0, base.hasNextCalls);
            assertEquals(0, base.nextCalls);
            var wrapped = assertThrows(ParquetException.class, iterator::hasNext);
            assertSame(failure, wrapped.getCause(), "the predicate failure stays the cause");
            assertTrue(wrapped.getMessage().contains(predicate.expression()),
                    "context must name the predicate expression but was: " + wrapped.getMessage());
            assertTrue(wrapped.getMessage().contains("at row 0"),
                    "context must name the row position but was: " + wrapped.getMessage());
            var second = assertThrows(ParquetException.class, iterator::hasNext);
            assertSame(wrapped, second, "the contextual failure is sticky and rethrown identically");
            var third = assertThrows(ParquetException.class, iterator::next);
            assertSame(wrapped, third);
            var fourth = assertThrows(ParquetException.class, iterator::getMatchingRowCount);
            assertSame(wrapped, fourth);
            assertEquals(1, evaluated.get(), "a failed predicate must not advance to later rows");
            assertEquals(1, base.nextCalls);
        }
    }

    @Test
    void delegateReadFailurePropagatesWithoutClosingAnUnownedReader() throws IOException {
        var failure = new UncheckedIOException(new IOException("read failure"));
        try (var reader = new ParquetFileReader(fixture())) {
            var base = new CountingDelegate(reader) {
                @Override
                public boolean hasNext() {
                    hasNextCalls++;
                    throw failure;
                }
            };
            var iterator = new FilteringParquetRowIterator(base, (RowColumnGroupFilter) null);
            assertEquals(0, base.hasNextCalls);
            assertSame(failure, assertThrows(UncheckedIOException.class, iterator::hasNext));
            assertSame(failure, assertThrows(UncheckedIOException.class, iterator::next));
            assertEquals(1, base.hasNextCalls);
            iterator.close();
            assertEquals(1, base.closeCalls);
            assertDoesNotThrow(() -> reader.getRowGroup(0).readColumn(0), "delegate owns the close policy");
        }
    }

    @Test
    void delegateNextFailureAfterTrueHasNextIsNotTreatedAsExhaustion() throws IOException {
        var failure = new NoSuchElementException("delegate is broken");
        try (var reader = new ParquetFileReader(fixture())) {
            var base = new CountingDelegate(reader) {
                @Override
                public boolean hasNext() {
                    hasNextCalls++;
                    return true;
                }

                @Override
                public RowColumnGroup next() {
                    nextCalls++;
                    throw failure;
                }
            };
            try (var iterator = new FilteringParquetRowIterator(base, (RowColumnGroupFilter) null)) {
                assertEquals(0, base.nextCalls);
                assertSame(failure, assertThrows(NoSuchElementException.class, iterator::hasNext));
                assertSame(failure, assertThrows(NoSuchElementException.class, iterator::hasNext));
                assertSame(failure, assertThrows(NoSuchElementException.class, iterator::next));
                assertEquals(1, base.nextCalls);
                assertEquals(1, base.hasNextCalls);
            }
            assertEquals(1, base.closeCalls);
        }
    }

    @Test
    void oneLookaheadIsCachedAndExactRowsAndRemainingCountAreReturned() throws IOException {
        AtomicInteger evaluated = new AtomicInteger();
        try (var reader = new ParquetFileReader(fixture())) {
            var base = new CountingDelegate(reader);
            RowColumnGroupFilter predicate = row -> {
                evaluated.incrementAndGet();
                return (Integer) row.getColumnValue("id") % 2 == 0;
            };
            try (var iterator = new FilteringParquetRowIterator(base, predicate)) {
                assertEquals(0, evaluated.get());
                assertTrue(iterator.hasNext());
                assertEquals(2, evaluated.get());
                assertTrue(iterator.hasNext());
                assertTrue(iterator.hasNext());
                assertEquals(2, evaluated.get());
                var ids = new ArrayList<Integer>();
                while (iterator.hasNext()) ids.add((Integer) iterator.next().getColumnValue("id"));
                assertEquals(List.of(2, 4, 6), ids);
                assertEquals(6, evaluated.get(), "each source row evaluates the predicate exactly once");
                assertEquals(6, iterator.getTotalRowCount());
                assertEquals(0, iterator.getMatchingRowCount());
                assertFalse(iterator.hasNext());
                assertThrows(NoSuchElementException.class, iterator::next);
            }
            assertEquals(1, base.closeCalls);
        }
        try (var reader = new ParquetFileReader(tempDir.resolve("rows.parquet"));
             var iterator = new FilteringParquetRowIterator(new ParquetRowIterator(reader, false),
                     (RowColumnGroupFilter) row -> (Integer) row.getColumnValue("id") % 2 == 0)) {
            assertEquals(2, iterator.next().getColumnValue("id"));
            assertEquals(2, iterator.getMatchingRowCount());
            assertFalse(iterator.hasNext());
        }
    }

    private static class CountingDelegate extends ParquetRowIterator {
        int hasNextCalls;
        int nextCalls;
        int closeCalls;

        CountingDelegate(ParquetFileReader reader) {
            super(reader, false);
        }

        @Override
        public boolean hasNext() {
            hasNextCalls++;
            return super.hasNext();
        }

        @Override
        public RowColumnGroup next() {
            nextCalls++;
            return super.next();
        }

        @Override
        public void close() throws IOException {
            closeCalls++;
            super.close();
        }
    }
}
