package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;
import io.github.aloksingh.parquet.util.filter.ColumnEqualFilter;
import io.github.aloksingh.parquet.util.filter.ColumnGreaterThanFilter;
import io.github.aloksingh.parquet.util.filter.ColumnGreaterThanOrEqualFilter;
import io.github.aloksingh.parquet.util.filter.ColumnIsNotNullFilter;
import io.github.aloksingh.parquet.util.filter.ColumnIsNullFilter;
import io.github.aloksingh.parquet.util.filter.ColumnLessThanFilter;
import io.github.aloksingh.parquet.util.filter.ColumnLessThanOrEqualFilter;
import io.github.aloksingh.parquet.util.filter.ColumnNotEqualFilter;
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilter;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilterSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Conservative row-group pruning wired into the row-group loading path: a group whose
 * statistics prove that no row can match is skipped before any of its chunks is read, and
 * pruning never changes the result.
 */
class RowGroupPruningTest {
    @TempDir
    Path tempDir;

    // ------------------------------------------------------------------ fixtures

    private static LogicalColumnDescriptor primitive(String name, Type type, boolean optional) {
        return new LogicalColumnDescriptor(name, LogicalType.PRIMITIVE, type,
                new ColumnDescriptor(type, new String[]{name}, optional ? 1 : 0, 0, 0));
    }

    private static SchemaDescriptor schemaOf(LogicalColumnDescriptor... columns) {
        return SchemaDescriptor.fromLogicalColumns("rows", List.of(columns));
    }

    private static SimpleRowColumnGroup row(SchemaDescriptor schema, Object... values) {
        return new SimpleRowColumnGroup(schema, values);
    }

    /**
     * Writes one single-row row group per value so each group's statistics are exact.
     */
    private Path writePerValueGroups(SchemaDescriptor schema, Object[] values) throws IOException {
        Path file = tempDir.resolve("groups-" + System.nanoTime() + ".parquet");
        try (var writer = new ParquetFileWriter(file, schema, CompressionCodec.UNCOMPRESSED,
                1024 * 1024, 1)) {
            for (Object value : values) {
                writer.addRow(row(schema, value));
            }
        }
        return file;
    }

    private static Object[] boxed(int... values) {
        Object[] out = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            out[i] = values[i];
        }
        return out;
    }

    // ------------------------------------------------------------------ (a) zero chunk reads

    @Test
    void droppedRowGroupPerformsZeroChunkReadsAndIsCounted() throws IOException {
        var id = primitive("id", Type.INT32, false);
        var schema = schemaOf(id);
        // Group 1 (id=5000) has min=max=5000: 'id < 100' proves it cannot match.
        Path file = writePerValueGroups(schema, boxed(10, 5000, 20));
        RowColumnGroupFilterSet filter =
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnLessThanFilter(id, 100));

        try (FileChunkReader source = new FileChunkReader(file)) {
            CountingReader counted = new CountingReader(source);
            try (ParquetFileReader reader = new ParquetFileReader(counted)) {
                assertEquals(3, reader.getMetadata().getNumRowGroups());
                long[] droppedRanges = chunkRanges(reader, 1);
                long[] keptRanges = concat(chunkRanges(reader, 0), chunkRanges(reader, 2));
                int metadataReads = counted.readCalls;

                try (ParquetRowIterator rows = reader.rowIterator(
                        ReadOptions.builder().filter(filter).build())) {
                    assertEquals(0, rows.getDroppedRowGroupCount(), "no group is dropped before iteration");
                    List<Object> ids = new ArrayList<>();
                    while (rows.hasNext()) {
                        ids.add(rows.next().getColumnValue(0));
                    }
                    assertEquals(List.of(10, 20), ids);
                    assertEquals(1, rows.getDroppedRowGroupCount(), "the impossible group is counted");
                }
                assertTrue(counted.readCalls > metadataReads, "kept groups are read");
                for (long position : counted.positions.subList(metadataReads, counted.positions.size())) {
                    assertFalse(within(position, droppedRanges),
                            "a dropped row group must perform ZERO chunk reads, saw read at " + position);
                    assertTrue(within(position, keptRanges),
                            "reads must stay inside kept row groups, saw read at " + position);
                }
                assertEquals(0, counted.bytesIn(droppedRanges, metadataReads),
                        "a dropped row group must contribute zero bytes read");
            }
        }
    }

    @Test
    void filteringIteratorHonorsPruningThroughTheSameLoadingPath() throws IOException {
        var id = primitive("id", Type.INT32, false);
        var schema = schemaOf(id);
        Path file = writePerValueGroups(schema, boxed(10, 5000, 20));
        RowColumnGroupFilterSet filter =
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnLessThanFilter(id, 100));

        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            ParquetRowIterator base = new ParquetRowIterator(reader, false);
            try (var iterator = new FilteringParquetRowIterator(base, filter)) {
                List<Object> ids = new ArrayList<>();
                while (iterator.hasNext()) {
                    ids.add(iterator.next().getColumnValue(0));
                }
                assertEquals(List.of(10, 20), ids);
            }
            assertEquals(1, base.getDroppedRowGroupCount(),
                    "the filtering wrapper drives pruning through the delegate's loading path");
        }
    }

    // ------------------------------------------------------------------ (b) equivalence

    @Test
    void pruningNeverChangesFilteredResults() throws IOException {
        var v = primitive("v", Type.INT32, true);
        var schemaA = schemaOf(v);
        Object[] valuesA = {null, 1, 2, 3, 10, 20, 30, 5, null, 15, 25, 35};
        Path fileA = writePerValueGroups(schemaA, valuesA);

        var d = primitive("d", Type.DOUBLE, true);
        var schemaB = schemaOf(d);
        Object[] valuesB = {1.0, Double.NaN, 2.5, Double.NaN, 10.5};
        Path fileB = writePerValueGroups(schemaB, valuesB);

        List<RowColumnGroupFilter> filtersA = List.of(
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnEqualFilter(v, 10)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnEqualFilter(v, 11)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnEqualFilter(v, 35)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnEqualFilter(v, null)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnNotEqualFilter(v, 10)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnIsNullFilter(v)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnIsNotNullFilter(v)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnLessThanFilter(v, 10)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnLessThanOrEqualFilter(v, 10)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnGreaterThanFilter(v, 30)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnGreaterThanOrEqualFilter(v, 35)),
                new RowColumnGroupFilterSet(FilterJoinType.All,
                        new ColumnGreaterThanOrEqualFilter(v, 10), new ColumnLessThanFilter(v, 30)),
                new RowColumnGroupFilterSet(FilterJoinType.Any,
                        new ColumnLessThanFilter(v, 2), new ColumnGreaterThanFilter(v, 30)),
                new RowColumnGroupFilterSet(FilterJoinType.Any,
                        new ColumnEqualFilter(v, 11), new ColumnEqualFilter(v, 999)));

        List<RowColumnGroupFilter> filtersB = List.of(
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnEqualFilter(d, 2.5)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnEqualFilter(d, 3.5)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnIsNullFilter(d)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnIsNotNullFilter(d)),
                new RowColumnGroupFilterSet(FilterJoinType.All, new ColumnLessThanFilter(d, 2.5)),
                new RowColumnGroupFilterSet(FilterJoinType.Any,
                        new ColumnEqualFilter(d, 10.5), new ColumnEqualFilter(d, 1.0)));

        assertPruningEquivalence(fileA, schemaA, filtersA);
        assertPruningEquivalence(fileB, schemaB, filtersB);
    }

    /**
     * Filtered results must be identical with pruning enabled and disabled, and sound.
     */
    private void assertPruningEquivalence(Path file, SchemaDescriptor schema,
                                          List<RowColumnGroupFilter> filters) throws IOException {
        for (RowColumnGroupFilter filter : filters) {
            List<Object[]> expected;
            try (ParquetFileReader reader = new ParquetFileReader(file)) {
                expected = new ArrayList<>();
                try (ParquetRowIterator scan = reader.rowIterator(ReadOptions.builder().build())) {
                    while (scan.hasNext()) {
                        RowColumnGroup sourceRow = scan.next();
                        if (filter.apply(sourceRow)) {
                            expected.add(values(sourceRow));
                        }
                    }
                }
            }
            List<Object[]> withPruning = filterScan(file, filter, true);
            List<Object[]> withoutPruning = filterScan(file, filter, false);
            assertRowsEqual(expected, withPruning, filter.expression() + " (pruning on)");
            assertRowsEqual(expected, withoutPruning, filter.expression() + " (pruning off)");
        }
    }

    private List<Object[]> filterScan(Path file, RowColumnGroupFilter filter, boolean pruning)
            throws IOException {
        List<Object[]> rows = new ArrayList<>();
        try (ParquetFileReader reader = new ParquetFileReader(file);
             var iterator = reader.rowIterator(
                     ReadOptions.builder().filter(filter).pruning(pruning).build())) {
            while (iterator.hasNext()) {
                rows.add(values(iterator.next()));
            }
        }
        return rows;
    }

    private static Object[] values(RowColumnGroup row) {
        Object[] values = new Object[row.getColumnCount()];
        for (int i = 0; i < values.length; i++) {
            values[i] = row.getColumnValue(i);
        }
        return values;
    }

    private static void assertRowsEqual(List<Object[]> expected, List<Object[]> actual,
                                        String label) {
        assertEquals(expected.size(), actual.size(), label + ": row count");
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i).length, actual.get(i).length, label + ": row " + i + " width");
            for (int column = 0; column < expected.get(i).length; column++) {
                assertEquals(expected.get(i)[column], actual.get(i)[column],
                        label + ": row " + i + " column " + column);
            }
        }
    }

    // ------------------------------------------------------------------ instrumentation

    @Test
    void droppedGroupCountStaysZeroWithoutAFilter() throws IOException {
        var id = primitive("id", Type.INT32, false);
        var schema = schemaOf(id);
        Path file = writePerValueGroups(schema, boxed(10, 5000, 20));
        try (ParquetFileReader reader = new ParquetFileReader(file);
             ParquetRowIterator rows = reader.rowIterator(ReadOptions.builder().build())) {
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            assertEquals(3, count, "without a filter nothing is pruned");
            assertEquals(0, rows.getDroppedRowGroupCount());
        }
    }

    // ------------------------------------------------------------------ helpers

    private static long[] chunkRanges(ParquetFileReader reader, int rowGroup) {
        ParquetMetadata.RowGroupMetadata group = reader.getMetadata().rowGroups().get(rowGroup);
        long[] ranges = new long[group.columns().size() * 2];
        for (int i = 0; i < group.columns().size(); i++) {
            ParquetMetadata.ColumnChunkMetadata chunk = group.columns().get(i);
            ranges[2 * i] = chunk.getFirstDataPageOffset();
            ranges[2 * i + 1] = chunk.getFirstDataPageOffset() + chunk.totalCompressedSize();
        }
        return ranges;
    }

    private static long[] concat(long[] first, long[] second) {
        long[] out = new long[first.length + second.length];
        System.arraycopy(first, 0, out, 0, first.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }

    private static boolean within(long position, long[] ranges) {
        for (int i = 0; i < ranges.length; i += 2) {
            if (position >= ranges[i] && position < ranges[i + 1]) {
                return true;
            }
        }
        return false;
    }

    private static final class CountingReader implements ChunkReader {
        private final ChunkReader source;
        private final List<Long> positions = new ArrayList<>();
        private final List<Integer> lengths = new ArrayList<>();
        private int readCalls;

        private CountingReader(ChunkReader source) {
            this.source = source;
        }

        @Override
        public long length() throws IOException {
            return source.length();
        }

        @Override
        public ByteBuffer readBytes(long position, int length) throws IOException {
            readCalls++;
            positions.add(position);
            lengths.add(length);
            return source.readBytes(position, length);
        }

        private long bytesIn(long[] ranges, int fromRead) {
            long total = 0;
            for (int i = fromRead; i < positions.size(); i++) {
                if (within(positions.get(i), ranges)) {
                    total += lengths.get(i);
                }
            }
            return total;
        }
    }
}
