package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilter;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilterSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Bounded batch decoding: batchSize/maxBatchBytes/limit bound how much is decoded and
 * retained per batch, nested values stay exact at batch seams, predicate-only columns join
 * the scan without surfacing in output rows, and every batched variant equals the unbounded
 * full scan.
 */
class BatchedScanTest {
    @TempDir
    Path tempDir;

    // ------------------------------------------------------------------ fixtures

    private static LogicalColumnDescriptor primitive(String name, Type type, boolean optional) {
        return new LogicalColumnDescriptor(name, LogicalType.PRIMITIVE, type,
                new ColumnDescriptor(type, new String[]{name}, optional ? 1 : 0, 0, 0));
    }

    private Path writeIntColumn(int rows, int pageSize) throws IOException {
        var id = primitive("id", Type.INT32, false);
        var schema = SchemaDescriptor.fromLogicalColumns("rows", List.of(id));
        Path file = tempDir.resolve("ints-" + pageSize + "-" + rows + ".parquet");
        try (var writer = new ParquetFileWriter(file, schema, CompressionCodec.UNCOMPRESSED,
                pageSize, 128 * 1024 * 1024)) {
            for (int i = 0; i < rows; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i}));
            }
        }
        return file;
    }

    private Path writeMapFile(int rows, int pageSize) throws IOException {
        var id = primitive("id", Type.INT32, false);
        var item = SchemaDescriptor.createStringMapColumn("item", true);
        var schema = SchemaDescriptor.fromLogicalColumns("rows", List.of(id, item));
        Path file = tempDir.resolve("maps-" + pageSize + "-" + rows + ".parquet");
        try (var writer = new ParquetFileWriter(file, schema, CompressionCodec.UNCOMPRESSED,
                pageSize, 128 * 1024 * 1024)) {
            for (int i = 0; i < rows; i++) {
                Map<String, String> entries = new LinkedHashMap<>();
                for (int entry = 0; entry < i % 7; entry++) {
                    entries.put("key-" + i + "-" + entry, "value-" + i + "-" + entry + "-"
                            + "x".repeat(entry * 11));
                }
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i, entries}));
            }
        }
        return file;
    }

    // ------------------------------------------------------------------ bounded decode

    @Test
    void smallLimitAndBudgetDoNotDecodeTheRemainingGroup() throws IOException {
        Path file = writeIntColumn(240, 64); // ~16 rows per data page: many pages per chunk

        // Reference: the full scan decodes every page of the chunk.
        Scan full = scan(file, ReadOptions.builder().build(), Long.MAX_VALUE);
        assertTrue(full.decodedPages >= 5, "fixture must span many pages, saw " + full.decodedPages);

        // A small limit must not decode or retain the remaining rows of the group.
        Scan limited = scan(file, ReadOptions.builder().limit(2).build(), Long.MAX_VALUE);
        assertEquals(List.of(0, 1), limited.values);
        assertEquals(2, limited.materializedRows, "limit(2) materializes exactly its two rows");
        assertTrue(limited.decodedPages <= 2,
                "limit(2) must not decode the remaining group, decoded " + limited.decodedPages
                        + " of " + full.decodedPages + " pages");

        // A small batch size bounds each decode step.
        try (ParquetFileReader reader = new ParquetFileReader(file);
             var rows = reader.rowIterator(ReadOptions.builder().batchSize(4).build())) {
            assertTrue(rows.hasNext());
            rows.next();
            rows.next();
            rows.next();
            assertEquals(4, rows.getMaterializedRowCount(), "batchSize(4) materializes 4 rows per batch");
            assertTrue(rows.getDecodedPageCount() <= 4,
                    "a batch decodes only the pages it needs, decoded " + rows.getDecodedPageCount());
        }

        // A tiny byte budget bounds each decode step to a single row.
        try (ParquetFileReader reader = new ParquetFileReader(file);
             var rows = reader.rowIterator(ReadOptions.builder().maxBatchBytes(1).build())) {
            for (int i = 0; i < 6; i++) {
                assertTrue(rows.hasNext());
                assertEquals(i, rows.next().getColumnValue(0));
                assertEquals(i + 1, rows.getMaterializedRowCount(),
                        "maxBatchBytes(1) materializes one row at a time");
            }
            assertTrue(rows.getDecodedPageCount() <= 6);
        }
    }

    // ------------------------------------------------------------------ nested seams

    @Test
    void nestedValuesAreExactAcrossBatchSeams() throws IOException {
        Path maps = writeMapFile(40, 64); // tiny pages: maps span several pages per row
        List<Object[]> expected = scan(maps, ReadOptions.builder().build(), Long.MAX_VALUE).valuesAsRows();

        for (int batchSize : new int[]{1, 2, 3, 5, 4096}) {
            List<Object[]> batched =
                    scan(maps, ReadOptions.builder().batchSize(batchSize).build(), Long.MAX_VALUE).valuesAsRows();
            assertRowsEqual(expected, batched, "map file batchSize=" + batchSize);
        }
        assertRowsEqual(expected,
                scan(maps, ReadOptions.builder().maxBatchBytes(1).build(), Long.MAX_VALUE).valuesAsRows(),
                "map file maxBatchBytes=1");

        // Real fixtures: lists and maps split exactly at every possible batch seam.
        for (String fixture : new String[]{"list_columns.parquet", "data_with_map_column.parquet"}) {
            Path file = Path.of("src/test/data", fixture);
            List<Object[]> unbounded = scan(file, ReadOptions.builder().build(), Long.MAX_VALUE).valuesAsRows();
            for (int batchSize : new int[]{1, 7, 5000}) {
                assertRowsEqual(unbounded,
                        scan(file, ReadOptions.builder().batchSize(batchSize).build(), Long.MAX_VALUE).valuesAsRows(),
                        fixture + " batchSize=" + batchSize);
            }
        }
    }

    // ------------------------------------------------------------------ predicate leaves

    @Test
    void filterColumnsOutsideTheProjectionAreReadButHidden() throws IOException {
        var keep = primitive("keep", Type.INT32, false);
        var hidden = primitive("hidden", Type.INT32, false);
        var schema = SchemaDescriptor.fromLogicalColumns("rows", List.of(keep, hidden));
        Path file = tempDir.resolve("hidden.parquet");
        try (var writer = new ParquetFileWriter(file, schema)) {
            for (int i = 0; i < 10; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i, i * 10}));
            }
        }

        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            LogicalColumnDescriptor hiddenColumn = reader.getSchema().getLogicalColumn("hidden");
            RowColumnGroupFilter filter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(hiddenColumn, 30));
            var chunkRanges = allChunkRanges(reader);
            long[] hiddenRanges = chunkRanges(reader, 1);

            CountingReader counted;
            try (FileChunkReader source = new FileChunkReader(file)) {
                counted = new CountingReader(source);
                try (ParquetFileReader instrumented = new ParquetFileReader(counted)) {
                    int metadataReads = counted.readCalls;
                    try (ParquetRowIterator rows = instrumented.rowIterator(
                            ReadOptions.builder().project("keep").filter(filter).build())) {
                        List<Object> kept = new ArrayList<>();
                        while (rows.hasNext()) {
                            RowColumnGroup row = rows.next();
                            assertEquals(1, row.getColumnCount(),
                                    "predicate-only columns must not surface in output rows");
                            assertEquals("keep", row.getSchema().getLogicalColumn(0).getName());
                            kept.add(row.getColumnValue(0));
                        }
                        assertEquals(List.of(3), kept, "residual filtering evaluates the full predicate");
                        assertEquals(1, rows.getSchema().getNumLogicalColumns());
                        assertEquals(0, rows.getDroppedRowGroupCount(),
                                "one group holds all rows and is not provably impossible");
                    }
                    assertTrue(counted.readCalls > metadataReads);
                    boolean sawHiddenChunk = false;
                    boolean sawKeepChunk = false;
                    for (long position : counted.positions) {
                        if (within(position, hiddenRanges)) {
                            sawHiddenChunk = true;
                        }
                        if (within(position, chunkRanges.get(0))) {
                            sawKeepChunk = true;
                        }
                    }
                    assertTrue(sawKeepChunk, "the projected chunk is read");
                    assertTrue(sawHiddenChunk, "the predicate-only chunk is read for filtering");
                }
            }

            // Equivalence with the unbounded full-scan filter result.
            RowColumnGroupFilter fullFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(reader.getSchema().getLogicalColumn("hidden"), 30));
            List<Object> expected = new ArrayList<>();
            try (ParquetRowIterator scan = reader.rowIterator(ReadOptions.builder().build())) {
                while (scan.hasNext()) {
                    RowColumnGroup row = scan.next();
                    if (fullFilter.apply(row)) {
                        expected.add(row.getColumnValue("keep"));
                    }
                }
            }
            assertEquals(List.of(3), expected);
        }
    }

    // ------------------------------------------------------------------ equivalence

    @Test
    void batchedResultsEqualTheUnboundedFullScan() throws IOException {
        Path maps = writeMapFile(25, 128);
        List<Object[]> unbounded = scan(maps, ReadOptions.builder().build(), Long.MAX_VALUE).valuesAsRows();
        assertEquals(25, unbounded.size());

        for (int batchSize : new int[]{1, 2, 3, 8, 25, 26, 1024}) {
            assertRowsEqual(unbounded,
                    scan(maps, ReadOptions.builder().batchSize(batchSize).build(), Long.MAX_VALUE).valuesAsRows(),
                    "batchSize=" + batchSize);
            assertRowsEqual(unbounded,
                    scan(maps, ReadOptions.builder().batchSize(batchSize).maxBatchBytes(1).build(),
                            Long.MAX_VALUE).valuesAsRows(),
                    "batchSize=" + batchSize + ", maxBatchBytes=1");
        }
        for (long limit = 0; limit <= 25; limit += 7) {
            List<Object[]> expected = unbounded.subList(0, (int) Math.min(limit, unbounded.size()));
            assertRowsEqual(expected,
                    scan(maps, ReadOptions.builder().limit(limit).batchSize(3).build(), Long.MAX_VALUE).valuesAsRows(),
                    "limit=" + limit);
        }
    }

    // ------------------------------------------------------------------ helpers

    private static final class Scan {
        final List<Object> values = new ArrayList<>();
        final List<Object[]> rows = new ArrayList<>();
        long materializedRows;
        int decodedPages;

        List<Object[]> valuesAsRows() {
            return rows;
        }
    }

    private Scan scan(Path file, ReadOptions options, long limit) throws IOException {
        Scan result = new Scan();
        try (ParquetFileReader reader = new ParquetFileReader(file);
             var iterator = reader.rowIterator(options)) {
            while (iterator.hasNext()) {
                RowColumnGroup row = iterator.next();
                result.rows.add(values(row));
                if (!result.rows.isEmpty()) {
                    result.values.add(row.getColumnValue(0));
                }
                if (result.rows.size() >= limit) {
                    break;
                }
            }
            result.materializedRows = iterator.getMaterializedRowCount();
            result.decodedPages = iterator.getDecodedPageCount();
        }
        return result;
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

    private static List<long[]> allChunkRanges(ParquetFileReader reader) {
        List<long[]> ranges = new ArrayList<>();
        for (var group : reader.getMetadata().rowGroups()) {
            for (var chunk : group.columns()) {
                ranges.add(new long[]{chunk.getFirstDataPageOffset(),
                        chunk.getFirstDataPageOffset() + chunk.totalCompressedSize()});
            }
        }
        return ranges;
    }

    private static long[] chunkRanges(ParquetFileReader reader, int column) {
        List<long[]> ranges = new ArrayList<>();
        for (var group : reader.getMetadata().rowGroups()) {
            var chunk = group.columns().get(column);
            ranges.add(new long[]{chunk.getFirstDataPageOffset(),
                    chunk.getFirstDataPageOffset() + chunk.totalCompressedSize()});
        }
        long[] out = new long[ranges.size() * 2];
        for (int i = 0; i < ranges.size(); i++) {
            out[2 * i] = ranges.get(i)[0];
            out[2 * i + 1] = ranges.get(i)[1];
        }
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
            return source.readBytes(position, length);
        }
    }
}
