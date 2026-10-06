package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class ReaderStreamingTest {
    private static final Path FIXTURE = Path.of("src/test/data/alltypes_plain.parquet");

    @Test
    void iteratorConstructionDoesNotReadColumnData() throws Exception {
        try (FileChunkReader source = new FileChunkReader(FIXTURE)) {
            CountingReader counted = new CountingReader(source);
            try (ParquetFileReader reader = new ParquetFileReader(counted)) {
                int metadataReads = counted.readCalls;
                try (ParquetRowIterator rows = new ParquetRowIterator(reader, false)) {
                    assertEquals(metadataReads, counted.readCalls,
                            "Constructing a row iterator must not load its first row group");
                    assertTrue(rows.hasNext());
                    assertTrue(counted.readCalls > metadataReads);
                    long count = 0;
                    while (rows.hasNext()) {
                        rows.next();
                        count++;
                    }
                    assertEquals(reader.getTotalRowCount(), count);
                }
            }
        }
    }

    @Test
    void projectionReadsOnlyTheSelectedPhysicalChunk() throws Exception {
        try (FileChunkReader source = new FileChunkReader(FIXTURE)) {
            CountingReader counted = new CountingReader(source);
            try (ParquetFileReader reader = new ParquetFileReader(counted)) {
                String name = reader.getSchema().getLogicalColumn(0).getName();
                java.util.List<Integer> expected = reader.getRowGroup(0).readColumn(0).decodeAsInt32();
                var selected = reader.getMetadata().rowGroups().get(0).columns().get(0);
                long start = selected.getFirstDataPageOffset();
                long end = start + selected.totalCompressedSize();
                int marker = counted.positions.size();
                Object options = projectedOptions(name);
                try (ParquetRowIterator rows = projectedIterator(reader, options)) {
                    assertEquals(marker, counted.positions.size());
                    java.util.List<Object> actual = new java.util.ArrayList<>();
                    while (rows.hasNext()) {
                        var row = rows.next();
                        assertEquals(1, row.getColumnCount());
                        assertEquals(name, row.getSchema().getLogicalColumn(0).getName());
                        actual.add(row.getColumnValue(0));
                    }
                    assertEquals(expected, actual);
                    assertTrue(counted.positions.size() > marker);
                    for (long position : counted.positions.subList(marker, counted.positions.size())) {
                        assertTrue(position >= start && position < end,
                                "Projection must not request another column chunk: " + position);
                    }
                }
            }
        }
    }

    @Test
    void zeroLimitDoesNotLoadAnyColumnPages() throws Exception {
        try (FileChunkReader source = new FileChunkReader(FIXTURE)) {
            CountingReader counted = new CountingReader(source);
            try (ParquetFileReader reader = new ParquetFileReader(counted)) {
                int metadataReads = counted.readCalls;
                try (ParquetRowIterator rows = reader.rowIterator(ReadOptions.builder().limit(0).build())) {
                    org.junit.jupiter.api.Assertions.assertFalse(rows.hasNext());
                    assertEquals(metadataReads, counted.readCalls);
                }
            }
        }
    }

    private static Object projectedOptions(String name) {
        try {
            Class<?> optionsClass = Class.forName("io.github.aloksingh.parquet.ReadOptions");
            Object builder = optionsClass.getMethod("builder").invoke(null);
            builder.getClass().getMethod("project", String[].class).invoke(builder, (Object) new String[]{name});
            return builder.getClass().getMethod("build").invoke(builder);
        } catch (ReflectiveOperationException e) {
            return org.junit.jupiter.api.Assertions.fail("The projected reader API is missing", e);
        }
    }

    private static ParquetRowIterator projectedIterator(ParquetFileReader reader, Object options) {
        try {
            return (ParquetRowIterator) ParquetFileReader.class.getMethod("rowIterator", options.getClass()).invoke(reader, options);
        } catch (ReflectiveOperationException e) {
            return org.junit.jupiter.api.Assertions.fail("The projected row iterator API is missing", e);
        }
    }

    private static final class CountingReader implements ChunkReader {
        private final ChunkReader source;
        private final java.util.List<Long> positions = new java.util.ArrayList<>();
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
