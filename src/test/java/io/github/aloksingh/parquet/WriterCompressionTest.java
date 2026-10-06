package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.parquet.format.ColumnMetaData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class WriterCompressionTest {
    @TempDir
    Path directory;

    @Test
    void zeroThresholdBypassesEvenAnUnsupportedCompressorFactory() throws Exception {
        var schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("disabled-compression.parquet");
        try (var writer = new ParquetFileWriter(destination, schema, CompressionCodec.LZO, 64, 128, 0.0)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{42}));
        }
        var column = WriterTestSupport.footer(destination).getRow_groups().getFirst().getColumns().getFirst();
        assertEquals(org.apache.parquet.format.CompressionCodec.UNCOMPRESSED, column.getMeta_data().getCodec());
        var page = WriterTestSupport.pages(destination, 0, 0).getFirst();
        assertFalse(page.header().getData_page_header_v2().isIs_compressed());
        try (var reader = new ParquetFileReader(destination)) {
            var rows = reader.rowIterator();
            assertTrue(rows.hasNext());
            assertEquals(42, rows.next().getColumnValue(0));
            assertFalse(rows.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64"})
    void mixedPageRetentionKeepsPerPageFlagsAndChunkMetadataConsistent(Type type) throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(type, 0, 0);
        Path destination = directory.resolve("mixed-" + type + ".parquet");
        Random random = new Random(20261005L + type.ordinal());
        List<Object> expected = new ArrayList<>();
        try (var writer = new ParquetFileWriter(destination, schema, CompressionCodec.SNAPPY,
                2048, 1 << 24, 0.5)) {
            // Alternating highly compressible and incompressible blocks so one chunk
            // accumulates pages of both retention outcomes (small 2048-byte page target).
            for (int block = 0; block < 4; block++) {
                boolean repetitive = block % 2 == 0;
                Object constant = typedValue(type, random);
                for (int row = 0; row < 4096; row++) {
                    Object value = repetitive ? constant : typedValue(type, random);
                    expected.add(value);
                    writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
                }
            }
        }
        var meta = WriterTestSupport.footer(destination).getRow_groups().getFirst().getColumns()
                .getFirst().getMeta_data();
        assertMixedChunkConsistent(destination, meta, 0, 0);
        try (var reader = new ParquetFileReader(destination)) {
            var rows = reader.rowIterator();
            for (Object value : expected) {
                assertTrue(rows.hasNext(), "row count must match");
                assertEquals(value, rows.next().getColumnValue(0));
            }
            assertFalse(rows.hasNext());
        }
    }

    @Test
    void largeNestedMapRowsKeepChunkMetadataConsistentAcrossMixedPages() throws Exception {
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("maps", List.of(
                SchemaDescriptor.createStringMapColumn("map", false, false)));
        Path destination = directory.resolve("map-mixed-pages.parquet");
        Random random = new Random(4242);
        List<Map<String, String>> expected = new ArrayList<>();
        try (var writer = new ParquetFileWriter(destination, schema, CompressionCodec.SNAPPY,
                4096, 1 << 24, 0.5)) {
            for (int block = 0; block < 4; block++) {
                boolean repetitive = block % 2 == 0;
                for (int row = 0; row < 32; row++) {
                    Map<String, String> map = new LinkedHashMap<>();
                    for (int entry = 0; entry < 40; entry++) {
                        String key = repetitive ? "key-" + entry : "key-" + entry + "-" + randomToken(random);
                        String value = repetitive ? "same-value-for-everything" : randomToken(random);
                        map.put(key, value);
                    }
                    expected.add(map);
                    writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{map}));
                }
            }
        }
        var columns = WriterTestSupport.footer(destination).getRow_groups().getFirst().getColumns();
        assertEquals(2, columns.size(), "MAP contributes key and value leaf chunks");
        for (int column = 0; column < 2; column++) {
            assertMixedChunkConsistent(destination, columns.get(column).getMeta_data(), 0, column);
        }
        try (var reader = new ParquetFileReader(destination)) {
            var rows = reader.rowIterator();
            for (Map<String, String> map : expected) {
                assertTrue(rows.hasNext(), "row count must match");
                assertEquals(map, rows.next().getColumnValue(0));
            }
            assertFalse(rows.hasNext());
        }
    }

    @Test
    void chunkCodecAdvertisesCompressionOnlyWhenAPageRetainedIt() throws Exception {
        var schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Random random = new Random(7);

        Path randomFile = directory.resolve("all-random.parquet");
        try (var writer = new ParquetFileWriter(randomFile, schema, CompressionCodec.SNAPPY,
                2048, 1 << 24, 0.5)) {
            for (int i = 0; i < 4096; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{random.nextInt()}));
            }
        }
        var randomMeta = WriterTestSupport.footer(randomFile).getRow_groups().getFirst()
                .getColumns().getFirst().getMeta_data();
        var randomPages = WriterTestSupport.pages(randomFile, 0, 0);
        assertTrue(randomPages.size() > 1, "expected several pages in one chunk");
        for (var page : randomPages) {
            assertFalse(page.header().getData_page_header_v2().isIs_compressed(),
                    "incompressible page must be stored raw");
        }
        assertEquals(org.apache.parquet.format.CompressionCodec.UNCOMPRESSED, randomMeta.getCodec(),
                "chunk with no retained page must not advertise a codec");

        Path repetitiveFile = directory.resolve("all-repetitive.parquet");
        try (var writer = new ParquetFileWriter(repetitiveFile, schema, CompressionCodec.SNAPPY,
                2048, 1 << 24, 0.5)) {
            for (int i = 0; i < 4096; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{12345}));
            }
        }
        var repetitiveMeta = WriterTestSupport.footer(repetitiveFile).getRow_groups().getFirst()
                .getColumns().getFirst().getMeta_data();
        var repetitivePages = WriterTestSupport.pages(repetitiveFile, 0, 0);
        assertTrue(repetitivePages.size() > 1, "expected several pages in one chunk");
        for (var page : repetitivePages) {
            assertTrue(page.header().getData_page_header_v2().isIs_compressed(),
                    "compressible page must be retained compressed");
        }
        assertEquals(org.apache.parquet.format.CompressionCodec.SNAPPY, repetitiveMeta.getCodec(),
                "chunk with a retained page must advertise the compression codec");
    }

    /**
     * Per page: the DataPageHeaderV2 flag must match how the body is actually stored, and the
     * chunk must contain both retention outcomes with codec/totals consistent with its pages.
     */
    private void assertMixedChunkConsistent(Path path, ColumnMetaData meta, int group, int column)
            throws Exception {
        var pages = WriterTestSupport.pages(path, group, column);
        assertTrue(pages.size() > 1, "several pages must form one chunk");
        Decompressor decompressor = Decompressor.create(CompressionCodec.SNAPPY);
        boolean anyCompressed = false;
        boolean anyRaw = false;
        long compressedTotal = 0;
        long uncompressedTotal = 0;
        for (var page : pages) {
            var v2 = page.header().getData_page_header_v2();
            assertNotNull(v2, "writer must emit V2 data pages");
            int levels = v2.getRepetition_levels_byte_length() + v2.getDefinition_levels_byte_length();
            byte[] values = Arrays.copyOfRange(page.payload(), levels, page.payload().length);
            int rawValuesLength = page.header().getUncompressed_page_size() - levels;
            if (v2.isIs_compressed()) {
                anyCompressed = true;
                assertTrue(values.length < rawValuesLength,
                        "retained page body must be smaller than its raw form");
                assertEquals(rawValuesLength,
                        decompressor.decompress(ByteBuffer.wrap(values), rawValuesLength).remaining(),
                        "flagged compressed body must decompress to the declared uncompressed size");
            } else {
                anyRaw = true;
                assertEquals(rawValuesLength, values.length,
                        "unflagged page body must be stored raw with matching sizes");
            }
            compressedTotal += page.headerSize() + page.payload().length;
            uncompressedTotal += page.headerSize() + page.header().getUncompressed_page_size();
        }
        assertTrue(anyCompressed, "compressible pages must be retained compressed");
        assertTrue(anyRaw, "incompressible pages must be stored uncompressed");
        assertEquals(anyCompressed, meta.getCodec() == org.apache.parquet.format.CompressionCodec.SNAPPY,
                "chunk codec must advertise SNAPPY exactly when a page retained compression");
        assertEquals(uncompressedTotal, meta.getTotal_uncompressed_size(),
                "chunk uncompressed total must sum its pages");
        assertEquals(compressedTotal, meta.getTotal_compressed_size(),
                "chunk compressed total must sum its stored pages");
    }

    private static Object typedValue(Type type, Random random) {
        return type == Type.INT32 ? (Object) random.nextInt() : (Object) random.nextLong();
    }

    private static String randomToken(Random random) {
        StringBuilder token = new StringBuilder(24);
        for (int i = 0; i < 24; i++) token.append((char) ('a' + random.nextInt(26)));
        return token.toString();
    }
}
