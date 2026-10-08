package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;
import io.github.aloksingh.parquet.writer.DictionaryOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WriterDictionaryEncodingTest {
    @TempDir
    Path directory;

    private static final List<org.apache.parquet.format.Encoding> DICT_ENCODINGS =
            List.of(org.apache.parquet.format.Encoding.RLE,
                    org.apache.parquet.format.Encoding.PLAIN,
                    org.apache.parquet.format.Encoding.RLE_DICTIONARY);

    private static byte[] le(int... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int value : values) buffer.putInt(value);
        return buffer.array();
    }

    private static byte[] bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) result[i] = (byte) values[i];
        return result;
    }

    private ParquetFileWriter writer(Path destination, SchemaDescriptor schema, int pageTarget, int groupTarget,
                                     DictionaryOptions options) {
        return new ParquetFileWriter(destination, schema, CompressionCodec.UNCOMPRESSED,
                pageTarget, groupTarget, 0.90, options);
    }

    private static List<Object> readColumn(Path path, int column) throws Exception {
        try (ParquetFileReader reader = new ParquetFileReader(path)) {
            var iterator = reader.rowIterator();
            java.util.List<Object> values = new java.util.ArrayList<>();
            while (iterator.hasNext()) {
                values.add(iterator.next().getColumnValue(column));
            }
            return values;
        }
    }

    @Test
    void repeatedValuesProduceWidthZeroIndexStreamAndPlainDictionaryBody() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("width-zero.parquet");
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1024 * 1024, DictionaryOptions.enabled())) {
            for (int i = 0; i < 4; i++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{5}));
        }
        var footer = WriterTestSupport.footer(destination);
        var meta = footer.getRow_groups().getFirst().getColumns().getFirst().getMeta_data();
        assertEquals(DICT_ENCODINGS, meta.getEncodings());
        assertTrue(meta.getDictionary_page_offset() < meta.getData_page_offset());

        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(2, pages.size());
        var dictionary = pages.get(0).header();
        assertEquals(org.apache.parquet.format.PageType.DICTIONARY_PAGE, dictionary.getType());
        assertEquals(1, dictionary.getDictionary_page_header().getNum_values());
        assertEquals(org.apache.parquet.format.Encoding.PLAIN, dictionary.getDictionary_page_header().getEncoding());
        assertArrayEquals(le(5), pages.get(0).payload());

        var data = pages.get(1).header();
        assertEquals(org.apache.parquet.format.PageType.DATA_PAGE_V2, data.getType());
        var header = data.getData_page_header_v2();
        assertEquals(org.apache.parquet.format.Encoding.RLE_DICTIONARY, header.getEncoding());
        assertEquals(4, header.getNum_values());
        assertEquals(4, header.getNum_rows());
        assertEquals(0, header.getNum_nulls());
        assertEquals(0, header.getDefinition_levels_byte_length());
        assertEquals(0, header.getRepetition_levels_byte_length());
        assertFalse(header.isIs_compressed());
        // One repeated run of four zero ids at bit width 0: width byte, then
        // header (4 << 1) with zero value bytes.
        assertArrayEquals(bytes(0x00, 0x08), pages.get(1).payload());

        assertEquals(List.of(5, 5, 5, 5), readColumn(destination, 0));
    }

    @Test
    void packedIndexStreamMatchesTheSpecCanonicalExample() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("packed.parquet");
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1024 * 1024, DictionaryOptions.enabled())) {
            for (int i = 0; i < 8; i++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i}));
        }
        var meta = WriterTestSupport.footer(destination).getRow_groups().getFirst()
                .getColumns().getFirst().getMeta_data();
        assertEquals(DICT_ENCODINGS, meta.getEncodings());
        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(2, pages.size());
        assertArrayEquals(le(0, 1, 2, 3, 4, 5, 6, 7), pages.get(0).payload());
        // The format document's worked example: one bit-packed group of eight
        // values 0..7 at width 3 is run header 03 then payload 88 C6 FA.
        assertArrayEquals(bytes(0x03, 0x03, 0x88, 0xC6, 0xFA), pages.get(1).payload());
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7), readColumn(destination, 0));
    }

    @Test
    void multiPageChunkSharesOneDictionaryPage() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("multi-page.parquet");
        int[] rows = {10, 20, 30, 10, 20, 30, 10, 20, 30, 10};
        try (ParquetFileWriter writer = writer(destination, schema, 16, 1024 * 1024, DictionaryOptions.enabled())) {
            for (int value : rows) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
        }
        var meta = WriterTestSupport.footer(destination).getRow_groups().getFirst()
                .getColumns().getFirst().getMeta_data();
        assertEquals(DICT_ENCODINGS, meta.getEncodings());
        assertTrue(meta.getDictionary_page_offset() < meta.getData_page_offset());
        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(4, pages.size());
        assertEquals(org.apache.parquet.format.PageType.DICTIONARY_PAGE, pages.get(0).header().getType());
        assertArrayEquals(le(10, 20, 30), pages.get(0).payload());
        for (int i = 1; i < pages.size(); i++) {
            assertEquals(org.apache.parquet.format.Encoding.RLE_DICTIONARY,
                    pages.get(i).header().getData_page_header_v2().getEncoding());
        }
        // The first data page indexes the first two values: width 1, two
        // repeated runs of one id each.
        assertArrayEquals(bytes(0x01, 0x02, 0x00, 0x02, 0x01), pages.get(1).payload());
        long total = 0;
        for (var page : pages) total += page.headerSize() + page.header().getUncompressed_page_size();
        assertEquals(total, meta.getTotal_uncompressed_size());
        assertEquals(List.of(10, 20, 30, 10, 20, 30, 10, 20, 30, 10), readColumn(destination, 0));
    }

    @Test
    void dictionaryBudgetExhaustionFallsBackToPlainMidChunk() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("fallback.parquet");
        int[] rows = {1, 2, 3, 1, 2, 3, 4, 5, 6, 7};
        try (ParquetFileWriter writer = writer(destination, schema, 20, 1024 * 1024,
                DictionaryOptions.enabled(12))) {
            for (int value : rows) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
        }
        var meta = WriterTestSupport.footer(destination).getRow_groups().getFirst()
                .getColumns().getFirst().getMeta_data();
        assertEquals(DICT_ENCODINGS, meta.getEncodings());
        assertTrue(meta.getDictionary_page_offset() < meta.getData_page_offset());
        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(4, pages.size());
        assertEquals(org.apache.parquet.format.PageType.DICTIONARY_PAGE, pages.get(0).header().getType());
        // Three distinct four-byte entries fit the 12-byte budget; the fourth
        // distinct value arms PLAIN fallback for the rest of the chunk.
        assertArrayEquals(le(1, 2, 3), pages.get(0).payload());
        assertEquals(org.apache.parquet.format.Encoding.RLE_DICTIONARY,
                pages.get(1).header().getData_page_header_v2().getEncoding());
        assertArrayEquals(bytes(0x02, 0x02, 0x00, 0x02, 0x01, 0x02, 0x02), pages.get(1).payload());
        // The middle page straddles fallback: its indexed prefix is re-encoded
        // PLAIN from the dictionary, followed by the raw suffix values.
        assertEquals(org.apache.parquet.format.Encoding.PLAIN,
                pages.get(2).header().getData_page_header_v2().getEncoding());
        assertArrayEquals(le(1, 2, 3, 4, 5, 6), pages.get(2).payload());
        assertEquals(org.apache.parquet.format.Encoding.PLAIN,
                pages.get(3).header().getData_page_header_v2().getEncoding());
        assertArrayEquals(le(7), pages.get(3).payload());
        assertEquals(List.of(1, 2, 3, 1, 2, 3, 4, 5, 6, 7), readColumn(destination, 0));
    }

    @Test
    void allNullColumnStaysPlainWithoutDictionaryPage() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 1, 0);
        Path destination = directory.resolve("all-null.parquet");
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1024 * 1024, DictionaryOptions.enabled())) {
            for (int i = 0; i < 4; i++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{null}));
        }
        var meta = WriterTestSupport.footer(destination).getRow_groups().getFirst()
                .getColumns().getFirst().getMeta_data();
        assertEquals(List.of(org.apache.parquet.format.Encoding.RLE, org.apache.parquet.format.Encoding.PLAIN),
                meta.getEncodings());
        assertFalse(meta.isSetDictionary_page_offset());
        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(1, pages.size());
        var header = pages.get(0).header().getData_page_header_v2();
        assertEquals(org.apache.parquet.format.Encoding.PLAIN, header.getEncoding());
        assertEquals(4, header.getNum_nulls());
        assertEquals(2, header.getDefinition_levels_byte_length());
        assertEquals(0, header.getRepetition_levels_byte_length());
        // Body holds only the definition levels: one repeated run of four nulls
        // (header 4 << 1, value 0) and an empty values region.
        assertArrayEquals(bytes(0x08, 0x00), pages.get(0).payload());
        assertEquals(java.util.Collections.nCopies(4, null), readColumn(destination, 0));
    }

    @Test
    void booleanColumnsAreNeverDictionaryEncoded() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.BOOLEAN, 0, 0);
        Path destination = directory.resolve("booleans.parquet");
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1024 * 1024, DictionaryOptions.enabled())) {
            for (int i = 0; i < 8; i++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i % 2 == 0}));
        }
        var meta = WriterTestSupport.footer(destination).getRow_groups().getFirst()
                .getColumns().getFirst().getMeta_data();
        assertEquals(List.of(org.apache.parquet.format.Encoding.RLE, org.apache.parquet.format.Encoding.PLAIN),
                meta.getEncodings());
        assertFalse(meta.isSetDictionary_page_offset());
        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(1, pages.size());
        assertEquals(org.apache.parquet.format.Encoding.PLAIN,
                pages.get(0).header().getData_page_header_v2().getEncoding());
        assertEquals(List.of(true, false, true, false, true, false, true, false), readColumn(destination, 0));
    }

    @Test
    void floatingPointEntriesKeepRawBitIdentity() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.DOUBLE, 0, 0);
        Path destination = directory.resolve("doubles.parquet");
        double zero = 0.0d;
        double negativeZero = Double.longBitsToDouble(0x8000_0000_0000_0000L);
        double canonicalNaN = Double.NaN;
        double payloadNaN = Double.longBitsToDouble(0x7ff8_0000_0000_0001L);
        double[] rows = {zero, negativeZero, canonicalNaN, payloadNaN, zero, negativeZero};
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1024 * 1024, DictionaryOptions.enabled())) {
            for (double value : rows) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
        }
        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(2, pages.size());
        // Distinct raw bit patterns are distinct entries; equal patterns merge.
        assertEquals(4, pages.get(0).header().getDictionary_page_header().getNum_values());
        var meta = WriterTestSupport.footer(destination).getRow_groups().getFirst()
                .getColumns().getFirst().getMeta_data();
        assertEquals(DICT_ENCODINGS, meta.getEncodings());
        List<Object> values = readColumn(destination, 0);
        assertEquals(rows.length, values.size());
        for (int i = 0; i < rows.length; i++) {
            assertEquals(Double.doubleToRawLongBits(rows[i]),
                    Double.doubleToRawLongBits((Double) values.get(i)), "row " + i);
        }
    }

    @Test
    void equalBinaryValuesShareOneDictionaryEntry() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.BYTE_ARRAY, 0, 0);
        Path destination = directory.resolve("binary.parquet");
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1024 * 1024, DictionaryOptions.enabled())) {
            for (int i = 0; i < 3; i++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{"abc"}));
        }
        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(2, pages.size());
        assertEquals(1, pages.get(0).header().getDictionary_page_header().getNum_values());
        // PLAIN body: 4-byte length prefix plus the three content bytes.
        assertArrayEquals(bytes(0x03, 0x00, 0x00, 0x00, 'a', 'b', 'c'), pages.get(0).payload());
        assertArrayEquals(bytes(0x00, 0x06), pages.get(1).payload());
        List<Object> values = readColumn(destination, 0);
        assertEquals(3, values.size());
        for (Object value : values) {
            assertArrayEquals(bytes('a', 'b', 'c'), (byte[]) value);
        }
    }

    @Test
    void dictionaryResetsBetweenRowGroups() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("groups.parquet");
        int[] rows = {1, 2, 1, 2};
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1, DictionaryOptions.enabled())) {
            for (int value : rows) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value}));
        }
        var footer = WriterTestSupport.footer(destination);
        assertEquals(4, footer.getRow_groupsSize());
        for (var group : footer.getRow_groups()) {
            var meta = group.getColumns().getFirst().getMeta_data();
            assertEquals(DICT_ENCODINGS, meta.getEncodings());
            assertTrue(meta.getDictionary_page_offset() < meta.getData_page_offset());
            var pages = WriterTestSupport.pages(destination, footer.getRow_groups().indexOf(group), 0);
            assertEquals(2, pages.size());
            assertEquals(org.apache.parquet.format.PageType.DICTIONARY_PAGE, pages.get(0).header().getType());
            assertEquals(1, pages.get(0).header().getDictionary_page_header().getNum_values());
        }
        assertEquals(List.of(1, 2, 1, 2), readColumn(destination, 0));
    }

    @Test
    void dictionaryEncodedColumnIsReadableByDuckDb() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("duckdb.parquet");
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1024 * 1024, DictionaryOptions.enabled())) {
            for (int i = 0; i < 30; i++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i % 3}));
        }
        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(org.apache.parquet.format.Encoding.RLE_DICTIONARY,
                pages.get(1).header().getData_page_header_v2().getEncoding());
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.prepareStatement(
                     "SELECT value, COUNT(*) FROM read_parquet(?) GROUP BY value ORDER BY value")) {
            statement.setString(1, destination.toString());
            try (var rows = statement.executeQuery()) {
                for (int expected = 0; expected <= 2; expected++) {
                    assertTrue(rows.next());
                    assertEquals(expected, rows.getInt(1));
                    assertEquals(10, rows.getInt(2));
                }
                assertFalse(rows.next());
            }
        }
    }

    @Test
    void mapLeavesAreDictionaryEncodedAndRemainInteroperable() throws Exception {
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("mapping", List.of(
                SchemaDescriptor.createMapColumn("map", Type.BYTE_ARRAY, Type.INT32, true, true)));
        Path destination = directory.resolve("map.parquet");
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1024 * 1024, DictionaryOptions.enabled())) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{Map.of("x", 7, "y", 8)}));
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{Map.of("x", 7)}));
        }
        for (int leaf = 0; leaf < 2; leaf++) {
            var meta = WriterTestSupport.footer(destination).getRow_groups().getFirst()
                    .getColumns().get(leaf).getMeta_data();
            assertEquals(DICT_ENCODINGS, meta.getEncodings());
            assertTrue(meta.getDictionary_page_offset() < meta.getData_page_offset());
        }
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.prepareStatement(
                     "SELECT map['x'], map['y'] FROM read_parquet(?) ORDER BY map['y'] DESC NULLS LAST")) {
            statement.setString(1, destination.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals(7, rows.getInt(1));
                assertEquals(8, rows.getInt(2));
                assertTrue(rows.next());
                assertEquals(7, rows.getInt(1));
                assertNull(rows.getObject(2));
                assertFalse(rows.next());
            }
        }
    }

    @Test
    void dictionaryValuesSurviveCallerMutationAfterAcceptance() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.BYTE_ARRAY, 0, 0);
        Path destination = directory.resolve("snapshot.parquet");
        byte[] raw = {'a', 'b'};
        try (ParquetFileWriter writer = writer(destination, schema, 1024, 1024 * 1024, DictionaryOptions.enabled())) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{raw}));
            raw[0] = 'q';
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{new byte[]{'a', 'b'}}));
        }
        var pages = WriterTestSupport.pages(destination, 0, 0);
        // Both rows share one entry; the caller's later mutation never reaches it.
        assertArrayEquals(bytes(0x02, 0x00, 0x00, 0x00, 'a', 'b'), pages.get(0).payload());
        List<Object> values = readColumn(destination, 0);
        assertEquals(2, values.size());
        for (Object value : values) {
            assertArrayEquals(bytes('a', 'b'), (byte[]) value);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void invalidDictionaryBudgetIsRejected(int maxDictionaryBytes) {
        assertThrows(IllegalArgumentException.class, () -> DictionaryOptions.enabled(maxDictionaryBytes));
        assertThrows(IllegalArgumentException.class, () -> new DictionaryOptions(true, maxDictionaryBytes));
    }

    @Test
    void oversizedSingleRowIsRejectedBeforeOpeningInDictionaryMode() throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.BYTE_ARRAY, 0, 0);
        Path destination = directory.resolve("hard-limit.parquet");
        byte[] sentinel = {7, 2, 7};
        Files.write(destination, sentinel);
        int[] opened = {0};
        var writer = new ParquetFileWriter(destination, schema, CompressionCodec.UNCOMPRESSED,
                64, 128, 0.90, DictionaryOptions.enabled()) {
            @Override
            int pageBodyLimit() {
                return 8;
            }

            @Override
            OutputStream openSink(Path path) throws IOException {
                opened[0]++;
                return super.openSink(path);
            }
        };
        assertThrows(IllegalArgumentException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{new byte[10]})));
        assertEquals(0, opened[0]);
        assertDoesNotThrow(writer::close);
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
    }
}
