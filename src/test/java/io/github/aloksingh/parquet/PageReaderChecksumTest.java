package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PageReaderChecksumTest {
    private static final ColumnDescriptor REQUIRED =
            new ColumnDescriptor(Type.INT32, new String[]{"value"}, 0, 0, 0);
    private static final ColumnDescriptor REPEATED =
            new ColumnDescriptor(Type.INT32, new String[]{"value"}, 1, 1, 0);

    @ParameterizedTest(name = "{0}")
    @MethodSource("pages")
    void verifiesCrcOverTheStoredBodyNotTheDecodedValues(String label, PageHeader header,
                                                         byte[] body, ColumnDescriptor descriptor) throws Exception {
        CRC32 crc = new CRC32();
        crc.update(body);
        header.setCrc((int) crc.getValue());
        assertNotNull(reader(header, body, descriptor, PageReadOptions.STRICT).readNextPage());
        header.setCrc(header.getCrc() ^ 1);
        PageReader strict = reader(header, body, descriptor, PageReadOptions.STRICT);
        ParquetException failure = assertThrows(ParquetException.class, strict::readNextPage);
        assertTrue(failure.getMessage().contains("CRC"), failure.getMessage());
        assertThrows(ParquetException.class, strict::readNextPage,
                "A failed read must not advance into successful-looking EOF");
        assertNotNull(reader(header, body, descriptor, PageReadOptions.DEFAULT).readNextPage());
    }

    static Stream<Arguments> pages() throws Exception {
        byte[] values = {42, 0, 0, 0};
        byte[] compressed = Compressor.create(CompressionCodec.SNAPPY).compress(values);
        byte[] v2Body = ByteBuffer.allocate(4 + compressed.length)
                .put(new byte[]{2, 0, 2, 1}).put(compressed).array();
        return Stream.of(
                Arguments.of("V1 compressed body", PageReaderSafetyTest.v1Header(compressed.length, 4, 1),
                        compressed, REQUIRED),
                Arguments.of("V2 encoded levels plus compressed values",
                        PageReaderV2SafetyTest.header(v2Body.length, 8, 2, 2, true), v2Body, REPEATED),
                Arguments.of("dictionary compressed body", new PageHeader(PageType.DICTIONARY_PAGE, 4, compressed.length)
                        .setDictionary_page_header(new DictionaryPageHeader(1, Encoding.PLAIN)), compressed, REQUIRED));
    }

    @Test
    void rejectsTheCorruptChecksumFixtureInStrictModeButExplicitlyRetainsLegacyDefault() throws Exception {
        String fixture = "src/test/data/rle-dict-uncompressed-corrupt-checksum.parquet";
        try (ParquetFileReader metadataReader = new ParquetFileReader(fixture);
             FileChunkReader chunk = new FileChunkReader(fixture)) {
            var metadata = metadataReader.getMetadata().rowGroups().get(0).columns().get(0);
            var descriptor = metadataReader.getSchema().getColumn(0);
            var strict = new PageReader(chunk, metadata, descriptor, PageReadOptions.STRICT);
            assertThrows(ParquetException.class, strict::readAllPages);
            var explicitLegacy = new PageReader(chunk, metadata, descriptor, PageReadOptions.DEFAULT).readAllPages();
            var oldConstructor = new PageReader(chunk, metadata, descriptor).readAllPages();
            assertFalse(explicitLegacy.isEmpty());
            assertEquals(explicitLegacy.size(), oldConstructor.size());
        }
    }

    private static PageReader reader(PageHeader header, byte[] body, ColumnDescriptor descriptor,
                                     PageReadOptions options) throws Exception {
        byte[] bytes = PageReaderSafetyTest.pageBytes(header, body);
        return new PageReader(new PageReaderSafetyTest.ArrayChunk(bytes, 2),
                PageReaderSafetyTest.metadata(bytes.length, CompressionCodec.SNAPPY), descriptor, options);
    }
}
