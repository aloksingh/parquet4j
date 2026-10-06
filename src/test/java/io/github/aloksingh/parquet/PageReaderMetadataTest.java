package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.ParquetMetadata.ColumnChunkMetadata;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PageReaderMetadataTest {
    private static final ColumnDescriptor REQUIRED =
            new ColumnDescriptor(Type.INT32, new String[]{"value"}, 0, 0, 0);

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidMetadata")
    void rejectsInvalidColumnChunkRangesBeforeReading(String label, long data, long dictionary,
                                                      long compressed, long uncompressed, long values) {
        ChunkReader chunk = new ChunkReader() {
            @Override
            public long length() {
                return 64;
            }

            @Override
            public ByteBuffer readBytes(long p, int n) {
                fail("Invalid metadata must not read bytes");
                return null;
            }
        };
        ColumnChunkMetadata metadata = new ColumnChunkMetadata(Type.INT32, new String[]{"value"},
                CompressionCodec.UNCOMPRESSED, data, dictionary, compressed, uncompressed, values, null);
        assertThrows(ParquetException.class, () -> new PageReader(chunk, metadata, REQUIRED), label);
    }

    static Stream<Arguments> invalidMetadata() {
        return Stream.of(
                Arguments.of("negative data offset", -1L, 0L, 1L, 1L, 1L),
                Arguments.of("negative dictionary offset (other than missing sentinel)", 4L, -2L, 4L, 4L, 1L),
                Arguments.of("negative compressed chunk size", 0L, 0L, -1L, 4L, 1L),
                Arguments.of("negative uncompressed chunk size", 0L, 0L, 4L, -1L, 1L),
                Arguments.of("negative chunk value count", 0L, 0L, 4L, 4L, -1L),
                Arguments.of("offset beyond EOF", 65L, 0L, 1L, 1L, 1L),
                Arguments.of("chunk crosses EOF", 60L, 0L, 5L, 5L, 1L),
                Arguments.of("end offset arithmetic overflows", 8L, 0L, Long.MAX_VALUE, 1L, 1L),
                Arguments.of("dictionary follows first data page", 4L, 8L, 16L, 16L, 1L),
                Arguments.of("first data page outside dictionary chunk", 20L, 4L, 8L, 8L, 1L));
    }

    @Test
    void reportsFailureToDetermineSourceLength() {
        ChunkReader chunk = new ChunkReader() {
            @Override
            public long length() throws IOException {
                throw new IOException("length failed");
            }

            @Override
            public ByteBuffer readBytes(long p, int n) {
                fail("Must determine length first");
                return null;
            }
        };
        ParquetException failure = assertThrows(ParquetException.class, () -> new PageReader(chunk,
                PageReaderSafetyTest.metadata(4, CompressionCodec.UNCOMPRESSED), REQUIRED));
        assertInstanceOf(IOException.class, failure.getCause());
    }

    @Test
    void acceptsTheMetadataReadersAbsentDictionarySentinel() throws Exception {
        byte[] bytes = PageReaderSafetyTest.pageBytes(PageReaderSafetyTest.v1Header(4, 4, 1), new byte[4]);
        var metadata = new ColumnChunkMetadata(Type.INT32, new String[]{"value"}, CompressionCodec.UNCOMPRESSED,
                0, -1, bytes.length, bytes.length, 1, null);
        PageReader reader = new PageReader(new PageReaderSafetyTest.ArrayChunk(bytes, 4), metadata, REQUIRED);
        assertNotNull(reader.readNextPage());
        assertNull(reader.readNextPage());
    }

    @Test
    void acceptsAnEmptyDictionaryOnlyChunkWithNoDataPageOffset() throws Exception {
        var header = new org.apache.parquet.format.PageHeader(org.apache.parquet.format.PageType.DICTIONARY_PAGE, 0, 0)
                .setDictionary_page_header(new org.apache.parquet.format.DictionaryPageHeader(0,
                        org.apache.parquet.format.Encoding.PLAIN));
        byte[] page = PageReaderSafetyTest.pageBytes(header, new byte[0]);
        byte[] bytes = ByteBuffer.allocate(4 + page.length).putInt(0).put(page).array();
        var metadata = new ColumnChunkMetadata(Type.INT32, new String[]{"value"}, CompressionCodec.UNCOMPRESSED,
                0, 4, page.length, page.length, 0, null);
        PageReader reader = new PageReader(new PageReaderSafetyTest.ArrayChunk(bytes, 4), metadata, REQUIRED);
        assertNotNull(reader.readNextPage());
        assertNull(reader.readNextPage());
    }

    @Test
    void acceptsAnEmptyChunkAtEof() throws IOException {
        PageReader reader = new PageReader(new PageReaderSafetyTest.ArrayChunk(new byte[0], 1),
                PageReaderSafetyTest.metadata(0, CompressionCodec.UNCOMPRESSED), REQUIRED);
        assertNull(reader.readNextPage());
    }
}
