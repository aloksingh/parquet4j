package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PageReaderV2SafetyTest {
    private static final ColumnDescriptor REPEATED =
            new ColumnDescriptor(Type.INT32, new String[]{"value"}, 1, 1, 0);

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidLengths")
    void validatesV2LevelLengthsBeforeBodyAllocation(String label, int repetition, int definition,
                                                     int uncompressed) throws Exception {
        PageHeader header = header(16, uncompressed, repetition, definition, false);
        byte[] bytes = PageReaderSafetyTest.pageBytes(header, new byte[16]);
        var chunk = new PageReaderSafetyTest.ArrayChunk(bytes, Integer.MAX_VALUE);
        PageReader reader = new PageReader(chunk,
                PageReaderSafetyTest.metadata(bytes.length, CompressionCodec.UNCOMPRESSED), REPEATED);
        ParquetException failure = assertThrows(ParquetException.class, reader::readNextPage, label);
        assertTrue(failure.getMessage().contains("level"), failure.getMessage());
        assertEquals(1, chunk.requests.size(), "V2 lengths must be checked before a body read");
    }

    static Stream<Arguments> invalidLengths() {
        return Stream.of(
                Arguments.of("negative repetitions", -1, 0, 16),
                Arguments.of("negative definitions", 0, -1, 16),
                Arguments.of("sum exceeds stored page", 12, 8, 16),
                Arguments.of("sum exceeds decoded page", 6, 4, 8));
    }

    @Test
    void exposesReadOnlyLevelAndValueSlices() throws Exception {
        byte[] body = {2, 0, 2, 1, 42, 0, 0, 0};
        PageHeader header = header(body.length, body.length, 2, 2, false);
        byte[] bytes = PageReaderSafetyTest.pageBytes(header, body);
        PageReader reader = new PageReader(new PageReaderSafetyTest.ArrayChunk(bytes, 3),
                PageReaderSafetyTest.metadata(bytes.length, CompressionCodec.UNCOMPRESSED), REPEATED);
        Page.DataPageV2 page = assertInstanceOf(Page.DataPageV2.class, reader.readNextPage());
        assertTrue(page.repetitionLevels().isReadOnly());
        assertTrue(page.definitionLevels().isReadOnly());
        assertTrue(page.data().isReadOnly());
        assertArrayEquals(new byte[]{2, 0}, PageReaderSafetyTest.bytes(page.repetitionLevels()));
        assertArrayEquals(new byte[]{2, 1}, PageReaderSafetyTest.bytes(page.definitionLevels()));
        assertArrayEquals(new byte[]{42, 0, 0, 0}, PageReaderSafetyTest.bytes(page.data()));
        assertEquals(0, page.data().position());
        assertThrows(java.nio.ReadOnlyBufferException.class, () -> page.definitionLevels().put(0, (byte) 3));
        assertNull(reader.readNextPage());
    }

    @Test
    void rejectsUncompressedV2OutputSizeMismatch() throws Exception {
        byte[] body = {2, 0, 2, 1, 42, 0, 0, 0};
        byte[] bytes = PageReaderSafetyTest.pageBytes(header(body.length, 12, 2, 2, false), body);
        PageReader reader = new PageReader(new PageReaderSafetyTest.ArrayChunk(bytes, 3),
                PageReaderSafetyTest.metadata(bytes.length, CompressionCodec.SNAPPY), REPEATED);
        assertThrows(ParquetException.class, reader::readNextPage);
    }

    @Test
    void treatsAnOmittedIsCompressedFieldAsTruePerThePinnedThriftDefault() throws Exception {
        byte[] values = {42, 0, 0, 0};
        byte[] compressed = Compressor.create(CompressionCodec.SNAPPY).compress(values);
        byte[] body = ByteBuffer.allocate(4 + compressed.length).put(new byte[]{2, 0, 2, 1}).put(compressed).array();
        PageHeader header = header(body.length, 8, 2, 2, false);
        header.getData_page_header_v2().unsetIs_compressed();
        byte[] bytes = PageReaderSafetyTest.pageBytes(header, body);
        PageReader reader = new PageReader(new PageReaderSafetyTest.ArrayChunk(bytes, 3),
                PageReaderSafetyTest.metadata(bytes.length, CompressionCodec.SNAPPY), REPEATED);
        Page.DataPageV2 page = assertInstanceOf(Page.DataPageV2.class, reader.readNextPage());
        assertTrue(page.isCompressed());
        assertArrayEquals(values, PageReaderSafetyTest.bytes(page.data()));
    }

    static PageHeader header(int compressed, int uncompressed, int repetition, int definition, boolean isCompressed) {
        return new PageHeader(PageType.DATA_PAGE_V2, uncompressed, compressed)
                .setData_page_header_v2(new DataPageHeaderV2(1, 0, 1, Encoding.PLAIN, definition, repetition)
                        .setIs_compressed(isCompressed));
    }
}
