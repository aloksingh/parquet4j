package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.Type;

import java.util.stream.Stream;

import org.apache.parquet.format.PageHeader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PageReaderSizeTest {
    private static final ColumnDescriptor REQUIRED =
            new ColumnDescriptor(Type.INT32, new String[]{"value"}, 0, 0, 0);
    private static final PageReadOptions SMALL_LIMITS = new PageReadOptions(1024, 16, 16, 4, false);

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidPageSizes")
    void validatesDeclaredSizesBeforeBodyReads(String field, int compressed, int uncompressed,
                                               int bodyLength, int chunkBodyLength) throws Exception {
        byte[] body = new byte[bodyLength];
        PageHeader header = PageReaderSafetyTest.v1Header(compressed, uncompressed, 1);
        byte[] data = PageReaderSafetyTest.pageBytes(header, body);
        int headerLength = data.length - body.length;
        var chunk = new PageReaderSafetyTest.ArrayChunk(data, Integer.MAX_VALUE);
        PageReader reader = new PageReader(chunk,
                PageReaderSafetyTest.metadata(headerLength + chunkBodyLength, CompressionCodec.UNCOMPRESSED),
                REQUIRED, SMALL_LIMITS);
        ParquetException failure = assertThrows(ParquetException.class, reader::readNextPage);
        assertTrue(failure.getMessage().contains(field), failure.getMessage());
        // No direct request for the body may be issued when its size is invalid.
        assertEquals(1, chunk.requests.size());
        assertEquals(0, chunk.requests.get(0)[0]);
    }

    static Stream<Arguments> invalidPageSizes() {
        return Stream.of(
                Arguments.of("compressed", -1, 4, 4, 4),
                Arguments.of("uncompressed", 4, -1, 4, 4),
                Arguments.of("compressed", 17, 4, 17, 17),
                Arguments.of("uncompressed", 4, 17, 4, 4),
                Arguments.of("chunk", 8, 8, 16, 4));
    }
}
