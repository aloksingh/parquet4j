package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.Type;

import java.util.stream.Stream;

import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PageReaderCountsTest {
    private static final ColumnDescriptor REQUIRED =
            new ColumnDescriptor(Type.INT32, new String[]{"value"}, 0, 0, 0);
    private static final PageReadOptions LIMITS = new PageReadOptions(1024, 1024, 1024, 4, false);

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidHeaders")
    void rejectsInvalidPageCountsOrMissingTypeHeadersBeforeBodyRead(String label, PageHeader header) throws Exception {
        byte[] bytes = PageReaderSafetyTest.pageBytes(header, new byte[4]);
        var chunk = new PageReaderSafetyTest.ArrayChunk(bytes, Integer.MAX_VALUE);
        PageReader reader = new PageReader(chunk,
                PageReaderSafetyTest.metadata(bytes.length, CompressionCodec.UNCOMPRESSED), REQUIRED, LIMITS);
        assertThrows(ParquetException.class, reader::readNextPage, label);
        assertEquals(1, chunk.requests.size(), "Header must be validated before a body read");
    }

    static Stream<Arguments> invalidHeaders() {
        return Stream.of(
                Arguments.of("negative V1 values", PageReaderSafetyTest.v1Header(4, 4, -1)),
                Arguments.of("excessive V1 values", PageReaderSafetyTest.v1Header(4, 4, 5)),
                Arguments.of("negative dictionary values", dictionary(-1)),
                Arguments.of("excessive dictionary values", dictionary(5)),
                Arguments.of("negative V2 values", v2(-1, 0, 0)),
                Arguments.of("excessive V2 values", v2(5, 0, 1)),
                Arguments.of("negative V2 null count", v2(1, -1, 1)),
                Arguments.of("V2 null count exceeds values", v2(1, 2, 1)),
                Arguments.of("negative V2 row count", v2(1, 0, -1)),
                Arguments.of("V2 row count exceeds values", v2(1, 0, 2)),
                Arguments.of("missing V1 header", new PageHeader(PageType.DATA_PAGE, 4, 4)),
                Arguments.of("missing V2 header", new PageHeader(PageType.DATA_PAGE_V2, 4, 4)),
                Arguments.of("missing dictionary header", new PageHeader(PageType.DICTIONARY_PAGE, 4, 4)));
    }

    static PageHeader dictionary(int values) {
        return new PageHeader(PageType.DICTIONARY_PAGE, 4, 4)
                .setDictionary_page_header(new DictionaryPageHeader(values, Encoding.PLAIN));
    }

    static PageHeader v2(int values, int nulls, int rows) {
        return new PageHeader(PageType.DATA_PAGE_V2, 4, 4)
                .setData_page_header_v2(new DataPageHeaderV2(values, nulls, rows, Encoding.PLAIN, 0, 0)
                        .setIs_compressed(false));
    }
}
