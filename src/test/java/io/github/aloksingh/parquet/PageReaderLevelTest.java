package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.Stream;

import org.apache.parquet.format.PageHeader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PageReaderLevelTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidV1Levels")
    void rejectsInvalidV1LevelSections(String label, byte[] body, int definition, int repetition) throws Exception {
        var descriptor = new ColumnDescriptor(Type.INT32, new String[]{"value"}, definition, repetition, 0);
        PageHeader header = PageReaderSafetyTest.v1Header(body.length, body.length, 1);
        byte[] bytes = PageReaderSafetyTest.pageBytes(header, body);
        PageReader reader = new PageReader(new PageReaderSafetyTest.ArrayChunk(bytes, Integer.MAX_VALUE),
                PageReaderSafetyTest.metadata(bytes.length, CompressionCodec.UNCOMPRESSED), descriptor);
        ParquetException failure = assertThrows(ParquetException.class, reader::readNextPage, label);
        assertTrue(failure.getMessage().contains("level"), failure.getMessage());
    }

    static Stream<Arguments> invalidV1Levels() {
        return Stream.of(
                Arguments.of("negative definition length", prefixed(-1, 4), 1, 0),
                Arguments.of("definition length overflows addition", prefixed(Integer.MAX_VALUE, 4), 1, 0),
                Arguments.of("definition section crosses page", prefixed(9, 4), 1, 0),
                Arguments.of("missing definition prefix", new byte[]{1, 2}, 1, 0),
                Arguments.of("missing repetition prefix", new byte[]{1, 2}, 1, 1),
                Arguments.of("missing definition after repetitions", prefixed(4, 4), 1, 1));
    }

    private static byte[] prefixed(int length, int payloadSize) {
        return ByteBuffer.allocate(4 + payloadSize).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(length).array();
    }
}
