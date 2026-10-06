package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.Type;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class WriterOptionsTest {
    @TempDir
    Path directory;

    static Stream<Arguments> invalidOptions() {
        return Stream.of(
                Arguments.of(true, CompressionCodec.UNCOMPRESSED, 64, 128, 0.9),
                Arguments.of(false, null, 64, 128, 0.9),
                Arguments.of(false, CompressionCodec.UNCOMPRESSED, 0, 128, 0.9),
                Arguments.of(false, CompressionCodec.UNCOMPRESSED, -1, 128, 0.9),
                Arguments.of(false, CompressionCodec.UNCOMPRESSED, 64, 0, 0.9),
                Arguments.of(false, CompressionCodec.UNCOMPRESSED, 64, -1, 0.9),
                Arguments.of(false, CompressionCodec.UNCOMPRESSED, 64, 128, Double.NaN),
                Arguments.of(false, CompressionCodec.UNCOMPRESSED, 64, 128, Double.POSITIVE_INFINITY),
                Arguments.of(false, CompressionCodec.UNCOMPRESSED, 64, 128, Double.NEGATIVE_INFINITY),
                Arguments.of(false, CompressionCodec.UNCOMPRESSED, 64, 128, -0.1),
                Arguments.of(false, CompressionCodec.UNCOMPRESSED, 64, 128, 1.1));
    }

    @ParameterizedTest
    @MethodSource("invalidOptions")
    void invalidConfigurationIsRejectedBeforeAnyFileIsOpened(boolean nullPath, CompressionCodec codec,
                                                             int pageTarget, int groupTarget, double ratio) throws Exception {
        Path sentinelPath = directory.resolve("sentinel.parquet");
        byte[] sentinel = {1, 3, 5};
        Files.write(sentinelPath, sentinel);
        var schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        assertThrows(IllegalArgumentException.class, () -> new ParquetFileWriter(
                nullPath ? null : sentinelPath, schema, codec, pageTarget, groupTarget, ratio));
        assertArrayEquals(sentinel, Files.readAllBytes(sentinelPath));
        try (var files = Files.list(directory)) {
            assertEquals(List.of("sentinel.parquet"), files.map(path -> path.getFileName().toString()).toList());
        }
    }
}
