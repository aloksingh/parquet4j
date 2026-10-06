package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class WriterPageLimitsTest {
    @TempDir
    Path directory;

    static Stream<Arguments> oversizedRows() {
        return Stream.of(Arguments.of(false, new byte[40], 32, 10),
                Arguments.of(true, Map.of(1, 1L, 2, 2L, 3, 3L, 4, 4L), 1024, 3));
    }

    @ParameterizedTest
    @MethodSource("oversizedRows")
    void rowsOverHardByteOrEventLimitsAreRejectedBeforeOpening(boolean map, Object value,
                                                               int byteLimit, int valueLimit) throws Exception {
        SchemaDescriptor schema = map ? SchemaDescriptor.fromLogicalColumns("map-limit", List.of(
                SchemaDescriptor.createMapColumn("map", Type.INT32, Type.INT64, false, false)))
                : WriterValidationTest.scalar(Type.BYTE_ARRAY, 0, 0);
        Path destination = directory.resolve("hard-limit.parquet");
        byte[] sentinel = {7, 2, 7};
        Files.write(destination, sentinel);
        int[] opened = {0};
        var writer = new ParquetFileWriter(destination, schema) {
            @Override
            int pageBodyLimit() {
                return byteLimit;
            }

            @Override
            int pageValueLimit() {
                return valueLimit;
            }

            @Override
            OutputStream openSink(Path path) throws IOException {
                opened[0]++;
                return super.openSink(path);
            }
        };
        assertThrows(IllegalArgumentException.class,
                () -> writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{value})));
        assertEquals(0, opened[0]);
        assertDoesNotThrow(writer::close);
        assertArrayEquals(sentinel, Files.readAllBytes(destination));
        try (var files = Files.list(directory)) {
            assertEquals(List.of("hard-limit.parquet"), files.map(path -> path.getFileName().toString()).toList());
        }
    }

    @ParameterizedTest
    @CsvSource({"8,10", "64,2"})
    void accumulatingRowsStartNewPagesBeforeEitherHardLimit(int byteLimit, int valueLimit) throws Exception {
        var schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("bounded-pages.parquet");
        try (var writer = new ParquetFileWriter(destination, schema, CompressionCodec.UNCOMPRESSED, 64, 128) {
            @Override
            int pageBodyLimit() {
                return byteLimit;
            }

            @Override
            int pageValueLimit() {
                return valueLimit;
            }
        }) {
            for (int i = 0; i < 5; i++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i}));
        }
        var footer = WriterTestSupport.footer(destination);
        assertEquals(1, footer.getRow_groupsSize());
        var pages = WriterTestSupport.pages(destination, 0, 0);
        assertEquals(3, pages.size());
        for (var page : pages) {
            assertTrue(page.header().getUncompressed_page_size() <= byteLimit);
            var data = page.header().getData_page_header_v2();
            assertTrue(data.getNum_values() <= valueLimit);
            assertEquals(data.getNum_values(), data.getNum_rows());
        }
        assertEquals(5, footer.getRow_groups().getFirst().getColumns().getFirst().getMeta_data().getNum_values());
        try (var reader = new ParquetFileReader(destination)) {
            var rows = reader.rowIterator();
            for (int i = 0; i < 5; i++) {
                assertTrue(rows.hasNext());
                assertEquals(i, rows.next().getColumnValue(0));
            }
            assertFalse(rows.hasNext());
        }
    }
}
