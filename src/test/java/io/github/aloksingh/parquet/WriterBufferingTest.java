package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.nio.file.Path;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class WriterBufferingTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(ints = {8, 16})
    void pageTargetsProduceIndependentRowBoundaryV2PagesInsideOneGroup(int pageTarget) throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("pages.parquet");
        int count = 10;
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema,
                CompressionCodec.UNCOMPRESSED, pageTarget, count * Integer.BYTES)) {
            for (int i = 0; i < count; i++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i}));
        }
        var footer = WriterTestSupport.footer(destination);
        assertEquals(1, footer.getRow_groupsSize());
        var pages = WriterTestSupport.pages(destination, 0, 0);
        int capacity = pageTarget / Integer.BYTES;
        assertEquals((count + capacity - 1) / capacity, pages.size());
        long uncompressed = 0;
        long compressed = 0;
        int observed = 0;
        for (var page : pages) {
            assertEquals(org.apache.parquet.format.PageType.DATA_PAGE_V2, page.header().getType());
            assertTrue(page.header().getUncompressed_page_size() <= pageTarget);
            var data = page.header().getData_page_header_v2();
            assertEquals(0, data.getNum_nulls());
            assertEquals(data.getNum_values(), data.getNum_rows());
            assertEquals(0, data.getDefinition_levels_byte_length());
            assertEquals(0, data.getRepetition_levels_byte_length());
            assertFalse(data.isIs_compressed());
            assertEquals(org.apache.parquet.format.Encoding.PLAIN, data.getEncoding());
            ByteBuffer values = ByteBuffer.wrap(page.payload()).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < data.getNum_values(); i++) assertEquals(observed++, values.getInt());
            assertFalse(values.hasRemaining());
            uncompressed += page.headerSize() + (long) page.header().getUncompressed_page_size();
            compressed += page.headerSize() + (long) page.header().getCompressed_page_size();
        }
        assertEquals(count, observed);
        var group = footer.getRow_groups().getFirst();
        var column = group.getColumns().getFirst().getMeta_data();
        assertEquals(uncompressed, column.getTotal_uncompressed_size());
        assertEquals(compressed, column.getTotal_compressed_size());
        assertEquals(uncompressed, group.getTotal_byte_size());
    }

    @ParameterizedTest
    @CsvSource({"10,12", "1200,16384"})
    void rowGroupsUseByteTargetsInsteadOfAFixedThousandRows(int rows, int target) throws Exception {
        SchemaDescriptor schema = WriterValidationTest.scalar(Type.INT32, 0, 0);
        Path destination = directory.resolve("groups.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema,
                CompressionCodec.UNCOMPRESSED, 32768, target)) {
            for (int i = 0; i < rows; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i}));
            }
        }
        var footer = WriterTestSupport.footer(destination);
        int capacity = target / Integer.BYTES;
        int expectedGroups = (rows + capacity - 1) / capacity;
        assertEquals(expectedGroups, footer.getRow_groupsSize());
        assertEquals(rows, footer.getNum_rows());
        long observed = 0;
        for (var group : footer.getRow_groups()) {
            long expected = Math.min(capacity, rows - observed);
            assertEquals(expected, group.getNum_rows());
            observed += group.getNum_rows();
        }
        assertEquals(rows, observed);
        try (ParquetFileReader reader = new ParquetFileReader(destination)) {
            var iterator = reader.rowIterator();
            for (int i = 0; i < rows; i++) {
                assertTrue(iterator.hasNext(), "Missing row " + i);
                assertEquals(i, iterator.next().getColumnValue(0));
            }
            assertFalse(iterator.hasNext());
        }
    }
}
