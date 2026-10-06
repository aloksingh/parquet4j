package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnBatch;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.DecodedPage;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Wiring and lifetime contract of the batch API on the reader: name resolution,
 * copy-on-construct ownership, read-only views, dictionary preservation until
 * materialization, and the unboxed required route.
 */
class BatchReaderApiTest {

    @Test
    void readColumnBatchResolvesPhysicalPathsAndLogicalNames() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/alltypes_dictionary.parquet")) {
            ParquetFileReader.RowGroupReader group = reader.getRowGroup(0);
            ColumnBatch byIndex = group.readColumnBatch(0);
            ColumnBatch byPath = group.readColumnBatch("id");
            ColumnBatch byName = group.readColumnBatch("id");
            assertEquals(byIndex.size(), byPath.size());
            BatchTestSupport.assertSameValues("index vs path", BatchTestSupport.batchValues(byIndex),
                    BatchTestSupport.batchValues(byPath));
            BatchTestSupport.assertSameValues("index vs name", BatchTestSupport.batchValues(byIndex),
                    BatchTestSupport.batchValues(byName));
            assertThrows(IllegalArgumentException.class, () -> group.readColumnBatch("no_such_column"));
        }
    }

    @Test
    void nestedLeafPathsAndLogicalNamesBothResolve() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/nullable.impala.parquet")) {
            ParquetFileReader.RowGroupReader group = reader.getRowGroup(0);
            ColumnBatch byLeafPath = group.readColumnBatch("int_array.list.element");
            ColumnBatch byLogicalName = group.readColumnBatch("int_array");
            assertEquals(byLeafPath.size(), byLogicalName.size());
            BatchTestSupport.assertSameValues("leaf path vs logical name",
                    BatchTestSupport.batchValues(byLeafPath), BatchTestSupport.batchValues(byLogicalName));
        }
    }

    @Test
    void batchesOwnTheirDataAndSurviveReaderClose() throws IOException {
        ColumnBatch closedFileBatch;
        List<Object> expected;
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/alltypes_dictionary.parquet")) {
            closedFileBatch = reader.getRowGroup(0).readColumnBatch(0);
            expected = BatchTestSupport.batchValues(closedFileBatch);
        }
        BatchTestSupport.assertSameValues("batch after reader close", expected,
                BatchTestSupport.batchValues(closedFileBatch));

        // Copy-on-construct: mutating the decoded page storage must not leak into the batch.
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/int32_with_null_pages.parquet")) {
            ColumnValues values = reader.getRowGroup(0).readColumn(0);
            ColumnBatch batch = values.toBatch();
            List<Object> snapshot = BatchTestSupport.batchValues(batch);
            for (DecodedPage page : values.decodedPages()) {
                if (page.values() instanceof int[] dense) {
                    Arrays.fill(dense, 999_999);
                }
            }
            BatchTestSupport.assertSameValues("batch after page mutation", snapshot,
                    BatchTestSupport.batchValues(batch));
        }
    }

    @Test
    void batchNumericViewsAreReadOnly() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/alltypes_dictionary.parquet")) {
            ColumnBatch batch = reader.getRowGroup(0).readColumnBatch(0);
            assertTrue(batch.intValues().isReadOnly());
            assertThrows(ReadOnlyBufferException.class, () -> batch.intValues().put(0, 7));
        }
    }

    @Test
    void dictionaryIndexesSurviveWiringUntilMaterialization() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/alltypes_dictionary.parquet")) {
            ColumnValues values = reader.getRowGroup(0).readColumn(0);
            ColumnBatch batch = values.toBatch();
            assertTrue(batch.isDictionaryEncoded(), "uniform dictionary chunk keeps its indexes");
            int[] expected = pageIndexes(values, batch.size());
            int[] actual = BatchTestSupport.toArray(batch.dictionaryIndices());
            assertArrayEquals(expected, actual, "dictionary indexes preserved verbatim");
            BatchTestSupport.assertSameValues("dictionary materialization",
                    BatchTestSupport.flatList(values, Type.INT32), BatchTestSupport.batchValues(batch));
        }
        // Sparse nulls: present rows keep their index, null rows are -1 slots.
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/datapage_v2.snappy.parquet")) {
            ColumnValues values = reader.getRowGroup(0).readColumn(0);
            ColumnBatch batch = values.toBatch();
            assertTrue(batch.isDictionaryEncoded());
            assertEquals("abc", new String(BatchTestSupport.rawBytes(batch.getBytes(0)),
                    java.nio.charset.StandardCharsets.UTF_8));
            assertTrue(batch.isNull(3));
            assertEquals(-1, batch.dictionaryIndices().get(3));
            assertEquals(batch.dictionaryIndices().get(0), batch.dictionaryIndices().get(4),
                    "the value reuses the first row's dictionary entry");
        }
    }

    /**
     * Concatenated per-page dictionary indexes at present events (null events become -1).
     */
    private static int[] pageIndexes(ColumnValues values, int size) {
        int[] indexes = new int[size];
        Arrays.fill(indexes, -1);
        int pos = 0;
        int maxDefinition = values.getColumnDescriptor().maxDefinitionLevel();
        for (DecodedPage page : values.decodedPages()) {
            int physical = 0;
            for (int event = 0; event < page.numValues(); event++) {
                if (page.definitionLevel(event) == maxDefinition) {
                    indexes[pos] = page.dictionaryIndices()[physical++];
                }
                pos++;
            }
        }
        return indexes;
    }

    @Test
    void mixedDictionaryAndPlainPagesMaterializeBoth() {
        ColumnDescriptor descriptor = BatchTestSupport.descriptor(Type.INT32, 0, 0);
        List<Page> pages = new ArrayList<>();
        pages.add(new Page.DictionaryPage(DecodingTestSupport.plain(Type.INT32, 10, 20), 2,
                Encoding.PLAIN_DICTIONARY));
        pages.add(dictionaryPage(descriptor, 0, 1));
        pages.add(DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{0, 0}, new int[]{0, 0}, DecodingTestSupport.plain(Type.INT32, 30, 40)));
        ColumnValues values = new ColumnValues(Type.INT32, pages, descriptor, logical(descriptor));
        ColumnBatch batch = values.toBatch();
        assertFalse(batch.isDictionaryEncoded(), "mixed chunks materialize both encodings");
        assertEquals(List.of(10, 20, 30, 40), values.decodeAsInt32());
        BatchTestSupport.assertSameValues("mixed chunk", List.of(10, 20, 30, 40),
                BatchTestSupport.batchValues(batch));
    }

    @Test
    void uniformDictionaryChunkPreservesIndexesAcrossPages() {
        ColumnDescriptor descriptor = BatchTestSupport.descriptor(Type.INT32, 0, 0);
        List<Page> pages = new ArrayList<>();
        pages.add(new Page.DictionaryPage(DecodingTestSupport.plain(Type.INT32, 10, 20), 2,
                Encoding.PLAIN_DICTIONARY));
        pages.add(dictionaryPage(descriptor, 0, 1));
        pages.add(dictionaryPage(descriptor, 1, 0));
        ColumnValues values = new ColumnValues(Type.INT32, pages, descriptor, logical(descriptor));
        ColumnBatch batch = values.toBatch();
        assertTrue(batch.isDictionaryEncoded());
        assertArrayEquals(new int[]{0, 1, 1, 0}, BatchTestSupport.toArray(batch.dictionaryIndices()));
        assertEquals(List.of(10, 20, 20, 10), values.decodeAsInt32());
        BatchTestSupport.assertSameValues("uniform dict chunk", List.of(10, 20, 20, 10),
                BatchTestSupport.batchValues(batch));
        assertEquals(2, values.toPageBatches().size());
        for (ColumnBatch pageBatch : values.toPageBatches()) {
            assertTrue(pageBatch.isDictionaryEncoded());
        }
    }

    /**
     * One PLAIN_DICTIONARY data page whose RLE runs are one value each (width 1).
     */
    private static Page dictionaryPage(ColumnDescriptor descriptor, int... indexes) {
        ByteBuffer levels = DecodingTestSupport.levels(1, indexes);
        ByteBuffer values = ByteBuffer.allocate(1 + levels.remaining());
        values.put((byte) 1).put(levels).flip();
        return DecodingTestSupport.page(false, descriptor, Encoding.PLAIN_DICTIONARY,
                new int[]{0, 0}, new int[]{0, 0}, values);
    }

    private static LogicalColumnDescriptor logical(ColumnDescriptor descriptor) {
        return new LogicalColumnDescriptor(descriptor.getPathString(), LogicalType.PRIMITIVE,
                descriptor.physicalType(), descriptor);
    }

    @Test
    void requiredUnboxedRouteProducesTypedArraysAndRejectsOptionalColumns() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/delta_encoding_required_column.parquet")) {
            ColumnValues required = reader.getRowGroup(0).readColumn(0);
            assertTrue(required.isRequiredNonRepeated());
            Object dense = required.decodeRequiredUnboxed();
            int[] values = (int[]) dense;
            assertEquals(required.decodeAsInt32().size(), values.length);
            List<Integer> boxed = required.decodeAsInt32();
            for (int i = 0; i < values.length; i++) {
                assertEquals(boxed.get(i).intValue(), values[i], "unboxed value at " + i);
            }
        }
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/delta_encoding_optional_column.parquet")) {
            ColumnValues optional = reader.getRowGroup(0).readColumn(1);
            assertFalse(optional.isRequiredNonRepeated());
            assertThrows(ParquetException.class, optional::decodeRequiredUnboxed);
        }
    }

    @Test
    void requiredBinaryColumnsUseSharedOffsetsAndPayload() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/delta_encoding_required_column.parquet")) {
            ColumnValues values = reader.getRowGroup(0).readColumn(9);
            Object dense = values.decodeRequiredUnboxed();
            byte[][] raw = (byte[][]) dense;
            List<byte[]> boxed = values.decodeAsByteArray();
            assertEquals(boxed.size(), raw.length);
            for (int i = 0; i < raw.length; i++) {
                assertArrayEquals(boxed.get(i), raw[i], "binary value at " + i);
            }
            ColumnBatch batch = values.toBatch();
            BatchTestSupport.assertSameValues("required binary batch", new ArrayList<>(boxed),
                    BatchTestSupport.batchValues(batch));
        }
    }

    @Test
    void pageBatchesAgreeWithReaderLevelWiring() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/int32_with_null_pages.parquet")) {
            ParquetFileReader.RowGroupReader group = reader.getRowGroup(0);
            List<ColumnBatch> pageBatches = group.readColumnPageBatches(0);
            assertTrue(pageBatches.size() > 1, "fixture chunk spans pages");
            List<Object> combined = new ArrayList<>();
            for (ColumnBatch pageBatch : pageBatches) {
                combined.addAll(BatchTestSupport.batchValues(pageBatch));
            }
            BatchTestSupport.assertSameValues("reader page batches",
                    BatchTestSupport.batchValues(group.readColumnBatch(0)), combined);
        }
    }
}
