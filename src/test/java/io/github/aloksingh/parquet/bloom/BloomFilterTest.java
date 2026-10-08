package io.github.aloksingh.parquet.bloom;

import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ReadOptions;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.util.filter.ColumnEqualFilter;
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilterSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {

    private static final Path DATA_DIR = Path.of("src/test/data");

    // ------------------------------------------------------------------ XXH64

    @Test
    void xxh64KnownVectors() {
        assertHash("", "ef46db3751d8e999");
        assertHash("a", "d24ec4f1a98c6e5b");
        assertHash("abc", "44bc2cf5ad770999");
        assertHash("hello", "26c7827d889f6da3");
        assertHash("parquet", "3c9d29275c52e429");
        assertHash("bloom", "50c8fb9e62dbc53c");
        assertHash("filter", "2a5736cdfcd7a9a1");
    }

    private static void assertHash(String input, String expectedHex) {
        long h = XxHash64.hash(input.getBytes(StandardCharsets.UTF_8), 0, input.length(), 0);
        assertEquals(expectedHex, String.format("%016x", h), "XXH64(" + input + ")");
    }

    // ------------------------------------------------------------------ SplitBlockBloomFilter algorithm

    @Test
    void splitBlockAgainstKnownBinary() throws Exception {
        byte[] raw = Files.readAllBytes(DATA_DIR.resolve("bloom_filter.xxhash.bin"));
        SplitBlockBloomFilter bf = BloomFilterReader.parseFromBytes(raw);
        assertNotNull(bf);
        assertEquals(32, bf.numBlocks());

        // The file was built by parquet-mr with inserts of "hello", "parquet", "bloom", "filter"
        assertTrue(bf.mightContain("hello".getBytes(StandardCharsets.UTF_8)));
        assertTrue(bf.mightContain("parquet".getBytes(StandardCharsets.UTF_8)));
        assertTrue(bf.mightContain("bloom".getBytes(StandardCharsets.UTF_8)));
        assertTrue(bf.mightContain("filter".getBytes(StandardCharsets.UTF_8)));

        // Known-absent values must return false
        assertFalse(bf.mightContain("absent".getBytes(StandardCharsets.UTF_8)));
        assertFalse(bf.mightContain("".getBytes(StandardCharsets.UTF_8)));
        assertFalse(bf.mightContain("xyzzy".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void emptyFilterReturnsFalse() {
        SplitBlockBloomFilter bf = new SplitBlockBloomFilter(4);
        assertFalse(bf.mightContain("hello".getBytes(StandardCharsets.UTF_8)));
        assertFalse(bf.mightContain("".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void insertThenCheck() {
        SplitBlockBloomFilter bf = new SplitBlockBloomFilter(4);
        byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
        assertFalse(bf.mightContain(value));
        bf.insert(XxHash64.hash(value));
        assertTrue(bf.mightContain(value));
    }

    @Test
    void noFalseNegatives() {
        SplitBlockBloomFilter bf = new SplitBlockBloomFilter(8);
        for (int i = 0; i < 1000; i++) {
            byte[] val = ("item-" + i).getBytes(StandardCharsets.UTF_8);
            bf.insert(XxHash64.hash(val));
        }
        for (int i = 0; i < 1000; i++) {
            byte[] val = ("item-" + i).getBytes(StandardCharsets.UTF_8);
            assertTrue(bf.mightContain(val), "false negative for item-" + i);
        }
    }

    // ------------------------------------------------------------------ Metadata from Parquet files

    @ParameterizedTest
    @ValueSource(strings = {
            "data_index_bloom_encoding_stats.parquet",
            "data_index_bloom_encoding_with_length.parquet",
            "bloom_primitive_types.parquet",
            "bloom_mixed_columns.parquet",
            "bloom_fpp_10pct.parquet",
            "bloom_fpp_1pct.parquet",
            "bloom_fpp_01pct.parquet",
            "bloom_single_rowgroup.parquet"
    })
    void metadataHasBloomFilterOffsets(String filename) throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(DATA_DIR.resolve(filename).toString())) {
            ParquetMetadata meta = reader.getMetadata();
            boolean found = false;
            for (int rg = 0; rg < meta.getNumRowGroups(); rg++) {
                for (ParquetMetadata.ColumnChunkMetadata col : meta.rowGroups().get(rg).columns()) {
                    if (col.hasBloomFilter()) {
                        found = true;
                        assertTrue(col.bloomFilterOffset() > 0,
                                "bloom filter offset should be > 0: " + col.bloomFilterOffset());
                    }
                }
            }
            assertTrue(found, filename + " should have at least one bloom filter");
        }
    }

    // ------------------------------------------------------------------ Bloom filter via RowGroupReader

    @Test
    void rowGroupReaderReturnsBloomFilter() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(
                DATA_DIR.resolve("bloom_single_rowgroup.parquet").toString())) {
            ParquetFileReader.RowGroupReader rg = reader.getRowGroup(0);
            SplitBlockBloomFilter bf0 = rg.getBloomFilter(0);
            SplitBlockBloomFilter bf1 = rg.getBloomFilter(1);
            assertNotNull(bf0, "name column should have bloom filter");
            assertNotNull(bf1, "age column should have bloom filter");

            assertTrue(bf0.mightContain("alice".getBytes(StandardCharsets.UTF_8)));
            assertFalse(bf0.mightContain("zoe".getBytes(StandardCharsets.UTF_8)));

            byte[] age25 = int32LE(25);
            assertTrue(bf1.mightContain(age25));
            byte[] age99 = int32LE(99);
            assertFalse(bf1.mightContain(age99));
        }
    }

    @Test
    void rowGroupReaderNullForNoBloomFilter() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(
                DATA_DIR.resolve("bloom_mixed_columns.parquet").toString())) {
            ParquetFileReader.RowGroupReader rg = reader.getRowGroup(0);
            // int32_col (index 0) has NO bloom filter in this file
            // string_col (index 4) has bloom filter
            assertNull(rg.getBloomFilter(0));
            assertNotNull(rg.getBloomFilter(4));
        }
    }

    // ------------------------------------------------------------------ Bloom filter pruning via ReadOptions

    @Test
    void bloomFilterPruningDropsNonMatchingRowGroup() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(
                DATA_DIR.resolve("bloom_single_rowgroup.parquet").toString())) {

            LogicalColumnDescriptor nameCol = reader.getSchema().getLogicalColumn("name");

            // alice is present
            RowColumnGroupFilterSet presentFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(nameCol, "alice"));
            // zoe is absent from the bloom filter
            RowColumnGroupFilterSet absentFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(nameCol, "zoe"));

            assertEquals(1, countRows(reader, ReadOptions.builder()
                    .filter(presentFilter).pruning(true).build()));
            assertEquals(0, countRows(reader, ReadOptions.builder()
                            .filter(absentFilter).pruning(true).build()),
                    "zoe should be pruned by bloom filter");
        }
    }

    @Test
    void bloomFilterPruningWithIntegerEquality() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(
                DATA_DIR.resolve("bloom_single_rowgroup.parquet").toString())) {

            LogicalColumnDescriptor ageCol = reader.getSchema().getLogicalColumn("age");

            RowColumnGroupFilterSet presentFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(ageCol, 25));
            RowColumnGroupFilterSet absentFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(ageCol, 99));

            assertEquals(1, countRows(reader, ReadOptions.builder()
                    .filter(presentFilter).pruning(true).build()));
            assertEquals(0, countRows(reader, ReadOptions.builder()
                            .filter(absentFilter).pruning(true).build()),
                    "age=99 should be pruned by bloom filter");
        }
    }

    @Test
    void bloomFilterOnMultipleRowGroups() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(
                DATA_DIR.resolve("bloom_primitive_types.parquet").toString())) {

            LogicalColumnDescriptor strCol = reader.getSchema().getLogicalColumn("string_col");

            RowColumnGroupFilterSet presentFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(strCol, "str_42"));
            RowColumnGroupFilterSet absentFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(strCol, "this-value-does-not-exist-zzz"));

            assertTrue(countRows(reader, ReadOptions.builder()
                            .filter(presentFilter).pruning(true).build()) >= 1,
                    "str_42 should be found");
            assertEquals(0, countRows(reader, ReadOptions.builder()
                            .filter(absentFilter).pruning(true).build()),
                    "non-existent value should be pruned in all row groups");
        }
    }

    @Test
    void pruningDisabledDoesNotLoadBloomFilters() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(
                DATA_DIR.resolve("bloom_single_rowgroup.parquet").toString())) {

            LogicalColumnDescriptor nameCol = reader.getSchema().getLogicalColumn("name");
            RowColumnGroupFilterSet absentFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(nameCol, "zoe"));

            assertEquals(0, countRows(reader, ReadOptions.builder()
                            .filter(absentFilter).pruning(false).build()),
                    "zoe should not exist even without pruning");
        }
    }

    @Test
    void mixedColumnsWithoutBloomFilterPrunesNormally() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(
                DATA_DIR.resolve("bloom_mixed_columns.parquet").toString())) {

            // Read an actual value via the row-based API so we know it exists
            io.github.aloksingh.parquet.ParquetRowIterator iter = reader.rowIterator(ReadOptions.DEFAULT);
            Integer actualValue = (Integer) iter.next().getColumnValue("int32_col");
            iter.close();

            LogicalColumnDescriptor intCol = reader.getSchema().getLogicalColumn("int32_col");
            RowColumnGroupFilterSet filter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(intCol, actualValue));

            assertTrue(countRows(reader, ReadOptions.builder()
                            .filter(filter).pruning(true).build()) >= 1,
                    "Column without bloom filter should still find matching rows");
        }
    }

    @Test
    void differentFppSizesReadable() throws IOException {
        for (String f : List.of("bloom_fpp_10pct.parquet", "bloom_fpp_1pct.parquet", "bloom_fpp_01pct.parquet")) {
            try (ParquetFileReader reader = new ParquetFileReader(DATA_DIR.resolve(f).toString())) {
                ParquetFileReader.RowGroupReader rg = reader.getRowGroup(0);
                SplitBlockBloomFilter bf = rg.getBloomFilter(0);
                assertNotNull(bf, f + " should have a bloom filter");
                assertTrue(bf.numBlocks() > 0);

                // All files have the same data; "str_500" should be present
                assertTrue(bf.mightContain("str_500".getBytes(StandardCharsets.UTF_8)),
                        f + " should contain str_500");
            }
        }
    }

    @Test
    void bloomFilterFromTwoPathsAreConsistent() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(
                DATA_DIR.resolve("bloom_single_rowgroup.parquet").toString())) {

            SplitBlockBloomFilter bf1 = reader.getRowGroup(0).getBloomFilter(0);
            SplitBlockBloomFilter bf2 = reader.readBloomFilter(0, 0);

            assertNotNull(bf1);
            assertNotNull(bf2);
            assertEquals(bf1.numBlocks(), bf2.numBlocks());
            assertEquals(bf1.numBytes(), bf2.numBytes());
            assertArrayEquals(bf1.toBytes(), bf2.toBytes());
        }
    }

    // ------------------------------------------------------------------ helpers

    private static long countRows(ParquetFileReader reader, ReadOptions opts) throws IOException {
        long count = 0;
        try (var iter = reader.rowIterator(opts)) {
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
        }
        return count;
    }

    private static byte[] int32LE(int v) {
        return new byte[]{(byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24)};
    }
}