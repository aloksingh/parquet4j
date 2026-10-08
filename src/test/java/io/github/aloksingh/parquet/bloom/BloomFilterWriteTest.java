package io.github.aloksingh.parquet.bloom;

import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ParquetFileWriter;
import io.github.aloksingh.parquet.ReadOptions;
import io.github.aloksingh.parquet.WriteOptions;
import io.github.aloksingh.parquet.model.*;
import io.github.aloksingh.parquet.util.filter.ColumnEqualFilter;
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilterSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterWriteTest {

    @TempDir
    Path tempDir;

    private static LogicalColumnDescriptor primitive(String name, Type type, boolean optional) {
        return new LogicalColumnDescriptor(name, LogicalType.PRIMITIVE, type,
                new ColumnDescriptor(type, new String[]{name}, optional ? 1 : 0, 0, 0,
                        type == Type.BYTE_ARRAY ? io.github.aloksingh.parquet.model.PrimitiveLogicalType.string()
                                : io.github.aloksingh.parquet.model.PrimitiveLogicalType.none()));
    }

    private static SchemaDescriptor schemaOf(LogicalColumnDescriptor... columns) {
        return SchemaDescriptor.fromLogicalColumns("test", List.of(columns));
    }

    // ------------------------------------------------------------------ basic round-trip

    @Test
    void writeWithBloomFilterThenReadBack() throws IOException {
        var nameCol = primitive("name", Type.BYTE_ARRAY, false);
        var ageCol = primitive("age", Type.INT32, false);
        SchemaDescriptor schema = schemaOf(nameCol, ageCol);

        Path file = tempDir.resolve("bloom_write_test.parquet");

        WriteOptions opts = WriteOptions.builder()
                .bloomFilter("name", 100, 0.01)
                .bloomFilter("age", 100, 0.01)
                .build();

        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema, opts)) {
            for (int i = 0; i < 50; i++) {
                final int ii = i;
                writer.addRow(new SimpleRowColumnGroup(schema,
                        idx -> idx == 0 ? "person-" + ii : ii + 20));
            }
        }

        try (ParquetFileReader reader = new ParquetFileReader(file.toString())) {
            ParquetFileReader.RowGroupReader rg = reader.getRowGroup(0);

            SplitBlockBloomFilter bfName = rg.getBloomFilter(0);
            SplitBlockBloomFilter bfAge = rg.getBloomFilter(1);
            assertNotNull(bfName);
            assertNotNull(bfAge);

            assertTrue(bfName.mightContain("person-5".getBytes(StandardCharsets.UTF_8)));
            assertFalse(bfName.mightContain("nonexistent".getBytes(StandardCharsets.UTF_8)));

            byte[] age25 = int32LE(25);
            assertTrue(bfAge.mightContain(age25));
            byte[] age99 = int32LE(99);
            assertFalse(bfAge.mightContain(age99));

            ParquetMetadata meta = reader.getMetadata();
            assertTrue(meta.rowGroups().get(0).columns().get(0).hasBloomFilter());
            assertTrue(meta.rowGroups().get(0).columns().get(1).hasBloomFilter());
        }
    }

    // ------------------------------------------------------------------ pruning with written data

    @Test
    void bloomFilterPruningWithWrittenFile() throws IOException {
        var idCol = primitive("id", Type.INT64, false);
        var labelCol = primitive("label", Type.BYTE_ARRAY, false);
        SchemaDescriptor schema = schemaOf(idCol, labelCol);

        Path file = tempDir.resolve("bloom_prune_write.parquet");

        WriteOptions opts = WriteOptions.builder()
                .bloomFilter("id", 100, 0.01)
                .bloomFilter("label", 100, 0.01)
                .build();

        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema, opts)) {
            for (int i = 0; i < 100; i++) {
                final long idVal = (long) i * 10;
                final String labelVal = "label-" + (i % 20);
                writer.addRow(new SimpleRowColumnGroup(schema,
                        idx -> idx == 0 ? idVal : labelVal));
            }
        }

        try (ParquetFileReader reader = new ParquetFileReader(file.toString())) {
            var labelDesc = reader.getSchema().getLogicalColumn("label");
            var idDesc = reader.getSchema().getLogicalColumn("id");

            // Present
            RowColumnGroupFilterSet presentFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(labelDesc, "label-5"));
            assertTrue(countRows(reader, ReadOptions.builder()
                    .filter(presentFilter).pruning(true).build()) >= 1);

            // Absent
            RowColumnGroupFilterSet absentFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(labelDesc, "label-this-does-not-exist"));
            assertEquals(0, countRows(reader, ReadOptions.builder()
                    .filter(absentFilter).pruning(true).build()));

            // ID present
            RowColumnGroupFilterSet idFilter = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(idDesc, 150L));
            assertEquals(1, countRows(reader, ReadOptions.builder()
                    .filter(idFilter).pruning(true).build()));

            // ID absent
            RowColumnGroupFilterSet idAbsent = new RowColumnGroupFilterSet(FilterJoinType.All,
                    new ColumnEqualFilter(idDesc, 9999L));
            assertEquals(0, countRows(reader, ReadOptions.builder()
                    .filter(idAbsent).pruning(true).build()));
        }
    }

    // ------------------------------------------------------------------ multiple row groups

    @Test
    void bloomFilterPerRowGroup() throws IOException {
        var sCol = primitive("s", Type.BYTE_ARRAY, false);
        SchemaDescriptor schema = schemaOf(sCol);

        Path file = tempDir.resolve("bloom_multi_rg.parquet");

        WriteOptions opts = WriteOptions.builder()
                .rowGroupSize(1024)
                .pageSize(256)
                .bloomFilter("s", 20, 0.01)
                .build();

        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema, opts)) {
            for (int i = 0; i < 200; i++) {
                final String val = "item-" + (i % 30);
                writer.addRow(new SimpleRowColumnGroup(schema, idx -> val));
            }
        }

        try (ParquetFileReader reader = new ParquetFileReader(file.toString())) {
            assertTrue(reader.getNumRowGroups() > 1);
            for (int rg = 0; rg < reader.getNumRowGroups(); rg++) {
                SplitBlockBloomFilter bf = reader.getRowGroup(rg).getBloomFilter(0);
                assertNotNull(bf, "Row group " + rg + " should have bloom filter");
                assertTrue(bf.numBlocks() > 0);
            }
        }
    }

    // ------------------------------------------------------------------ mixed columns

    @Test
    void mixedBloomFilterColumns() throws IOException {
        var a = primitive("a", Type.BYTE_ARRAY, false);
        var b = primitive("b", Type.INT32, false);
        var c = primitive("c", Type.INT64, false);
        SchemaDescriptor schema = schemaOf(a, b, c);

        Path file = tempDir.resolve("bloom_mixed.parquet");

        WriteOptions opts = WriteOptions.builder()
                .bloomFilter("a", 100, 0.01)
                .bloomFilter("c", 100, 0.01)
                .build();

        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema, opts)) {
            for (int i = 0; i < 50; i++) {
                final int ii = i;
                writer.addRow(new SimpleRowColumnGroup(schema,
                        idx -> switch (idx) {
                            case 0 -> "val-" + ii;
                            case 1 -> ii;
                            case 2 -> (long) ii * 10;
                            default -> null;
                        }));
            }
        }

        try (ParquetFileReader reader = new ParquetFileReader(file.toString())) {
            ParquetFileReader.RowGroupReader rg = reader.getRowGroup(0);
            assertNotNull(rg.getBloomFilter(0), "column a");
            assertNull(rg.getBloomFilter(1), "column b should NOT have bloom filter");
            assertNotNull(rg.getBloomFilter(2), "column c");
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