package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.aloksingh.parquet.model.ColumnBatch;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Equivalence of the primitive columnar batch API with the boxed ColumnValues list
 * adapters and the row API across encodings and null patterns: same count, order,
 * null placement, and exact values (binary compared as raw bytes).
 *
 * <p>Encoding x null-pattern coverage matrix. Cells marked [W] are generated with
 * the project writer (PLAIN family only — the writer emits PLAIN; dictionary and
 * delta fixtures are read-only); cells marked [F] come from src/test/data fixtures.
 * "absent" means no fixture exists and the writer cannot produce the cell.
 *
 * <pre>
 *                        required        optional        optional        optional        nested
 *                                                        no-nulls        sparse          all-null        null elements
 * PLAIN                  [W:req_*        [W:opt_bin      [W:opt_*        [W:allnull_*    [F:nullable.impala
 *                        [F:nonnullable  [F:binary.foo   [F:int32_with_  [F:nulls.snappy  nested_struct.b
 *                        .impala ID,     [F:alltypes_    _null_pages     b_struct.b_c_   .list.element
 *                        nested_Struct   plain bool_col] plain.int32_    int]            [F:nonnullable
 *                        .a]                     field]                          impala nested_Struct
 *                                                                                .B.list.element]
 * PLAIN_DICTIONARY /     [F:datapage_    [F:alltypes_    [F:datapage_    absent          [F:nullable.impala
 * RLE_DICTIONARY         v2 c]           dictionary id,  v2 a]                           int_array.list
 *                                        string_col,                                     .element
 *                                        timestamp_col]         [F:list_columns int64_list
 *                                                                                .list.item,
 *                                                                                utf8_list.list.item,
 *                                                                                datapage_v2 e.list.element]
 * DELTA_BINARY_PACKED    [F:delta_       [F:delta_       [F:delta_       absent          absent (delta
 *                        encoding_       binary_packed   encoding_                       encodings appear
 *                        required_column int_value]      optional_column                 only on flat
 *                        c_customer_sk:,                 c_current_cdemo_sk]             leaves in the
 *                        c_birth_year:,                                                  available fixtures)
 *                        datapage_v2 b]
 * DELTA_LENGTH_BYTE_     absent          [F:delta_       absent          absent          absent
 * ARRAY                                  length_byte_
 *                                        array FRUIT]
 * DELTA_BYTE_ARRAY       [F:delta_       [F:delta_byte_  [F:delta_byte_  [F:delta_byte_  absent
 *                        encoding_       array           array           array
 *                        required_column c_customer_id]  c_first_name]   c_login]
 *                        c_customer_id,
 *                        c_first_name]
 * BYTE_STREAM_SPLIT      absent          [F:byte_stream_  absent          absent          absent
 *                                        split.zstd f32,
 *                                        f64, extended
 *                                        int32/f5 flba]
 * RLE boolean            [F:datapage_    absent          [F:rle_boolean_ absent          absent
 *                        v2 d]                           encoding
 *                                                        datatype_boolean]
 * </pre>
 *
 * <p>Row-API equality is asserted for every non-repeated cell (one row value per
 * level event). For repeated/nested cells the batch holds one entry per level
 * event while rows hold containers; that leg is asserted in
 * {@link #rowApiNestedContainersMatchListContainersWhenSurfaced()}.
 */
class BatchEquivalenceTest {

    record Cell(String file, String columnPath, String encoding, String nullPattern) {
    }

    static List<Cell> fixtureCells() {
        return List.of(
                // PLAIN
                new Cell("nonnullable.impala.parquet", "ID", "PLAIN", "required"),
                new Cell("nonnullable.impala.parquet", "nested_Struct.a", "PLAIN", "required"),
                new Cell("binary.parquet", "foo", "PLAIN", "optional-no-nulls"),
                new Cell("alltypes_plain.parquet", "bool_col", "PLAIN", "optional-no-nulls"),
                new Cell("int32_with_null_pages.parquet", "int32_field", "PLAIN", "optional-sparse"),
                new Cell("nullable.impala.parquet", "nested_struct.A", "PLAIN", "optional-sparse"),
                new Cell("nulls.snappy.parquet", "b_struct.b_c_int", "PLAIN", "optional-all-null"),
                new Cell("nullable.impala.parquet", "nested_struct.b.list.element", "PLAIN", "nested-nulls"),
                new Cell("nonnullable.impala.parquet", "nested_Struct.B.list.element", "PLAIN", "nested"),
                // PLAIN_DICTIONARY / RLE_DICTIONARY
                new Cell("datapage_v2.snappy.parquet", "c", "RLE_DICTIONARY", "required"),
                new Cell("alltypes_dictionary.parquet", "id", "PLAIN_DICTIONARY", "optional-no-nulls"),
                new Cell("alltypes_dictionary.parquet", "string_col", "PLAIN_DICTIONARY", "optional-no-nulls"),
                new Cell("alltypes_dictionary.parquet", "timestamp_col", "PLAIN_DICTIONARY", "optional-no-nulls"),
                new Cell("datapage_v2.snappy.parquet", "a", "RLE_DICTIONARY", "optional-sparse"),
                new Cell("nullable.impala.parquet", "int_array.list.element", "PLAIN_DICTIONARY", "nested-nulls"),
                new Cell("nullable.impala.parquet", "nested_struct.C.d.list.element.list.element.E",
                        "PLAIN_DICTIONARY", "nested-nulls"),
                new Cell("list_columns.parquet", "int64_list.list.item", "PLAIN_DICTIONARY", "nested-nulls"),
                new Cell("list_columns.parquet", "utf8_list.list.item", "PLAIN_DICTIONARY", "nested-nulls"),
                new Cell("datapage_v2.snappy.parquet", "e.list.element", "RLE_DICTIONARY", "nested-nulls"),
                // DELTA_BINARY_PACKED
                new Cell("delta_encoding_required_column.parquet", "c_customer_sk:", "DELTA_BINARY_PACKED", "required"),
                new Cell("delta_encoding_required_column.parquet", "c_birth_year:", "DELTA_BINARY_PACKED", "required"),
                new Cell("datapage_v2.snappy.parquet", "b", "DELTA_BINARY_PACKED", "required"),
                new Cell("delta_binary_packed.parquet", "int_value", "DELTA_BINARY_PACKED", "optional-no-nulls"),
                new Cell("delta_encoding_optional_column.parquet", "c_current_cdemo_sk", "DELTA_BINARY_PACKED", "optional-sparse"),
                // DELTA_LENGTH_BYTE_ARRAY
                new Cell("delta_length_byte_array.parquet", "FRUIT", "DELTA_LENGTH_BYTE_ARRAY", "optional-no-nulls"),
                // DELTA_BYTE_ARRAY
                new Cell("delta_encoding_required_column.parquet", "c_customer_id:", "DELTA_BYTE_ARRAY", "required"),
                new Cell("delta_encoding_required_column.parquet", "c_first_name:", "DELTA_BYTE_ARRAY", "required"),
                new Cell("delta_byte_array.parquet", "c_customer_id", "DELTA_BYTE_ARRAY", "optional-no-nulls"),
                new Cell("delta_byte_array.parquet", "c_first_name", "DELTA_BYTE_ARRAY", "optional-sparse"),
                new Cell("delta_byte_array.parquet", "c_login", "DELTA_BYTE_ARRAY", "optional-all-null"),
                // BYTE_STREAM_SPLIT
                new Cell("byte_stream_split.zstd.parquet", "f32", "BYTE_STREAM_SPLIT", "optional-no-nulls"),
                new Cell("byte_stream_split.zstd.parquet", "f64", "BYTE_STREAM_SPLIT", "optional-no-nulls"),
                new Cell("byte_stream_split_extended.gzip.parquet", "int32_byte_stream_split",
                        "BYTE_STREAM_SPLIT", "optional-no-nulls"),
                new Cell("byte_stream_split_extended.gzip.parquet", "flba5_byte_stream_split",
                        "BYTE_STREAM_SPLIT", "optional-no-nulls"),
                // RLE boolean
                new Cell("datapage_v2.snappy.parquet", "d", "RLE", "required"),
                new Cell("rle_boolean_encoding.parquet", "datatype_boolean", "RLE", "optional-sparse"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixtureCells")
    void fixtureColumnsAgreeAcrossBatchListAndRowViews(Cell cell) throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(Path.of("src/test/data", cell.file()))) {
            int column = findColumn(reader, cell.columnPath());
            ColumnDescriptor descriptor = reader.getSchema().getColumn(column);
            verifyCoverageClaim(cell, reader, column);
            String label = cell.file() + ":" + cell.columnPath();
            List<Object> flat = BatchTestSupport.flatListAcrossRowGroups(reader, column);
            List<Object> batch = BatchTestSupport.batchValuesAcrossRowGroups(reader, column);
            BatchTestSupport.assertSameValues(label + " batch==list", flat, batch);
            if (descriptor.physicalType() == Type.INT96) {
                BatchTestSupport.assertRowApiRejectsColumn(reader, column);
            } else if (descriptor.maxRepetitionLevel() == 0) {
                List<Object> rows = BatchTestSupport.rowValues(reader, column);
                BatchTestSupport.assertRowValuesMatchList(label + " row==list", descriptor, flat, rows);
            }
        }
    }

    /**
     * Self-check: each matrix cell really holds the encoding and null pattern it claims.
     */
    private static void verifyCoverageClaim(Cell cell, ParquetFileReader reader, int column) throws IOException {
        ColumnDescriptor descriptor = reader.getSchema().getColumn(column);
        List<Object> flat = BatchTestSupport.flatListAcrossRowGroups(reader, column);
        for (int group = 0; group < reader.getNumRowGroups(); group++) {
            for (Page page : reader.getRowGroup(group).readColumn(column).getPages()) {
                if (page instanceof Page.DataPage dataPage) {
                    assertEncoding(cell, dataPage.encoding());
                } else if (page instanceof Page.DataPageV2 dataPage) {
                    assertEncoding(cell, dataPage.encoding());
                }
            }
        }
        long nulls = flat.stream().filter(value -> value == null).count();
        switch (cell.nullPattern()) {
            case "required" -> {
                assertEquals(0, descriptor.maxDefinitionLevel(), cell + ": required columns have no definition levels");
                assertEquals(0, descriptor.maxRepetitionLevel(), cell + ": required cells are nonrepeated here");
                assertEquals(0, nulls, cell + ": required columns hold no nulls");
            }
            case "optional-no-nulls" -> {
                assertTrue(descriptor.maxDefinitionLevel() > 0, cell + ": optional columns carry definition levels");
                assertEquals(0, descriptor.maxRepetitionLevel());
                assertEquals(0, nulls, cell + ": claimed no nulls");
            }
            case "optional-sparse" -> {
                assertTrue(descriptor.maxDefinitionLevel() > 0);
                assertEquals(0, descriptor.maxRepetitionLevel());
                assertTrue(nulls > 0 && nulls < flat.size(), cell + ": claimed sparse nulls, found " + nulls);
            }
            case "optional-all-null" -> {
                assertTrue(descriptor.maxDefinitionLevel() > 0);
                assertEquals(0, descriptor.maxRepetitionLevel());
                assertEquals(flat.size(), nulls, cell + ": claimed all-null");
                assertFalse(flat.isEmpty());
            }
            case "nested", "nested-nulls" -> {
                assertTrue(descriptor.maxRepetitionLevel() > 0, cell + ": nested cells are repeated");
                assertTrue(nulls > 0 || cell.nullPattern().equals("nested"), cell + ": claimed nested nulls");
            }
            default -> throw new AssertionError("Unknown null-pattern claim: " + cell.nullPattern());
        }
    }

    private static void assertEncoding(Cell cell, Encoding encoding) {
        boolean matches = switch (cell.encoding()) {
            case "PLAIN" -> encoding == Encoding.PLAIN;
            case "PLAIN_DICTIONARY" -> encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY;
            case "RLE_DICTIONARY" -> encoding == Encoding.RLE_DICTIONARY;
            case "RLE" -> encoding == Encoding.RLE;
            default -> encoding.name().equals(cell.encoding());
        };
        assertTrue(matches, cell + ": found page encoding " + encoding + ", claimed " + cell.encoding());
    }

    private static int findColumn(ParquetFileReader reader, String path) {
        for (int i = 0; i < reader.getSchema().getNumColumns(); i++) {
            if (reader.getSchema().getColumn(i).getPathString().equals(path)) {
                return i;
            }
        }
        throw new AssertionError("Fixture column not found: " + path);
    }

    // ===== Writer-generated PLAIN cells: required, optional sparse, optional all-null =====

    @Test
    void writerGeneratedPlainFileAgreesAcrossBatchListAndRowViews(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("batch_equiv_plain.parquet");
        SchemaWriter writer = SchemaWriter.plain(file);
        for (int i = 0; i < 2000; i++) {
            writer.addRow(i);
        }
        writer.close();
        assertTripleEquivalence(file);
    }

    @Test
    void writerGeneratedMultiRowGroupFileAgreesAcrossRowGroups(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("batch_equiv_groups.parquet");
        SchemaWriter writer = SchemaWriter.rowGroups(file);
        for (int i = 0; i < 200; i++) {
            writer.addRow(i);
        }
        writer.close();
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            assertTrue(reader.getNumRowGroups() > 1, "expected several row groups in " + file);
        }
        assertTripleEquivalence(file);
    }

    /**
     * Batch == list == row values for every column of a writer file, across row groups.
     */
    private static void assertTripleEquivalence(Path file) throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            for (int column = 0; column < reader.getSchema().getNumColumns(); column++) {
                ColumnDescriptor descriptor = reader.getSchema().getColumn(column);
                String label = file.getFileName() + ":" + descriptor.getPathString();
                List<Object> flat = BatchTestSupport.flatListAcrossRowGroups(reader, column);
                List<Object> batch = BatchTestSupport.batchValuesAcrossRowGroups(reader, column);
                BatchTestSupport.assertSameValues(label + " batch==list", flat, batch);
                List<Object> rows = BatchTestSupport.rowValues(reader, column);
                BatchTestSupport.assertRowValuesMatchList(label + " row==list", descriptor, flat, rows);
                if (descriptor.maxDefinitionLevel() == 0 && descriptor.maxRepetitionLevel() == 0) {
                    // The unboxed route must agree with the boxed list for required columns.
                    List<Object> unboxed = new ArrayList<>(flat.size());
                    for (int group = 0; group < reader.getNumRowGroups(); group++) {
                        ColumnValues values = reader.getRowGroup(group).readColumn(column);
                        Object dense = values.decodeRequiredUnboxed();
                        for (int i = 0; i < java.lang.reflect.Array.getLength(dense); i++) {
                            unboxed.add(java.lang.reflect.Array.get(dense, i));
                        }
                    }
                    BatchTestSupport.assertSameValues(label + " unboxed==list", flat, unboxed);
                }
            }
        }
    }

    // ===== Nested cells: batch == flat list == documented containers =====

    @Test
    void nestedNullElementsAgreeWithDocumentedContainers() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/nullable.impala.parquet")) {
            int column = findColumn(reader, "nested_struct.b.list.element");
            ColumnValues values = reader.getRowGroup(0).readColumn(column);
            List<Object> flat = BatchTestSupport.flatList(values, Type.INT32);
            BatchTestSupport.assertSameValues("nullable.impala nested batch==list",
                    flat, BatchTestSupport.batchValues(reader.getRowGroup(0).readColumnBatch(column)));
            // Documented PyArrow oracle (also asserted by DecodingFixtureTest).
            List<List<Integer>> expected = Arrays.asList(List.of(1), Arrays.asList((Integer) null),
                    null, null, null, null, Arrays.asList(2, 3, null));
            assertEquals(expected, values.decodeAsList(2, 3, value -> (Integer) value));
        }
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/list_columns.parquet")) {
            for (int column = 0; column < 2; column++) {
                ColumnValues values = reader.getRowGroup(0).readColumn(column);
                List<Object> flat = BatchTestSupport.flatList(values,
                        reader.getSchema().getColumn(column).physicalType());
                BatchTestSupport.assertSameValues("list_columns batch==list column " + column,
                        flat, BatchTestSupport.batchValues(reader.getRowGroup(0).readColumnBatch(column)));
            }
            List<List<Long>> numbers = reader.getRowGroup(0).readColumn(0)
                    .decodeAsList(1, 2, value -> (Long) value);
            assertEquals(Arrays.asList(List.of(1L, 2L, 3L), Arrays.asList((Long) null, 1L), List.of(4L)), numbers);
            List<List<String>> strings = reader.getRowGroup(0).readColumn(1)
                    .decodeAsList(1, 2, value -> new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8));
            assertEquals(Arrays.asList(List.of("abc", "efg", "hij"), null,
                    Arrays.asList("efg", null, "hij", "xyz")), strings);
        }
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/datapage_v2.snappy.parquet")) {
            ColumnValues values = reader.getRowGroup(0).readColumn(4);
            List<Object> flat = BatchTestSupport.flatList(values, Type.INT32);
            BatchTestSupport.assertSameValues("datapage_v2 e.list batch==list",
                    flat, BatchTestSupport.batchValues(reader.getRowGroup(0).readColumnBatch(4)));
            assertEquals(Arrays.asList(List.of(1, 2, 3), null, null, List.of(1, 2, 3), List.of(1, 2)),
                    values.decodeAsList(1, 2, value -> (Integer) value));
        }
    }

    /**
     * Row-API leg for nested cells: when the row iterator surfaces one container per
     * row, its flattened elements must equal the list containers' elements. The row
     * API currently surfaces nested columns as flat events (its nested containers are
     * owned outside this change), so this leg is guarded and reports itself as skipped
     * until that representation lands.
     */
    @Test
    void rowApiNestedContainersMatchListContainersWhenSurfaced() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/list_columns.parquet")) {
            ColumnValues values = reader.getRowGroup(0).readColumn(0);
            List<Object> rows = BatchTestSupport.rowValues(reader, 0);
            assumeTrue(BatchTestSupport.rowSurfaceIsContainerBased(rows),
                    "row API surfaces nested columns as flat events, not containers");
            List<List<Object>> containers = BatchTestSupport.listContainers(values, 1, 2);
            assertEquals(containers.size(), rows.size(), "row count");
            for (int i = 0; i < containers.size(); i++) {
                assertEquals(containers.get(i), rows.get(i), "row container at " + i);
            }
        }
    }

    /**
     * Page batches concatenate to exactly the whole-chunk batch (multi-page chunks).
     */
    @Test
    void pageBatchesConcatenateToTheWholeChunkBatch() throws IOException {
        for (String fixture : new String[]{"int32_with_null_pages.parquet", "alltypes_tiny_pages.parquet"}) {
            try (ParquetFileReader reader = new ParquetFileReader("src/test/data/" + fixture)) {
                for (int column = 0; column < reader.getSchema().getNumColumns(); column++) {
                    ParquetFileReader.RowGroupReader group = reader.getRowGroup(0);
                    ColumnValues values = group.readColumn(column);
                    List<ColumnBatch> pageBatches = values.toPageBatches();
                    if (pageBatches.size() < 2) {
                        continue; // single-page chunks are covered by the cell matrix
                    }
                    List<Object> combined = new ArrayList<>();
                    for (ColumnBatch pageBatch : pageBatches) {
                        combined.addAll(BatchTestSupport.batchValues(pageBatch));
                    }
                    String label = fixture + ":" + reader.getSchema().getColumn(column).getPathString();
                    BatchTestSupport.assertSameValues(label + " pageBatches==batch",
                            BatchTestSupport.batchValues(group.readColumnBatch(column)), combined);
                    BatchTestSupport.assertSameValues(label + " pageBatches==list",
                            BatchTestSupport.flatList(values, reader.getSchema().getColumn(column).physicalType()), combined);
                }
            }
        }
    }

    /**
     * Minimal writer harness: PLAIN-only writer with required, sparse-null and all-null columns.
     */
    private static final class SchemaWriter {
        private final ParquetFileWriter writer;
        private final SchemaDescriptor schema;

        private SchemaWriter(ParquetFileWriter writer, SchemaDescriptor schema) {
            this.writer = writer;
            this.schema = schema;
        }

        static SchemaWriter plain(Path file) throws IOException {
            return build(file, Integer.MAX_VALUE);
        }

        static SchemaWriter rowGroups(Path file) throws IOException {
            return build(file, 1); // every row forms its own row group
        }

        private static SchemaWriter build(Path file, int rowGroupSize) throws IOException {
            List<LogicalColumnDescriptor> columns = new ArrayList<>();
            columns.add(plain("req_i32", Type.INT32, 0, 0));
            columns.add(plain("req_i64", Type.INT64, 0, 0));
            columns.add(plain("req_f32", Type.FLOAT, 0, 0));
            columns.add(plain("req_f64", Type.DOUBLE, 0, 0));
            columns.add(plain("req_bool", Type.BOOLEAN, 0, 0));
            columns.add(plain("req_bin", Type.BYTE_ARRAY, 0, 0));
            columns.add(plain("req_flba", Type.FIXED_LEN_BYTE_ARRAY, 0, 3));
            columns.add(plain("opt_i32", Type.INT32, 1, 0));
            columns.add(plain("opt_bin", Type.BYTE_ARRAY, 1, 0));
            columns.add(plain("allnull_i32", Type.INT32, 1, 0));
            columns.add(plain("allnull_bin", Type.BYTE_ARRAY, 1, 0));
            SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("batch_equiv", columns);
            ParquetFileWriter writer = new ParquetFileWriter(file, schema,
                    CompressionCodec.UNCOMPRESSED, 2048, rowGroupSize);
            return new SchemaWriter(writer, schema);
        }

        private static LogicalColumnDescriptor plain(String name, Type type, int maxDefinition, int typeLength) {
            ColumnDescriptor descriptor =
                    new ColumnDescriptor(type, new String[]{name}, maxDefinition, 0, typeLength);
            return new LogicalColumnDescriptor(name, LogicalType.PRIMITIVE, type, descriptor);
        }

        void addRow(int i) {
            Object[] values = new Object[]{
                    i,
                    1_000_000_000L + i,
                    i + 0.5f,
                    i + 0.25d,
                    (i & 1) == 0,
                    ("value-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    new byte[]{(byte) i, (byte) (i >> 8), (byte) (i >> 16)},
                    i % 3 == 0 ? null : i,
                    i % 4 == 0 ? null : ("opt-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    null,
                    null
            };
            writer.addRow(new SimpleRowColumnGroup(schema, values));
        }

        void close() throws IOException {
            writer.close();
        }
    }
}
