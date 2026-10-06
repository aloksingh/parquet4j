package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.PrimitiveLogicalType;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Read-path coverage for logical annotations (improvement 14): the row API must apply the
 * schema's logical annotations exactly — STRING/ENUM/JSON as UTF-8 text, unannotated
 * BYTE_ARRAY/BSON and FIXED_LEN_BYTE_ARRAY as raw bytes, DECIMAL as exact scaled values,
 * TIMESTAMP/TIME/DATE with their unit and UTC adjustment, INTEGER with its documented
 * carrier — and reject INT96 explicitly while raw bytes stay reachable. Also covers the
 * annotated LIST/MAP/STRUCT schema tree, central leaf-index resolution, the
 * MAP-misclassification guard, and logical/physical descriptor alignment.
 */
public class LogicalAnnotationReadPathTest {

    private static final String DATA = "src/test/data/";

    @TempDir
    Path tempDir;

    // ===== binary: unannotated BYTE_ARRAY is raw bytes, byte-exact =====

    @Test
    void unannotatedBinaryRoundTripsByteExact() throws Exception {
        byte[] payload = {(byte) 0xff, 0x00, (byte) 0x80};
        ColumnDescriptor bin = new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"bin"}, 0, 0, 0);
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("bin_schema",
                List.of(new LogicalColumnDescriptor("bin", LogicalType.PRIMITIVE, Type.BYTE_ARRAY, bin)));
        Path file = tempDir.resolve("binary_ff0080.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{payload}));
        }
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            RowColumnGroup row = reader.rowIterator().next();
            Object value = row.getColumnValue(0);
            assertArrayEquals(payload, (byte[]) value,
                    "the ff0080 case must round-trip byte-exact as raw bytes");
            assertTrue(!(value instanceof String), "raw binary must not be lossy text");
            // Raw physical access agrees exactly.
            assertArrayEquals(payload, reader.getRowGroup(0).readColumn(0)
                    .decodeAsByteArray().get(0));
        }
    }

    // ===== string: only annotated text decodes as UTF-8 =====

    @Test
    void stringAnnotationDecodesAsTextAndKeepsUtf8Bytes() throws Exception {
        String text = "héllo ✓ ünicode";
        ColumnDescriptor col = new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"s"}, 0, 0, 0,
                PrimitiveLogicalType.string());
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("str_schema",
                List.of(new LogicalColumnDescriptor("s", LogicalType.PRIMITIVE, Type.BYTE_ARRAY, col)));
        Path file = tempDir.resolve("string_rt.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{text}));
        }
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            assertEquals(PrimitiveLogicalType.Kind.STRING,
                    reader.getSchema().getColumn(0).annotation().kind(),
                    "the writer must emit the STRING annotation into the footer");
            RowColumnGroup row = reader.rowIterator().next();
            assertEquals(text, row.getColumnValue(0));
            assertArrayEquals(text.getBytes(StandardCharsets.UTF_8),
                    reader.getRowGroup(0).readColumn(0).decodeAsByteArray().get(0));
        }
    }

    // ===== DECIMAL fixtures: exact scaled logical values + exact raw physicals =====

    @Test
    void decimalInt64FixtureDecodesExactScaledValues() throws Exception {
        assertDecimalFixture("int64_decimal.parquet", values -> values.decodeAsInt64().get(0),
                100L, 11);
    }

    @Test
    void decimalInt32FixtureDecodesExactScaledValues() throws Exception {
        assertDecimalFixture("int32_decimal.parquet", values -> values.decodeAsInt32().get(0),
                100, 11);
    }

    @Test
    void decimalByteArrayFixtureDecodesExactScaledValues() throws Exception {
        assertDecimalFixture("byte_array_decimal.parquet",
                values -> new BigInteger(values.decodeAsByteArray().get(0)), new BigInteger("100"), 11);
    }

    @Test
    void decimalFixedLengthFixtureDecodesExactScaledValues() throws Exception {
        assertDecimalFixture("fixed_length_decimal.parquet",
                values -> new BigInteger(values.decodeAsFixedByteArray().get(0)),
                new BigInteger("100"), 11);
    }

    @Test
    void decimalFixedLengthLegacyFixtureDecodesExactScaledValues() throws Exception {
        assertDecimalFixture("fixed_length_decimal_legacy.parquet",
                values -> new BigInteger(values.decodeAsFixedByteArray().get(0)),
                new BigInteger("100"), 6);
    }

    private interface RawFirst {
        Object apply(io.github.aloksingh.parquet.model.ColumnValues values);
    }

    private void assertDecimalFixture(String fixture, RawFirst rawFirst, Object expectedRaw,
                                      int typeLength) throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader(DATA + fixture)) {
            ColumnDescriptor column = reader.getSchema().getColumn(0);
            assertEquals(PrimitiveLogicalType.Kind.DECIMAL, column.annotation().kind());
            if (column.physicalType() == Type.FIXED_LEN_BYTE_ARRAY) {
                assertEquals(typeLength, column.typeLength());
            }
            List<Object> rows = new ArrayList<>();
            RowColumnGroupIterator iterator = reader.rowIterator();
            while (iterator.hasNext()) {
                rows.add(iterator.next().getColumnValue(0));
            }
            assertTrue(rows.size() >= 3, fixture + ": expected at least 3 rows");
            for (int i = 0; i < 3; i++) {
                Object value = rows.get(i);
                assertTrue(value instanceof BigDecimal, fixture + ": DECIMAL must surface as BigDecimal");
                BigDecimal decimal = (BigDecimal) value;
                assertEquals(0, decimal.compareTo(new BigDecimal((i + 1) + ".00")),
                        "Expected exact scaled value " + (i + 1) + ".00 but got " + decimal);
                assertEquals(2, decimal.scale(), fixture + ": scale must come from the annotation");
            }
            // Raw physical of the first row is the unscaled value 100.
            assertEquals(0, new BigDecimal(expectedRaw.toString()).compareTo(new BigDecimal("100")),
                    fixture + ": raw expectation sanity");
            Object raw = rawFirst.apply(reader.getRowGroup(0).readColumn(0));
            if (expectedRaw instanceof BigInteger) {
                assertEquals(0, ((BigInteger) expectedRaw).compareTo((BigInteger) raw),
                        fixture + ": raw physical must be the exact unscaled value");
            } else {
                assertEquals(expectedRaw, raw, fixture + ": raw physical must be the exact unscaled value");
            }
        }
    }

    // ===== timestamps: unit and adjustedToUTC are applied; INT96 is rejected =====

    @Test
    void timestampTimeAndDateAnnotationsApplyUnitAndUtc() throws Exception {
        long micros = 1_700_000_000_123_456L;  // 2023-11-14T22:13:20.123456Z
        ColumnDescriptor ts = new ColumnDescriptor(Type.INT64, new String[]{"ts"}, 0, 0, 0,
                PrimitiveLogicalType.timestamp(PrimitiveLogicalType.TimeUnit.MICROS, true));
        ColumnDescriptor local = new ColumnDescriptor(Type.INT64, new String[]{"local"}, 0, 0, 0,
                PrimitiveLogicalType.timestamp(PrimitiveLogicalType.TimeUnit.MILLIS, false));
        ColumnDescriptor time = new ColumnDescriptor(Type.INT32, new String[]{"t"}, 0, 0, 0,
                PrimitiveLogicalType.time(PrimitiveLogicalType.TimeUnit.MILLIS, true));
        ColumnDescriptor date = new ColumnDescriptor(Type.INT32, new String[]{"d"}, 0, 0, 0,
                PrimitiveLogicalType.date());
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("temporal",
                List.of(describe("ts", ts), describe("local", local), describe("t", time),
                        describe("d", date)));
        Path file = tempDir.resolve("temporal_rt.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{
                    micros, 1_700_000_000_123L, 3_661_123, 19_678}));
        }
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            assertEquals(PrimitiveLogicalType.Kind.TIMESTAMP, reader.getSchema().getColumn(0)
                    .annotation().kind(), "the writer must emit annotations into the footer");
            RowColumnGroup row = reader.rowIterator().next();
            assertEquals(Instant.ofEpochSecond(1_700_000_000L, 123_456_000L), row.getColumnValue(0),
                    "TIMESTAMP(MICROS, adjustedToUTC) must yield the exact Instant");
            assertEquals(LocalDateTime.ofEpochSecond(1_700_000_000L, 123_000_000,
                            java.time.ZoneOffset.UTC), row.getColumnValue(1),
                    "TIMESTAMP(MILLIS, not adjusted) must yield the exact wall-clock LocalDateTime");
            assertEquals(LocalTime.ofSecondOfDay(3661).plusNanos(123_000_000L),
                    row.getColumnValue(2), "TIME(MILLIS) must apply its unit");
            assertEquals(LocalDate.ofEpochDay(19_678), row.getColumnValue(3));
            // Raw physicals remain the exact counts.
            assertEquals(micros, reader.getRowGroup(0).readColumn(0).decodeAsInt64().get(0));
        }
    }

    @Test
    void int96IsRejectedExplicitlyWithRawBytesPreserved() throws Exception {
        try (ParquetFileReader reader = new ParquetFileReader(DATA + "int96_from_spark.parquet")) {
            RowColumnGroup row = reader.rowIterator().next();
            ParquetException failure = assertThrows(ParquetException.class,
                    () -> row.getColumnValue("a"));
            assertTrue(failure.getMessage().contains("INT96"),
                    "rejection must name INT96 but was: " + failure.getMessage());
            // Raw bytes stay available and exact.
            byte[] raw = reader.getRowGroup(0).readColumn(0).decodeAsInt96().get(0);
            assertEquals(12, raw.length);
            assertArrayEquals(new byte[]{0, 42, 30, -39, 99, 67, 0, 0, -105, -118, 37, 0}, raw);
        }
    }

    @Test
    void userdataInt96ColumnRejectedWhileOtherColumnsDecodeExactly() throws Exception {
        try (ParquetFileReader reader = new ParquetFileReader(DATA + "userdata.parquet")) {
            SchemaDescriptor schema = reader.getSchema();
            assertEquals("registration_dttm", schema.getLogicalColumn(0).getName());
            RowColumnGroup row = reader.rowIterator().next();
            assertThrows(ParquetException.class, () -> row.getColumnValue(0));
            // Exact logical values on the same row.
            assertEquals(1, row.getColumnValue("id"));
            assertEquals("Amanda", row.getColumnValue("first_name"));
            assertEquals("Jordan", row.getColumnValue("last_name"));
            assertEquals(49756.53, (Double) row.getColumnValue("salary"), 0.0001);
            // Exact raw INT96 bytes for the rejected column.
            byte[] raw = reader.getRowGroup(0).readColumn(0).decodeAsInt96().get(0);
            assertArrayEquals(new byte[]{0, 42, -23, 108, -14, 25, 0, 0, 78, 127, 37, 0}, raw);
        }
    }

    // ===== LISTs: per-row item lists, empty/absent list is null =====

    @Test
    void listColumnsSurfaceExactPerRowItemLists() throws Exception {
        try (ParquetFileReader reader = new ParquetFileReader(DATA + "list_columns.parquet")) {
            SchemaDescriptor schema = reader.getSchema();
            assertEquals("int64_list.list.item", schema.getLogicalColumn(0).getName());
            assertEquals("utf8_list.list.item", schema.getLogicalColumn(1).getName());
            List<RowColumnGroup> rows = new ArrayList<>();
            RowColumnGroupIterator iterator = reader.rowIterator();
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
            assertEquals(List.of(List.of(1L, 2L, 3L), Arrays.asList((Long) null, 1L), List.of(4L)),
                    rows.stream().map(r -> r.getColumnValue(0)).toList(),
                    "numeric list rows must match exactly (nulls preserved)");
            assertEquals(Arrays.asList(List.of("abc", "efg", "hij"), null,
                            Arrays.asList("efg", null, "hij", "xyz")),
                    rows.stream().map(r -> r.getColumnValue(1)).toList(),
                    "STRING-annotated list rows must match exactly; a null list is null");
            // Raw values agree exactly.
            assertEquals(Arrays.asList(1L, 2L, 3L, null, 1L, 4L),
                    reader.getRowGroup(0).readColumn(0).decodeAsInt64());
        }
    }

    @Test
    void emptyListRowIsNullNotAnItem() throws Exception {
        try (ParquetFileReader reader = new ParquetFileReader(DATA + "null_list.parquet")) {
            assertEquals("emptylist.list.item", reader.getSchema().getLogicalColumn(0).getName());
            RowColumnGroup row = reader.rowIterator().next();
            assertNull(row.getColumnValue(0), "a row with no items has no per-row list value");
        }
    }

    // ===== nested MAPs and primitive-MAP-primitive rows =====

    @Test
    void nestedMapsMaterializeRecursively() throws Exception {
        try (ParquetFileReader reader = new ParquetFileReader(DATA + "nested_maps.snappy.parquet")) {
            SchemaDescriptor schema = reader.getSchema();
            assertEquals(LogicalType.MAP, schema.getLogicalColumn(0).getLogicalType(),
                    "'a' must be classified as a MAP column");
            List<RowColumnGroup> rows = new ArrayList<>();
            RowColumnGroupIterator iterator = reader.rowIterator();
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
            assertTrue(rows.size() >= 4);
            Map<Object, Object> row0 = castMap(rows.get(0).getColumnValue("a"));
            assertEquals(Map.of(1, true, 2, false), row0.get("a"),
                    "map values that are maps must nest as Maps");
            assertEquals(Map.of(1, true), castMap(rows.get(1).getColumnValue("a")).get("b"));
            assertTrue(castMap(rows.get(2).getColumnValue("a")).containsKey("c"));
            assertNull(castMap(rows.get(2).getColumnValue("a")).get("c"),
                    "a null inner map value stays null");
            assertEquals(Map.of(), castMap(rows.get(3).getColumnValue("a")).get("d"),
                    "an empty inner map stays an empty map");
        }
    }

    @Test
    void primitiveMapPrimitiveRowsAlignWithLogicalColumns() throws Exception {
        try (ParquetFileReader reader = new ParquetFileReader(DATA + "nested_maps.snappy.parquet")) {
            SchemaDescriptor schema = reader.getSchema();
            // Logical column order: map first, then the primitives around it.
            assertEquals(List.of("a", "b", "c"),
                    schema.logicalColumns().stream().map(LogicalColumnDescriptor::getName).toList());
            RowColumnGroup row = reader.rowIterator().next();
            assertEquals(3, row.getColumnCount());
            assertTrue(row.getColumnValue(0) instanceof Map);
            assertEquals(1, row.getColumnValue(1));
            assertEquals(1.0, (Double) row.getColumnValue(2), 0.0001);
            // Physical leaves stay reachable and ordered.
            assertEquals(List.of("a.key_value.key", "a.key_value.value.key_value.key",
                            "a.key_value.value.key_value.value", "b", "c"),
                    schema.columns().stream().map(ColumnDescriptor::getPathString).toList());
            assertEquals(row.getColumns().get(1), schema.getLogicalColumn(1),
                    "row columns must be the logical descriptors in value order");
            assertSame(schema.columns(), row.getPhysicalColumns());
        }
    }

    @Test
    void primitiveMapPrimitiveWriterRoundTrip() throws Exception {
        ColumnDescriptor id = new ColumnDescriptor(Type.INT32, new String[]{"id"}, 0, 0, 0);
        LogicalColumnDescriptor attrs = SchemaDescriptor.createStringMapColumn("attrs", true);
        ColumnDescriptor name = new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"name"}, 1, 0, 0,
                PrimitiveLogicalType.string());
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("pm",
                List.of(new LogicalColumnDescriptor("id", LogicalType.PRIMITIVE, Type.INT32, id),
                        attrs, describe("name", name)));
        Path file = tempDir.resolve("primitive_map_primitive.parquet");
        Map<String, String> entries = new LinkedHashMap<>();
        entries.put("k1", "v1");
        entries.put("k2", "v2");
        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{1, entries, "one"}));
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{2, Map.of(), "two"}));
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{3, null, null}));
        }
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            SchemaDescriptor readSchema = reader.getSchema();
            assertEquals(List.of("id", "attrs", "name"),
                    readSchema.logicalColumns().stream().map(LogicalColumnDescriptor::getName).toList());
            assertEquals(List.of("id", "attrs.key_value.key", "attrs.key_value.value", "name"),
                    readSchema.columns().stream().map(ColumnDescriptor::getPathString).toList());
            List<RowColumnGroup> rows = new ArrayList<>();
            RowColumnGroupIterator iterator = reader.rowIterator();
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
            assertEquals(3, rows.size());
            RowColumnGroup first = rows.get(0);
            assertEquals(3, first.getColumnCount());
            assertEquals(1, first.getColumnValue(0));
            assertEquals(entries, first.getColumnValue(1));
            assertEquals("one", first.getColumnValue(2));
            assertEquals(Map.of(), rows.get(1).getColumnValue(1));
            assertNull(rows.get(2).getColumnValue(1), "null map stays null");
            assertNull(rows.get(2).getColumnValue(2));
        }
    }

    // ===== schema tree classification: the MAP-misclassification guard =====

    @Test
    void unannotatedStructureWithMapLikeChildNamesIsNotAMap() {
        List<SchemaElement> elements = new ArrayList<>();
        element(elements, "guards", FieldRepetitionType.REQUIRED, 3, null);
        // key/value child names directly: not a MAP shape at all
        element(elements, "foo", FieldRepetitionType.REQUIRED, 2, null);
        element(elements, "key", FieldRepetitionType.REQUIRED, null,
                org.apache.parquet.format.Type.INT32);
        element(elements, "value", FieldRepetitionType.OPTIONAL, null,
                org.apache.parquet.format.Type.INT32);
        // key_value wrapper with an extra child: not the exact legacy MAP pattern
        element(elements, "bar", FieldRepetitionType.REQUIRED, 1, null);
        element(elements, "key_value", FieldRepetitionType.REPEATED, 3, null);
        element(elements, "key", FieldRepetitionType.REQUIRED, null,
                org.apache.parquet.format.Type.INT32);
        element(elements, "value", FieldRepetitionType.OPTIONAL, null,
                org.apache.parquet.format.Type.INT32);
        element(elements, "extra", FieldRepetitionType.OPTIONAL, null,
                org.apache.parquet.format.Type.INT32);
        // the exact legacy key_value/key/value pattern survives as the last-resort heuristic
        element(elements, "baz", FieldRepetitionType.REQUIRED, 1, null);
        element(elements, "key_value", FieldRepetitionType.REPEATED, 2, null);
        element(elements, "key", FieldRepetitionType.REQUIRED, null,
                org.apache.parquet.format.Type.INT32);
        element(elements, "value", FieldRepetitionType.OPTIONAL, null,
                org.apache.parquet.format.Type.INT32);
        SchemaDescriptor schema = SchemaDescriptor.fromSchemaElements("guards", elements);

        assertEquals(LogicalType.STRUCT, schema.node("foo").kind(),
                "unannotated key/value children must not be read as a MAP");
        assertEquals(LogicalType.STRUCT, schema.node("bar").kind(),
                "a key_value wrapper that is not the exact legacy pattern must not be read as a MAP");
        assertEquals(LogicalType.MAP, schema.node("baz").kind(),
                "the exact legacy key_value/key/value pattern is the documented last resort");
        assertEquals(List.of("foo.key", "foo.value", "bar.key_value.key",
                        "bar.key_value.value", "bar.key_value.extra", "baz"),
                schema.logicalColumns().stream().map(LogicalColumnDescriptor::getName).toList(),
                "structs flatten to leaf columns; only the recognized MAP collapses");
    }

    // ===== central leaf-index resolution: duplicates, nested paths, case =====

    @Test
    void schemaTreeResolvesDuplicateNamesAndNestedPaths() {
        List<SchemaElement> elements = new ArrayList<>();
        element(elements, "dupes", FieldRepetitionType.REQUIRED, 5, null);
        element(elements, "a", FieldRepetitionType.REQUIRED, 1, null);
        element(elements, "x", FieldRepetitionType.OPTIONAL, null,
                org.apache.parquet.format.Type.INT32);
        element(elements, "b", FieldRepetitionType.REQUIRED, 1, null);
        element(elements, "x", FieldRepetitionType.OPTIONAL, null,
                org.apache.parquet.format.Type.INT32);
        element(elements, "x", FieldRepetitionType.OPTIONAL, null,
                org.apache.parquet.format.Type.INT32);
        element(elements, "dup", FieldRepetitionType.REQUIRED, 1, null);
        element(elements, "x", FieldRepetitionType.OPTIONAL, null,
                org.apache.parquet.format.Type.INT32);
        element(elements, "dup", FieldRepetitionType.REQUIRED, 1, null);
        element(elements, "x", FieldRepetitionType.OPTIONAL, null,
                org.apache.parquet.format.Type.INT32);
        SchemaDescriptor schema = SchemaDescriptor.fromSchemaElements("dupes", elements);

        assertEquals(List.of("a.x", "b.x", "x", "dup.x", "dup.x"),
                schema.logicalColumns().stream().map(LogicalColumnDescriptor::getName).toList());

        // Leaf indexes resolve centrally by full path and are distinct for same-named leaves.
        assertEquals(0, schema.leafIndex(new String[]{"a", "x"}));
        assertEquals(1, schema.leafIndex(new String[]{"b", "x"}));
        assertEquals(2, schema.leafIndex(new String[]{"x"}));
        assertEquals(2, schema.leafIndex("x"));
        assertNotEqualsIndex(schema.leafIndex("a.x"), schema.leafIndex("b.x"));

        // Case-sensitive: no collision between x and X.
        assertNull(schema.getLogicalColumn("X"), "name lookup must be case-sensitive");
        assertNotNull(schema.getLogicalColumn("x"));

        // Exact name wins; duplicate logical names resolve to the first occurrence.
        assertEquals("x", schema.getLogicalColumn("x").getName());
        assertSame(schema.getLogicalColumn(3), schema.getLogicalColumn("dup.x"),
                "duplicate names must resolve to the first occurrence");

        // Nested paths address nodes; a single-leaf container resolves to its leaf column.
        assertNotNull(schema.node("a.x"));
        assertSame(schema.getLogicalColumn(0), schema.getLogicalColumn("a"),
                "a container name must resolve to its single leaf's logical column");

        // Prefix leaf-index resolution feeds nested lookups.
        assertEquals(List.of(3, 4), schema.leafIndexesByPathPrefix(new String[]{"dup"}));

        // Physical ownership resolves back to the owning logical column.
        assertSame(schema.getLogicalColumn(1), schema.findLogicalColumnByPhysicalIndex(1));
        assertSame(schema.getLogicalColumn(4), schema.findLogicalColumnByPhysicalIndex(4));
    }

    private static void assertNotEqualsIndex(int left, int right) {
        assertTrue(left != right, "leaf indexes " + left + " and " + right + " must differ");
    }

    @SuppressWarnings("unchecked")
    private static Map<Object, Object> castMap(Object value) {
        assertTrue(value instanceof Map, "expected a Map but got " + value);
        return (Map<Object, Object>) value;
    }

    private static LogicalColumnDescriptor describe(String name, ColumnDescriptor descriptor) {
        return new LogicalColumnDescriptor(name, LogicalType.PRIMITIVE,
                descriptor.physicalType(), descriptor);
    }

    // ===== SchemaElement construction helper (flat, depth-first element list) =====

    private static void element(List<SchemaElement> out, String name, FieldRepetitionType repetition,
                                Integer numChildren, org.apache.parquet.format.Type type) {
        SchemaElement element = new SchemaElement();
        element.setName(name);
        element.setRepetition_type(repetition);
        if (numChildren != null) {
            element.setNum_children(numChildren);
        } else {
            element.setType(type);
        }
        out.add(element);
    }
}
