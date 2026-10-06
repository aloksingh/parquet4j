package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.MapMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class WriterIndependentTest {
    @TempDir
    Path directory;

    @Test
    void fixedLengthMapLeavesSerializeTheirRequiredWidths() throws Exception {
        var key = new ColumnDescriptor(Type.FIXED_LEN_BYTE_ARRAY,
                new String[]{"map", "key_value", "key"}, 2, 1, 3);
        var value = new ColumnDescriptor(Type.FIXED_LEN_BYTE_ARRAY,
                new String[]{"map", "key_value", "value"}, 3, 1, 4);
        var metadata = new MapMetadata(0, 1, Type.FIXED_LEN_BYTE_ARRAY, Type.FIXED_LEN_BYTE_ARRAY,
                key, value);
        var logical = new LogicalColumnDescriptor("map", LogicalType.MAP, metadata);
        var schema = SchemaDescriptor.fromLogicalColumns("fixed-map", List.of(logical));
        Path destination = directory.resolve("fixed-map.parquet");
        try (var writer = new ParquetFileWriter(destination, schema)) {
            writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{
                    Map.of(new byte[]{'k', 'e', 'y'}, new byte[]{'d', 'a', 't', 'a'})}));
        }
        var footer = WriterTestSupport.footer(destination);
        assertEquals(3, footer.getSchema().get(3).getType_length());
        assertEquals(4, footer.getSchema().get(4).getType_length());
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.prepareStatement(
                     "SELECT hex(e.key), hex(e.value) FROM read_parquet(?), UNNEST(map_entries(\"map\")) AS t(e)")) {
            statement.setString(1, destination.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals("6B6579", rows.getString(1));
                assertEquals("64617461", rows.getString(2));
                assertFalse(rows.next());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"UNCOMPRESSED", "ZSTD", "GZIP", "SNAPPY", "LZ4"})
    void independentEngineReadsNullablePhysicalValuesAcrossPagesAndGroups(CompressionCodec codec) throws Exception {
        List<ColumnDescriptor> columns = List.of(
                new ColumnDescriptor(Type.BOOLEAN, new String[]{"flag"}, 1, 0, 0),
                new ColumnDescriptor(Type.INT32, new String[]{"number"}, 1, 0, 0),
                new ColumnDescriptor(Type.INT64, new String[]{"id"}, 0, 0, 0),
                new ColumnDescriptor(Type.FLOAT, new String[]{"floating"}, 1, 0, 0),
                new ColumnDescriptor(Type.DOUBLE, new String[]{"precise"}, 1, 0, 0),
                new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"raw"}, 1, 0, 0),
                new ColumnDescriptor(Type.FIXED_LEN_BYTE_ARRAY, new String[]{"fixed"}, 0, 0, 3));
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("physical",
                SchemaDescriptor.createLogicalColumnsFromPhysical(columns));
        Path destination = directory.resolve(codec.name() + "-physical.parquet");
        int count = 35;
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema, codec, 16, 400)) {
            for (int i = 0; i < count; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, physicalRow(i)));
            }
        }
        var footer = WriterTestSupport.footer(destination);
        assertTrue(footer.getRow_groupsSize() > 1);
        assertTrue(WriterTestSupport.pages(destination, 0, 2).size() > 1);
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var statement = connection.prepareStatement("SELECT * FROM read_parquet(?) ORDER BY id")) {
            statement.setString(1, destination.toString());
            try (var rows = statement.executeQuery()) {
                for (int i = 0; i < count; i++) {
                    Object[] expected = physicalRow(i);
                    assertTrue(rows.next(), "Missing row " + i);
                    assertEquals(expected[0], rows.getObject(1));
                    assertEquals(expected[1], rows.getObject(2));
                    assertEquals(expected[2], rows.getLong(3));
                    if (expected[3] == null) assertNull(rows.getObject(4));
                    else assertEquals((Float) expected[3], rows.getFloat(4));
                    if (expected[4] == null) assertNull(rows.getObject(5));
                    else assertEquals((Double) expected[4], rows.getDouble(5));
                    assertArrayEquals((byte[]) expected[5], rows.getBytes(6));
                    assertArrayEquals((byte[]) expected[6], rows.getBytes(7));
                }
                assertFalse(rows.next());
            }
        }
    }

    private static Object[] physicalRow(int i) {
        return new Object[]{i % 4 == 0 ? null : i % 2 == 0, i % 5 == 0 ? null : i - 17,
                (1L << 40) + i, i % 3 == 0 ? null : i + 0.25f, i % 7 == 0 ? null : -i - 0.125d,
                i % 6 == 0 ? null : new byte[]{(byte) i, (byte) (255 - i)},
                new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2)}};
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"UNCOMPRESSED", "ZSTD"})
    void oversizedMapRowsStayWholeAndEveryV2PageStartsANewLogicalRow(CompressionCodec codec) throws Exception {
        var before = new ColumnDescriptor(Type.INT32, new String[]{"id"}, 0, 0, 0);
        var after = new ColumnDescriptor(Type.INT64, new String[]{"after"}, 0, 0, 0);
        var logical = new ArrayList<>(SchemaDescriptor.createLogicalColumnsFromPhysical(List.of(before)));
        logical.add(SchemaDescriptor.createMapColumn("map", Type.INT32, Type.INT64, true, true));
        logical.addAll(SchemaDescriptor.createLogicalColumnsFromPhysical(List.of(after)));
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("oversized-map", logical);
        Map<Integer, Long> big = new LinkedHashMap<>();
        for (int i = 0; i < 40; i++) big.put(i, i % 7 == 0 ? null : i * 1001L);
        Map<Integer, Long> mixed = new LinkedHashMap<>();
        mixed.put(2, 20L);
        mixed.put(3, null);
        mixed.put(4, 40L);
        List<Map<Integer, Long>> maps = new ArrayList<>();
        maps.add(big);
        maps.add(Map.of());
        maps.add(null);
        maps.add(Map.of(2, 9L));
        maps.add(mixed);
        int pageTarget = 16;
        Path destination = directory.resolve(codec.name() + "-oversized-map.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(destination, schema, codec, pageTarget, 256)) {
            for (int i = 0; i < maps.size(); i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i, maps.get(i), 1000L + i}));
            }
        }
        var footer = WriterTestSupport.footer(destination);
        assertTrue(footer.getRow_groupsSize() > 1);
        boolean oversized = false;
        long keyNulls = 0;
        long valueNulls = 0;
        for (int groupIndex = 0; groupIndex < footer.getRow_groupsSize(); groupIndex++) {
            var group = footer.getRow_groups().get(groupIndex);
            keyNulls += group.getColumns().get(1).getMeta_data().getStatistics().getNull_count();
            valueNulls += group.getColumns().get(2).getMeta_data().getStatistics().getNull_count();
            for (int column : new int[]{1, 2}) {
                long observedRows = 0;
                for (var page : WriterTestSupport.pages(destination, groupIndex, column)) {
                    assertEquals(org.apache.parquet.format.PageType.DATA_PAGE_V2, page.header().getType());
                    var data = page.header().getData_page_header_v2();
                    int repLength = data.getRepetition_levels_byte_length();
                    int[] repetitions = WriterTestSupport.decodeLevels(Arrays.copyOfRange(page.payload(), 0, repLength),
                            1, data.getNum_values());
                    assertEquals(0, repetitions[0], "V2 pages cannot start inside a MAP row");
                    long rows = Arrays.stream(repetitions).filter(level -> level == 0).count();
                    assertEquals(rows, data.getNum_rows());
                    observedRows += rows;
                    int maxDefinition = column == 1 ? 2 : 3;
                    int[] definitions = WriterTestSupport.decodeLevels(Arrays.copyOfRange(page.payload(), repLength,
                            repLength + data.getDefinition_levels_byte_length()), 2, data.getNum_values());
                    long nulls = Arrays.stream(definitions).filter(level -> level < maxDefinition).count();
                    assertEquals(nulls, data.getNum_nulls());
                    assertEquals(nulls, data.getStatistics().getNull_count());
                    if (page.header().getUncompressed_page_size() > pageTarget) {
                        oversized = true;
                        assertEquals(1, data.getNum_rows(), "Only a single complete row may exceed a soft target");
                    }
                }
                assertEquals(group.getNum_rows(), observedRows);
            }
        }
        assertTrue(oversized, "Oracle must actually exercise an oversized MAP row");
        long placeholders = maps.stream().filter(map -> map == null || map.isEmpty()).count();
        long nullValues = maps.stream().filter(Objects::nonNull).flatMap(map -> map.values().stream())
                .filter(Objects::isNull).count();
        assertEquals(placeholders, keyNulls);
        assertEquals(placeholders + nullValues, valueNulls);
        try (var connection = DriverManager.getConnection("jdbc:duckdb:");
             var rowsStatement = connection.prepareStatement(
                     "SELECT id, \"map\" IS NULL, cardinality(\"map\"), \"after\" FROM read_parquet(?) ORDER BY id");
             var entriesStatement = connection.prepareStatement(
                     "SELECT id, e.key, e.value FROM read_parquet(?), UNNEST(map_entries(\"map\")) AS t(e) ORDER BY id, e.key")) {
            rowsStatement.setString(1, destination.toString());
            try (var rows = rowsStatement.executeQuery()) {
                for (int i = 0; i < maps.size(); i++) {
                    assertTrue(rows.next());
                    assertEquals(i, rows.getInt(1));
                    assertEquals(maps.get(i) == null, rows.getBoolean(2));
                    if (maps.get(i) == null) assertNull(rows.getObject(3));
                    else assertEquals(maps.get(i).size(), rows.getInt(3));
                    assertEquals(1000L + i, rows.getLong(4));
                }
                assertFalse(rows.next());
            }
            entriesStatement.setString(1, destination.toString());
            try (var entries = entriesStatement.executeQuery()) {
                for (int row = 0; row < maps.size(); row++) {
                    Map<Integer, Long> map = maps.get(row);
                    if (map == null) continue;
                    for (var entry : map.entrySet()) {
                        assertTrue(entries.next(), "Missing MAP entry " + row + ":" + entry.getKey());
                        assertEquals(row, entries.getInt(1));
                        assertEquals(entry.getKey().intValue(), entries.getInt(2));
                        if (entry.getValue() == null) assertNull(entries.getObject(3));
                        else assertEquals(entry.getValue().longValue(), entries.getLong(3));
                    }
                }
                assertFalse(entries.next());
            }
        }
    }
}
