package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DecodingFixtureTest {
    @Test
    void nestedImpalaListMatchesDocumentedNullsAtBothStructAndListDepths() throws IOException {
        // NullableImpalaTest documents these exact PyArrow values, but its legacy
        // assertion incorrectly accepts empty lists for rows whose LIST is absent.
        List<List<Integer>> expected = Arrays.asList(List.of(1), Arrays.asList((Integer) null),
                null, null, null, null, Arrays.asList(2, 3, null));
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/nullable.impala.parquet")) {
            int column = -1;
            for (int i = 0; i < reader.getSchema().getNumColumns(); i++) {
                if (reader.getSchema().getColumn(i).getPathString().equals("nested_struct.b.list.element")) column = i;
            }
            assertTrue(column >= 0);
            assertEquals(expected, reader.getRowGroup(0).readColumn(column).decodeAsList(2, 3, value -> (Integer) value));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"rle_boolean_encoding.parquet", "byte_stream_split.zstd.parquet",
            "delta_binary_packed.parquet", "delta_length_byte_array.parquet", "delta_byte_array.parquet"})
    void physicalAdaptersMatchEveryJsonOracleValue(String filename) throws IOException {
        Path path = Path.of("src/test/data", filename);
        JsonArray rows = JsonParser.parseString(Files.readString(Path.of(path + ".json"))).getAsJsonArray();
        try (ParquetFileReader reader = new ParquetFileReader(path.toString())) {
            assertEquals(rows.size(), reader.getTotalRowCount());
            for (int column = 0; column < reader.getSchema().getNumColumns(); column++) {
                ColumnDescriptor descriptor = reader.getSchema().getColumn(column);
                assertEquals(0, descriptor.maxRepetitionLevel(), "The JSON fixtures in this test are flat");
                List<Object> expected = new ArrayList<>();
                for (JsonElement row : rows) {
                    JsonElement value = row.getAsJsonObject().get(descriptor.getPathString());
                    assertNotNull(value, descriptor.getPathString());
                    expected.add(value.isJsonNull() ? null : switch (descriptor.physicalType()) {
                        case INT32 -> value.getAsInt();
                        case INT64 -> value.getAsLong();
                        case FLOAT -> value.getAsFloat();
                        case DOUBLE -> value.getAsDouble();
                        case BOOLEAN -> value.getAsBoolean();
                        case BYTE_ARRAY -> value.getAsString();
                        default ->
                                throw new AssertionError("No logical conversion oracle for " + descriptor.physicalType());
                    });
                }
                List<Object> actual = new ArrayList<>();
                for (int group = 0; group < reader.getMetadata().getNumRowGroups(); group++) {
                    ColumnValues values = reader.getRowGroup(group).readColumn(column);
                    actual.addAll(switch (descriptor.physicalType()) {
                        case INT32 -> values.decodeAsInt32();
                        case INT64 -> values.decodeAsInt64();
                        case FLOAT -> values.decodeAsFloat();
                        case DOUBLE -> values.decodeAsDouble();
                        case BOOLEAN -> values.decodeAsBoolean();
                        case BYTE_ARRAY -> values.decodeAsString();
                        default -> throw new AssertionError(descriptor.physicalType());
                    });
                }
                assertEquals(expected, actual, filename + ":" + descriptor.getPathString());
            }
        }
    }

    @Test
    void v2LegacyFixtureDecodesAllFiveColumnsExactlyWithoutSwallowingErrors() throws IOException {
        try (ParquetFileReader reader = new ParquetFileReader("src/test/data/datapage_v2.snappy.parquet")) {
            ParquetFileReader.RowGroupReader group = reader.getRowGroup(0);
            assertEquals(Arrays.asList("abc", "abc", "abc", null, "abc"), group.readColumn(0).decodeAsString());
            assertEquals(List.of(1, 2, 3, 4, 5), group.readColumn(1).decodeAsInt32());
            assertEquals(List.of(2.0, 3.0, 4.0, 5.0, 2.0), group.readColumn(2).decodeAsDouble());
            assertEquals(List.of(true, true, true, false, true), group.readColumn(3).decodeAsBoolean());
            assertEquals(Arrays.asList(List.of(1, 2, 3), null, null, List.of(1, 2, 3), List.of(1, 2)),
                    group.readColumn(4).decodeAsList(1, 2, value -> (Integer) value));
        }
    }

    @Test
    void v1LegacyBooleanFixturePreservesEveryPackedBoolean() throws IOException {
        Path path = Path.of("src/test/data/alltypes_plain.parquet");
        JsonArray rows = JsonParser.parseString(Files.readString(Path.of(path + ".json"))).getAsJsonArray();
        try (ParquetFileReader reader = new ParquetFileReader(path.toString())) {
            int booleanColumn = -1;
            for (int i = 0; i < reader.getSchema().getNumColumns(); i++) {
                if (reader.getSchema().getColumn(i).physicalType() == Type.BOOLEAN) booleanColumn = i;
            }
            assertTrue(booleanColumn >= 0);
            ColumnValues column = reader.getRowGroup(0).readColumn(booleanColumn);
            assertTrue(column.getPages().stream().anyMatch(page -> page instanceof Page.DataPage));
            String name = reader.getSchema().getColumn(booleanColumn).getPathString();
            List<Boolean> expected = new ArrayList<>();
            for (JsonElement row : rows) {
                JsonElement value = row.getAsJsonObject().get(name);
                expected.add(value.isJsonNull() ? null : value.getAsBoolean());
            }
            assertEquals(expected, column.decodeAsBoolean());
        }
    }
}
