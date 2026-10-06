package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.aloksingh.parquet.model.ColumnBatch;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.PrimitiveLogicalType;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Shared comparison helpers for the batch/list/row equivalence tests.
 */
final class BatchTestSupport {
    private BatchTestSupport() {
    }

    /**
     * Flat boxed list exactly as the ColumnValues list adapters return it (one entry per level event).
     */
    static List<Object> flatList(ColumnValues values, Type type) {
        return switch (type) {
            case INT32 -> new ArrayList<>(values.decodeAsInt32());
            case INT64 -> new ArrayList<>(values.decodeAsInt64());
            case FLOAT -> new ArrayList<>(values.decodeAsFloat());
            case DOUBLE -> new ArrayList<>(values.decodeAsDouble());
            case BOOLEAN -> new ArrayList<>(values.decodeAsBoolean());
            default -> new ArrayList<>(values.decodeAsRawBytes());
        };
    }

    /**
     * One boxed entry per batch slot; binary values are copied out as raw bytes.
     */
    static List<Object> batchValues(ColumnBatch batch) {
        List<Object> values = new ArrayList<>(batch.size());
        for (int i = 0; i < batch.size(); i++) {
            values.add(batchValue(batch, i));
        }
        return values;
    }

    static Object batchValue(ColumnBatch batch, int row) {
        if (batch.isNull(row)) {
            return null;
        }
        if (isBinary(batch.physicalType())) {
            return rawBytes(batch.getBytes(row));
        }
        return batch.getObject(row);
    }

    /**
     * Copies a read-only int buffer (for example dictionary indexes) into an int array.
     */
    static int[] toArray(java.nio.IntBuffer buffer) {
        int[] values = new int[buffer.remaining()];
        buffer.duplicate().get(values);
        return values;
    }

    static byte[] rawBytes(ByteBuffer view) {
        byte[] bytes = new byte[view.remaining()];
        view.duplicate().get(bytes);
        return bytes;
    }

    static boolean isBinary(Type type) {
        return type == Type.BYTE_ARRAY || type == Type.FIXED_LEN_BYTE_ARRAY || type == Type.INT96;
    }

    /**
     * Strict equality: count, order, null placement, and exact values (binary as raw bytes).
     */
    static void assertSameValues(String label, List<?> expected, List<?> actual) {
        assertEquals(expected.size(), actual.size(), label + ": value count");
        for (int i = 0; i < expected.size(); i++) {
            Object left = expected.get(i);
            Object right = actual.get(i);
            if (left == null || right == null) {
                assertTrue(left == null && right == null, label + ": null placement at " + i
                        + " (expected " + left + ", found " + right + ")");
                continue;
            }
            if (left instanceof byte[] leftBytes) {
                assertArrayEquals(leftBytes, (byte[]) right, label + ": raw bytes at " + i);
            } else {
                assertEquals(left, right, label + ": value at " + i);
            }
        }
    }

    /**
     * Collects one value per row through the row API (rowIterator) for the logical
     * column that owns the given physical column.
     */
    static List<Object> rowValues(ParquetFileReader reader, int physicalIndex) throws IOException {
        SchemaDescriptor schema = reader.getSchema();
        LogicalColumnDescriptor logical = schema.findLogicalColumnByPhysicalIndex(physicalIndex);
        List<Object> values = new ArrayList<>();
        RowColumnGroupIterator iterator = reader.rowIterator(false);
        while (iterator.hasNext()) {
            values.add(iterator.next().getColumnValue(logical.getName()));
        }
        return values;
    }

    /**
     * Row-API leg for non-repeated columns: the row value must equal the flat raw value
     * converted through the column's logical annotation (STRING/ENUM/JSON decode as UTF-8
     * text; DECIMAL/TIMESTAMP/TIME/DATE/INTEGER/UUID convert to their carrier types;
     * unannotated BYTE_ARRAY/BSON and FIXED_LEN_BYTE_ARRAY keep raw bytes). INT96 has no
     * row representation and must be rejected before this helper is called.
     */
    static void assertRowValuesMatchList(String label, ColumnDescriptor descriptor,
                                         List<?> flat, List<?> rows) {
        assertEquals(flat.size(), rows.size(), label + ": row count");
        PrimitiveLogicalType annotation = descriptor.annotation();
        for (int i = 0; i < flat.size(); i++) {
            Object expected = flat.get(i);
            Object actual = rows.get(i);
            if (expected == null) {
                assertNull(actual, label + ": row null placement at " + i);
                continue;
            }
            if (descriptor.physicalType() == Type.INT96) {
                fail(label + ": INT96 must be rejected by the row API, not compared at " + i);
            }
            Object expectedLogical = annotation.kind() == PrimitiveLogicalType.Kind.NONE
                    ? expected : annotation.toLogicalValue(expected);
            if (expectedLogical instanceof byte[] bytes) {
                assertArrayEquals(bytes, (byte[]) actual, label + ": row bytes at " + i);
            } else {
                assertEquals(expectedLogical, actual, label + ": row value at " + i);
            }
        }
    }

    /**
     * The row API must reject a column's values explicitly instead of returning nulls.
     */
    static void assertRowApiRejectsColumn(ParquetFileReader reader, int physicalIndex) {
        LogicalColumnDescriptor logical =
                reader.getSchema().findLogicalColumnByPhysicalIndex(physicalIndex);
        assertNotNull(logical, "no logical column owns physical index " + physicalIndex);
        try (ParquetRowIterator iterator = new ParquetRowIterator(reader, false)) {
            assertTrue(iterator.hasNext());
            RowColumnGroup row = iterator.next();
            assertThrows(ParquetException.class, () -> row.getColumnValue(logical.getName()),
                    "expected explicit rejection for column " + logical.getName());
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Flattens row-API container values (one List per row) into their element sequence.
     */
    static List<Object> flattenContainers(List<?> rowValues) {
        List<Object> elements = new ArrayList<>();
        for (Object value : rowValues) {
            if (value instanceof List<?> container) {
                elements.addAll(container);
            }
        }
        return elements;
    }

    /**
     * Builds a per-row container view from the flat event list with the documented thresholds.
     */
    static List<List<Object>> listContainers(ColumnValues values, int listDefinition, int elementDefinition) {
        return values.decodeAsList(listDefinition, elementDefinition, value -> (Object) value);
    }

    /**
     * Every row value is null or a List: the row API surfaces nested columns as containers.
     */
    static boolean rowSurfaceIsContainerBased(List<?> rowValues) {
        return rowValues.stream().allMatch(value -> value == null || value instanceof List);
    }

    /**
     * Concatenates the per-row-group flat lists of one physical column across the file.
     */
    static List<Object> flatListAcrossRowGroups(ParquetFileReader reader, int physicalIndex) throws IOException {
        Type type = reader.getSchema().getColumn(physicalIndex).physicalType();
        List<Object> values = new ArrayList<>();
        for (int group = 0; group < reader.getNumRowGroups(); group++) {
            values.addAll(flatList(reader.getRowGroup(group).readColumn(physicalIndex), type));
        }
        return values;
    }

    /**
     * Concatenates the per-row-group batches of one physical column across the file.
     */
    static List<Object> batchValuesAcrossRowGroups(ParquetFileReader reader, int physicalIndex) throws IOException {
        List<Object> values = new ArrayList<>();
        for (int group = 0; group < reader.getNumRowGroups(); group++) {
            values.addAll(batchValues(reader.getRowGroup(group).readColumnBatch(physicalIndex)));
        }
        return values;
    }

    static ColumnDescriptor descriptor(Type type, int definitions, int repetitions) {
        return new ColumnDescriptor(type, new String[]{"col"}, definitions, repetitions,
                type == Type.FIXED_LEN_BYTE_ARRAY ? 3 : 0);
    }
}
