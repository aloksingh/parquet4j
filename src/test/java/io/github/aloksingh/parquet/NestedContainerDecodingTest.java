package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Regression tests for row-level nested container decoding: null element slots at any
 * depth, continuation events, and V1 page boundaries that split the key and value
 * leaves of a MAP at different points.
 */
class NestedContainerDecodingTest {

    /**
     * Serves synthetic columns to {@link NestedStructureReader}; readMap only calls readColumn.
     */
    private static final class SyntheticRowGroup extends ParquetFileReader.RowGroupReader {
        private final Map<Integer, ColumnValues> columns;

        SyntheticRowGroup(Map<Integer, ColumnValues> columns) {
            super(null, null, null);
            this.columns = columns;
        }

        @Override
        public ColumnValues readColumn(int columnIndex) {
            return columns.get(columnIndex);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(Object value) {
        return value == null ? null : new String((byte[]) value, StandardCharsets.UTF_8);
    }

    @Test
    void mapLeavesSplitAcrossDifferentV1PageBoundariesJoinByRowEvents() throws IOException {
        // optional group m (MAP) { repeated group key_value { required binary key (UTF8);
        //                                                     optional binary value (UTF8) } }
        // Row 0 {"a": "1", "b": null}; row 1 null map; row 2 {"c": "3", "d": "4"}.
        // The key leaf splits mid-row-0 while the value leaf splits mid-row-2.
        ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 1);
        ColumnDescriptor valueDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 3, 1);
        ColumnValues keys = new ColumnValues(Type.BYTE_ARRAY, List.of(
                DecodingTestSupport.page(false, keyDescriptor, Encoding.PLAIN,
                        new int[]{2}, new int[]{0}, DecodingTestSupport.plain(Type.BYTE_ARRAY, bytes("a"))),
                DecodingTestSupport.page(false, keyDescriptor, Encoding.PLAIN,
                        new int[]{2, 0, 2, 2}, new int[]{1, 0, 0, 1},
                        DecodingTestSupport.plain(Type.BYTE_ARRAY, bytes("b"), bytes("c"), bytes("d")))),
                keyDescriptor, null);
        ColumnValues values = new ColumnValues(Type.BYTE_ARRAY, List.of(
                DecodingTestSupport.page(false, valueDescriptor, Encoding.PLAIN,
                        new int[]{3, 2, 0, 3}, new int[]{0, 1, 0, 0},
                        DecodingTestSupport.plain(Type.BYTE_ARRAY, bytes("1"), bytes("3"))),
                DecodingTestSupport.page(false, valueDescriptor, Encoding.PLAIN,
                        new int[]{3}, new int[]{1}, DecodingTestSupport.plain(Type.BYTE_ARRAY, bytes("4")))),
                valueDescriptor, null);

        NestedStructureReader reader = new NestedStructureReader(
                new SyntheticRowGroup(Map.of(0, keys, 1, values)), null);
        Map<String, String> first = new LinkedHashMap<>();
        first.put("a", "1");
        first.put("b", null);
        Map<String, String> last = new LinkedHashMap<>();
        last.put("c", "3");
        last.put("d", "4");
        assertEquals(Arrays.asList(first, null, last),
                reader.readMap(0, 1, NestedContainerDecodingTest::string,
                        NestedContainerDecodingTest::string));
    }

    @Test
    void requiredMapNullValueEntriesJoinAcrossV1PageBoundaries() throws IOException {
        // map_no_value shape: REQUIRED group m (MAP) { repeated group key_value {
        //   required int32 key; optional int32 value } } — every value is a null entry.
        // Row 0 {1: null, 2: null}; row 1 {3: null}, with a page boundary inside row 0
        // of the value leaf but at a row boundary of the key leaf.
        ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.INT32, 1, 1);
        ColumnDescriptor valueDescriptor = DecodingTestSupport.descriptor(Type.INT32, 2, 1);
        ColumnValues keys = new ColumnValues(Type.INT32, List.of(
                DecodingTestSupport.page(false, keyDescriptor, Encoding.PLAIN,
                        new int[]{1, 1}, new int[]{0, 1}, DecodingTestSupport.plain(Type.INT32, 1, 2)),
                DecodingTestSupport.page(false, keyDescriptor, Encoding.PLAIN,
                        new int[]{1}, new int[]{0}, DecodingTestSupport.plain(Type.INT32, 3))),
                keyDescriptor, null);
        ColumnValues values = new ColumnValues(Type.INT32, List.of(
                DecodingTestSupport.page(false, valueDescriptor, Encoding.PLAIN,
                        new int[]{1}, new int[]{0}, DecodingTestSupport.plain(Type.INT32)),
                DecodingTestSupport.page(false, valueDescriptor, Encoding.PLAIN,
                        new int[]{1, 1}, new int[]{1, 0}, DecodingTestSupport.plain(Type.INT32))),
                valueDescriptor, null);

        NestedStructureReader reader = new NestedStructureReader(
                new SyntheticRowGroup(Map.of(0, keys, 1, values)), null);
        Map<Integer, Integer> first = new LinkedHashMap<>();
        first.put(1, null);
        first.put(2, null);
        Map<Integer, Integer> last = new LinkedHashMap<>();
        last.put(3, null);
        assertEquals(Arrays.asList(first, last),
                reader.readMap(0, 1, value -> (Integer) value, value -> (Integer) value));
    }

    @Test
    void nullElementSlotsOnContinuationsAndPageBoundariesAppendNullEntries() {
        // Optional elements under two optional ancestors (max definition 4) with explicit
        // thresholds (2, 3): row 0 spans a V1 page boundary and its second page starts with
        // two null element slots (definitions 3 and 2) that must both append null instead of
        // being dropped or rejected; row 1 is a null container; row 2 holds one null element.
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 4, 1);
        Page first = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{4, 3}, new int[]{0, 1}, DecodingTestSupport.plain(Type.INT32, 10));
        Page second = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{3, 2, 4, 0, 3}, new int[]{1, 1, 1, 0, 0},
                DecodingTestSupport.plain(Type.INT32, 30));
        ColumnValues column = new ColumnValues(Type.INT32, List.of(first, second), descriptor, null);
        assertEquals(Arrays.asList(Arrays.asList(10, null, null, null, 30),
                        null, Arrays.asList((Integer) null)),
                column.decodeAsList(2, 3, value -> (Integer) value));
    }
}
