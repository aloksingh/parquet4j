package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.Type;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class DecodingContainerTest {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2, 3})
    void requiredAndNestedOptionalMapsUseStructuralDefinitionThresholds(int entryDefinition) {
        for (boolean optionalValue : new boolean[]{false, true}) {
            ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.INT32, entryDefinition, 1);
            ColumnDescriptor valueDescriptor = DecodingTestSupport.descriptor(Type.INT32,
                    entryDefinition + (optionalValue ? 1 : 0), 1);
            int[] keyDefinitions = entryDefinition == 1 ? new int[]{0, 1, 1}
                    : new int[]{0, entryDefinition - 1, entryDefinition, entryDefinition};
            int[] repetitions = entryDefinition == 1 ? new int[]{0, 0, 1} : new int[]{0, 0, 0, 1};
            int[] valueDefinitions = keyDefinitions.clone();
            valueDefinitions[valueDefinitions.length - 2] = valueDescriptor.maxDefinitionLevel();
            valueDefinitions[valueDefinitions.length - 1] = optionalValue ? entryDefinition : valueDescriptor.maxDefinitionLevel();
            ColumnValues keys = new ColumnValues(Type.INT32, List.of(DecodingTestSupport.page(false,
                    keyDescriptor, Encoding.PLAIN, keyDefinitions, repetitions, DecodingTestSupport.plain(Type.INT32, 17, 23))),
                    keyDescriptor, null);
            ColumnValues values = new ColumnValues(Type.INT32, List.of(DecodingTestSupport.page(true,
                    valueDescriptor, Encoding.PLAIN, valueDefinitions, repetitions, optionalValue
                            ? DecodingTestSupport.plain(Type.INT32, 101) : DecodingTestSupport.plain(Type.INT32, 101, 202))),
                    valueDescriptor, null);
            java.util.Map<Integer, Integer> entries = new java.util.LinkedHashMap<>();
            entries.put(17, 101);
            entries.put(23, optionalValue ? null : 202);
            List<java.util.Map<Integer, Integer>> expected = entryDefinition == 1
                    ? List.of(java.util.Map.of(), entries) : Arrays.asList(null, java.util.Map.of(), entries);
            assertEquals(expected, ColumnValues.decodeMapFromKeyValueColumns(keys, values,
                    value -> (Integer) value, value -> (Integer) value));
        }
    }

    @Test
    void malformedContainerContinuationsAndLeafAlignmentAreRejected() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 3, 1);
        Page continuation = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{3}, new int[]{1}, DecodingTestSupport.plain(Type.INT32, 17));
        assertThrows(io.github.aloksingh.parquet.model.ParquetException.class,
                () -> new ColumnValues(Type.INT32, List.of(continuation), descriptor, null).decodeAsList(value -> value));
        ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.INT32, 2, 1);
        Page keysPage = DecodingTestSupport.page(false, keyDescriptor, Encoding.PLAIN,
                new int[]{2, 2}, new int[]{0, 1}, DecodingTestSupport.plain(Type.INT32, 17, 23));
        ColumnValues keys = new ColumnValues(Type.INT32, List.of(keysPage), keyDescriptor, null);
        int[][] definitions = {{3}, {3, 3}, {1, 3}};
        int[][] repetitions = {{0}, {0, 0}, {0, 1}};
        for (int i = 0; i < definitions.length; i++) {
            int count = (int) java.util.Arrays.stream(definitions[i]).filter(level -> level == 3).count();
            Object[] payload = count == 1 ? new Object[]{101} : new Object[]{101, 202};
            Page valuesPage = DecodingTestSupport.page(true, descriptor, Encoding.PLAIN,
                    definitions[i], repetitions[i], DecodingTestSupport.plain(Type.INT32, payload));
            ColumnValues values = new ColumnValues(Type.INT32, List.of(valuesPage), descriptor, null);
            assertThrows(io.github.aloksingh.parquet.model.ParquetException.class,
                    () -> ColumnValues.decodeMapFromKeyValueColumns(keys, values, value -> value, value -> value));
        }
    }

    @Test
    void mapKeysAndRequiredValuesCannotBecomeNullDuringMaterialization() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 1, 1);
        Page keyPage = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{1}, new int[]{0}, DecodingTestSupport.plain(Type.INT32, 17));
        Page valuePage = DecodingTestSupport.page(true, descriptor, Encoding.PLAIN,
                new int[]{1}, new int[]{0}, DecodingTestSupport.plain(Type.INT32, 23));
        ColumnValues keys = new ColumnValues(Type.INT32, List.of(keyPage), descriptor, null);
        ColumnValues values = new ColumnValues(Type.INT32, List.of(valuePage), descriptor, null);
        assertThrows(io.github.aloksingh.parquet.model.ParquetException.class,
                () -> ColumnValues.decodeMapFromKeyValueColumns(keys, values, value -> null, value -> value));
        assertThrows(io.github.aloksingh.parquet.model.ParquetException.class,
                () -> ColumnValues.decodeMapFromKeyValueColumns(keys, values, value -> value, value -> null));
    }

    @Test
    void aSinglePhysicalLeafCannotGuessAlternatingMapKeysAndValues() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 2, 1);
        Page page = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{2, 2}, new int[]{0, 1}, DecodingTestSupport.plain(Type.INT32, 10, 20));
        ColumnValues column = new ColumnValues(Type.INT32, List.of(page), descriptor, null);
        assertThrows(io.github.aloksingh.parquet.model.ParquetException.class,
                () -> column.decodeAsMap(value -> value, value -> value));
    }

    @Test
    void mapLeavesAlignLogicalEventsRatherThanPageIndexesOrPageVersions() {
        ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 1);
        ColumnDescriptor valueDescriptor = DecodingTestSupport.descriptor(Type.INT32, 3, 1);
        Page.DictionaryPage dictionary = new Page.DictionaryPage(DecodingTestSupport.plain(Type.BYTE_ARRAY,
                new byte[]{'a'}, new byte[]{'b'}, new byte[]{'c'}, new byte[]{'d'}), 4, Encoding.PLAIN);
        ColumnValues keys = new ColumnValues(Type.BYTE_ARRAY, List.of(dictionary,
                DecodingTestSupport.page(false, keyDescriptor, Encoding.RLE_DICTIONARY,
                        new int[]{2, 2}, new int[]{0, 1}, java.nio.ByteBuffer.wrap(new byte[]{2, 2, 0, 2, 1})),
                DecodingTestSupport.page(true, keyDescriptor, Encoding.RLE_DICTIONARY,
                        new int[]{1, 0}, new int[2], java.nio.ByteBuffer.allocate(0)),
                DecodingTestSupport.page(false, keyDescriptor, Encoding.RLE_DICTIONARY,
                        new int[]{2, 2}, new int[]{0, 1}, java.nio.ByteBuffer.wrap(new byte[]{2, 2, 2, 2, 3}))),
                keyDescriptor, null);
        ColumnValues values = new ColumnValues(Type.INT32, List.of(
                DecodingTestSupport.page(true, valueDescriptor, Encoding.PLAIN,
                        new int[]{3, 2, 1}, new int[]{0, 1, 0}, DecodingTestSupport.plain(Type.INT32, 10)),
                DecodingTestSupport.page(false, valueDescriptor, Encoding.PLAIN,
                        new int[]{0, 3, 3}, new int[]{0, 0, 1}, DecodingTestSupport.plain(Type.INT32, 30, 40))),
                valueDescriptor, null);
        java.util.Map<String, Integer> first = new java.util.LinkedHashMap<>();
        first.put("a", 10);
        first.put("b", null);
        java.util.Map<String, Integer> last = new java.util.LinkedHashMap<>();
        last.put("c", 30);
        last.put("d", 40);
        assertEquals(Arrays.asList(first, java.util.Map.of(), null, last),
                ColumnValues.decodeMapFromKeyValueColumns(keys, values,
                        value -> new String((byte[]) value, java.nio.charset.StandardCharsets.UTF_8),
                        value -> (Integer) value));
    }

    @Test
    void explicitListThresholdsHandleRequiredListsAndOptionalAncestors() {
        for (boolean v2 : new boolean[]{false, true}) {
            ColumnDescriptor required = DecodingTestSupport.descriptor(Type.INT32, 2, 1);
            Page requiredPage = DecodingTestSupport.page(v2, required, Encoding.PLAIN,
                    new int[]{0, 1, 2, 2}, new int[]{0, 0, 1, 0}, DecodingTestSupport.plain(Type.INT32, 17, 23));
            ColumnValues requiredColumn = new ColumnValues(Type.INT32, List.of(requiredPage), required, null);
            assertEquals(Arrays.asList(List.of(), Arrays.asList(null, 17), List.of(23)),
                    requiredColumn.decodeAsList(0, 1, value -> (Integer) value));

            ColumnDescriptor nested = DecodingTestSupport.descriptor(Type.INT32, 4, 1);
            Page nestedPage = DecodingTestSupport.page(v2, nested, Encoding.PLAIN,
                    new int[]{0, 1, 2, 3, 4}, new int[]{0, 0, 0, 0, 1}, DecodingTestSupport.plain(Type.INT32, 17));
            ColumnValues nestedColumn = new ColumnValues(Type.INT32, List.of(nestedPage), nested, null);
            assertEquals(Arrays.asList(null, null, List.of(), Arrays.asList(null, 17)),
                    nestedColumn.decodeAsList(2, 3, value -> (Integer) value));
        }
    }

    @Test
    void v1ListContinuationRetainsActiveContainerAndPhysicalCursorAcrossPages() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 3, 1);
        Page first = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{3, 3}, new int[]{0, 1}, DecodingTestSupport.plain(Type.INT32, 10, 20));
        Page second = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{3, 1, 0, 2, 3}, new int[]{1, 0, 0, 0, 1},
                DecodingTestSupport.plain(Type.INT32, 30, 40));
        ColumnValues column = new ColumnValues(Type.INT32, List.of(first, second), descriptor, null);
        assertEquals(Arrays.asList(List.of(10, 20, 30), List.of(), null, Arrays.asList(null, 40)),
                column.decodeAsList(value -> (Integer) value));
    }
}
