package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class DecodingNullablePageTest {
    @org.junit.jupiter.api.Test
    void columnAdaptersCannotReturnValuesOfADifferentDescriptorType() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT64, 0, 0);
        Page page = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{0}, new int[]{0}, DecodingTestSupport.plain(Type.INT64, 17L));
        assertThrows(io.github.aloksingh.parquet.model.ParquetException.class,
                () -> new ColumnValues(Type.INT32, java.util.List.of(page), descriptor, null).decodeAsInt32());
    }

    @Test
    void nestedBinaryDictionaryAdapterConsumesIndexesOnlyAtMaximumDefinition() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            Page.DictionaryPage dictionary = new Page.DictionaryPage(DecodingTestSupport.plain(Type.BYTE_ARRAY,
                    new byte[]{'a'}, new byte[]{'b'}), 2, Encoding.PLAIN);
            Page page = DecodingTestSupport.page(v2, descriptor, Encoding.RLE_DICTIONARY,
                    new int[]{2, 1, 2}, new int[3], ByteBuffer.wrap(new byte[]{1, 3, 2}));
            ColumnValues column = new ColumnValues(Type.BYTE_ARRAY, List.of(dictionary, page), descriptor, null);
            assertEquals(Arrays.asList("a", null, "b"), column.decodeAsString());
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = Type.class, names = {"INT32", "INT64"})
    void integerAdaptersReadRepetitionLevelsBeforeDefinitions(Type type) {
        Object first = type == Type.INT32 ? (Object) 10 : 10L;
        Object second = type == Type.INT32 ? (Object) 20 : 20L;
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 2, 1);
        for (boolean v2 : new boolean[]{false, true}) {
            Page page = DecodingTestSupport.page(v2, descriptor, Encoding.PLAIN,
                    new int[]{2, 1, 2}, new int[]{0, 1, 0}, DecodingTestSupport.plain(type, first, second));
            ColumnValues column = new ColumnValues(type, List.of(page), descriptor, null);
            assertEquals(Arrays.asList(first, null, second),
                    type == Type.INT32 ? column.decodeAsInt32() : column.decodeAsInt64());
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = Type.class, names = {"FLOAT", "DOUBLE"})
    void floatingAdaptersApplyTheSameNestedNullEventsForPlainDictionaryAndSplit(Type type) {
        Object first = type == Type.FLOAT ? (Object) 1.25f : 1.25;
        Object second = type == Type.FLOAT ? (Object) 2.5f : 2.5;
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 2, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            for (Encoding encoding : new Encoding[]{Encoding.PLAIN, Encoding.RLE_DICTIONARY,
                    Encoding.BYTE_STREAM_SPLIT}) {
                ByteBuffer data;
                List<Page> pages = new java.util.ArrayList<>();
                ByteBuffer plain = DecodingTestSupport.plain(type, first, second);
                if (encoding == Encoding.RLE_DICTIONARY) {
                    pages.add(new Page.DictionaryPage(plain, 2, Encoding.PLAIN));
                    data = ByteBuffer.wrap(new byte[]{1, 3, 2});
                } else if (encoding == Encoding.BYTE_STREAM_SPLIT) {
                    int width = type == Type.FLOAT ? 4 : 8;
                    data = ByteBuffer.allocate(width * 2);
                    for (int lane = 0; lane < width; lane++) {
                        data.put(plain.get(lane)).put(plain.get(width + lane));
                    }
                    data.flip();
                } else {
                    data = plain;
                }
                pages.add(DecodingTestSupport.page(v2, descriptor, encoding,
                        new int[]{2, 1, 2}, new int[3], data));
                ColumnValues column = new ColumnValues(type, pages, descriptor, null);
                List<?> values = type == Type.FLOAT ? column.decodeAsFloat() : column.decodeAsDouble();
                assertEquals(Arrays.asList(first, null, second), values, v2 + ":" + encoding);
            }
        }
    }

    @Test
    void nestedV2BinaryTreatsEveryDefinitionBelowMaximumAsAbsent() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 0);
        Page page = DecodingTestSupport.page(true, descriptor, Encoding.PLAIN,
                new int[]{2, 1, 2}, new int[]{0, 0, 0},
                DecodingTestSupport.plain(Type.BYTE_ARRAY, new byte[]{'a'}, new byte[]{'b'}));
        ColumnValues column = new ColumnValues(Type.BYTE_ARRAY, List.of(page), descriptor, null);

        assertEquals(Arrays.asList("a", null, "b"), column.decodeAsString());
    }

    @Test
    void v2DoubleConsumesPhysicalSlotsOnlyForPresentEvents() {
        ColumnDescriptor descriptor = new ColumnDescriptor(Type.DOUBLE, new String[]{"d"}, 1, 0, 0);
        ByteBuffer data = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        data.putDouble(1.25).putDouble(2.5).flip();
        Page page = new Page.DataPageV2(data, 3, 1, 3, Encoding.PLAIN,
                ByteBuffer.wrap(new byte[]{2, 1, 2, 0, 2, 1}), ByteBuffer.allocate(0), false);
        ColumnValues column = new ColumnValues(Type.DOUBLE, List.of(page), descriptor, null);

        assertEquals(Arrays.asList(1.25, null, 2.5), column.decodeAsDouble());
    }

    @Test
    void v2FloatConsumesPhysicalSlotsOnlyForPresentEvents() {
        ColumnDescriptor descriptor = new ColumnDescriptor(Type.FLOAT, new String[]{"f"}, 1, 0, 0);
        ByteBuffer data = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        data.putFloat(1.25f).putFloat(2.5f).flip();
        // Three singleton RLE runs at bit width one: present, absent, present.
        Page page = new Page.DataPageV2(data, 3, 1, 3, Encoding.PLAIN,
                ByteBuffer.wrap(new byte[]{2, 1, 2, 0, 2, 1}), ByteBuffer.allocate(0), false);
        ColumnValues column = new ColumnValues(Type.FLOAT, List.of(page), descriptor, null);

        assertEquals(Arrays.asList(1.25f, null, 2.5f), column.decodeAsFloat());
        assertEquals(0, data.position(), "Decoding must not consume the supplied page buffer");
    }
}
