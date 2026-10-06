package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnPageDecoder;
import io.github.aloksingh.parquet.model.DecodedPage;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ColumnPageDecoderDictionaryTest {
    @org.junit.jupiter.api.Test
    void dictionaryMustBeInstalledBeforeAnyDataPage() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 0, 0);
        ColumnPageDecoder decoder = new ColumnPageDecoder(descriptor);
        decoder.decode(DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
                new int[]{0}, new int[]{0}, DecodingTestSupport.plain(Type.INT32, 17)));
        assertThrows(io.github.aloksingh.parquet.model.ParquetException.class,
                () -> decoder.setDictionary(new Page.DictionaryPage(
                        DecodingTestSupport.plain(Type.INT32, 17), 1, Encoding.PLAIN)));
    }

    @org.junit.jupiter.api.Test
    void binaryMaterializationDoesNotLetCallersCorruptTheCachedDictionary() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 0, 0);
        Page.DictionaryPage dictionary = new Page.DictionaryPage(
                DecodingTestSupport.plain(Type.BYTE_ARRAY, new byte[]{'a', 'b', 'c'}), 1, Encoding.PLAIN);
        Page source = DecodingTestSupport.page(false, descriptor, Encoding.RLE_DICTIONARY,
                new int[]{0}, new int[]{0}, ByteBuffer.wrap(new byte[]{0, 2}));
        io.github.aloksingh.parquet.model.ColumnValues column = new io.github.aloksingh.parquet.model.ColumnValues(
                Type.BYTE_ARRAY, java.util.List.of(dictionary, source), descriptor, null);
        byte[] materialized = column.decodeAsByteArray().get(0);
        materialized[0] = 'z';
        assertArrayEquals(new byte[]{'a', 'b', 'c'}, column.decodeAsByteArray().get(0));
        assertArrayEquals(new byte[]{'a', 'b', 'c'}, (byte[]) column.decodedPages().get(0).dictionary()[0]);
    }

    @org.junit.jupiter.api.Test
    void columnAdaptersCacheDecodedPagesAndTheDictionaryAcrossMaterializations() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 1, 0);
        Page.DictionaryPage dictionary = new Page.DictionaryPage(
                DecodingTestSupport.plain(Type.INT32, 17, 23), 2, Encoding.PLAIN);
        Page first = DecodingTestSupport.page(false, descriptor, Encoding.RLE_DICTIONARY,
                new int[]{1, 0, 1}, new int[3], ByteBuffer.wrap(new byte[]{1, 3, 2}));
        Page second = DecodingTestSupport.page(true, descriptor, Encoding.RLE_DICTIONARY,
                new int[]{1, 1}, new int[2], ByteBuffer.wrap(new byte[]{1, 3, 1}));
        io.github.aloksingh.parquet.model.ColumnValues column = new io.github.aloksingh.parquet.model.ColumnValues(
                Type.INT32, java.util.List.of(dictionary, first, second), descriptor, null);
        java.util.List<DecodedPage> pages = column.decodedPages();
        assertEquals(java.util.Arrays.asList(17, null, 23, 23, 17), column.decodeAsInt32());
        assertEquals(java.util.Arrays.asList(17, null, 23, 23, 17), column.decodeAsInt32());
        assertSame(pages, column.decodedPages());
        assertSame(pages.get(0).dictionary(), pages.get(1).dictionary());
        assertThrows(UnsupportedOperationException.class, () -> pages.clear());
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64", "FLOAT", "DOUBLE", "BOOLEAN", "BYTE_ARRAY"})
    void dictionaryIndexesAddressOnlyNonNullSlotsAndDictionaryIsReused(Type type) {
        Object first = switch (type) {
            case INT32 -> -17;
            case INT64 -> -9_000_000_000L;
            case FLOAT -> 1.25f;
            case DOUBLE -> 1.25;
            case BOOLEAN -> true;
            case BYTE_ARRAY -> new byte[]{'a'};
            default -> throw new AssertionError(type);
        };
        Object second = switch (type) {
            case INT32 -> 23;
            case INT64 -> 9_000_000_000L;
            case FLOAT -> 2.5f;
            case DOUBLE -> 2.5;
            case BOOLEAN -> false;
            case BYTE_ARRAY -> new byte[]{'b', 'c'};
            default -> throw new AssertionError(type);
        };
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 2, 0);
        Page.DictionaryPage dictionary = new Page.DictionaryPage(
                DecodingTestSupport.plain(type, first, second), 2, Encoding.PLAIN);
        ColumnPageDecoder decoder = new ColumnPageDecoder(descriptor);
        decoder.setDictionary(dictionary);
        Object[] previousDictionary = null;
        for (boolean v2 : new boolean[]{false, true}) {
            for (Encoding encoding : new Encoding[]{Encoding.RLE_DICTIONARY, Encoding.PLAIN_DICTIONARY}) {
                DecodedPage page = decoder.decode(DecodingTestSupport.page(v2, descriptor, encoding,
                        new int[]{2, 1, 2, 2}, new int[4], ByteBuffer.wrap(new byte[]{1, 3, 2})));
                assertArrayEquals(new int[]{0, 1, 0}, page.dictionaryIndices());
                assertSame(page.dictionaryIndices(), page.values());
                assertEquals(3, page.nonNullCount());
                assertEquals(2, page.dictionary().length);
                if (type == Type.BYTE_ARRAY) {
                    assertArrayEquals((byte[]) first, (byte[]) page.physicalValue(0));
                    assertArrayEquals((byte[]) second, (byte[]) page.physicalValue(1));
                } else {
                    assertEquals(first, page.physicalValue(0));
                    assertEquals(second, page.physicalValue(1));
                }
                if (previousDictionary != null) assertSame(previousDictionary, page.dictionary());
                previousDictionary = page.dictionary();
                decoder.setDictionary(dictionary); // Re-registering the same page must not rebuild it.
            }
        }
    }
}
