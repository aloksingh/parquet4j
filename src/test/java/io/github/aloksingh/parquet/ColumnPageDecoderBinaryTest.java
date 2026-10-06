package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.BinaryValues;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnPageDecoder;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.DecodedPage;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ColumnPageDecoderBinaryTest {
    @org.junit.jupiter.api.Test
    void fixedDeltaPrefixesPreserveRawValuesWithoutLengthPrefixes() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.FIXED_LEN_BYTE_ARRAY, 1, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            ByteBuffer prefix = DecodingTestSupport.delta(0, 2, 1);
            ByteBuffer suffix = DecodingTestSupport.delta(3, 1, 2);
            ByteBuffer data = ByteBuffer.allocate(prefix.remaining() + suffix.remaining() + 6);
            data.put(prefix).put(suffix).put(new byte[]{'a', 'b', 'c', 'd', 'e', 'f'}).flip();
            Page source = DecodingTestSupport.page(v2, descriptor, Encoding.DELTA_BYTE_ARRAY,
                    new int[]{1, 0, 1, 1}, new int[4], data);
            BinaryValues values = assertInstanceOf(BinaryValues.class, new ColumnPageDecoder(descriptor).decode(source).values());
            assertArrayEquals(new int[]{0, 3, 6, 9}, values.offsets());
            assertArrayEquals(new byte[]{'a', 'b', 'c'}, values.bytesAt(0));
            assertArrayEquals(new byte[]{'a', 'b', 'd'}, values.bytesAt(1));
            assertArrayEquals(new byte[]{'a', 'e', 'f'}, values.bytesAt(2));
        }
    }

    @org.junit.jupiter.api.Test
    void fixedByteStreamSplitProducesExactRawBytesInBothPageVersions() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.FIXED_LEN_BYTE_ARRAY, 2, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            Page source = DecodingTestSupport.page(v2, descriptor, Encoding.BYTE_STREAM_SPLIT,
                    new int[]{2, 1, 2}, new int[3], ByteBuffer.wrap(new byte[]{0, 66, -1, -1, 66, 0}));
            BinaryValues values = assertInstanceOf(BinaryValues.class, new ColumnPageDecoder(descriptor).decode(source).values());
            assertArrayEquals(new int[]{0, 3, 6}, values.offsets());
            assertArrayEquals(new byte[]{0, -1, 66}, values.bytesAt(0));
            assertArrayEquals(new byte[]{66, -1, 0}, values.bytesAt(1));
        }
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"FIXED_LEN_BYTE_ARRAY", "INT96"})
    void fixedAndInt96PlainAndDictionaryPreserveRawBytes(Type type) {
        byte[] first = type == Type.INT96
                ? new byte[]{0, -1, 66, 1, 2, 3, 4, 5, 6, 7, 8, 9} : new byte[]{0, -1, 66};
        byte[] second = type == Type.INT96
                ? new byte[]{9, 8, 7, 6, 5, 4, 3, 2, 1, 66, -1, 0} : new byte[]{66, -1, 0};
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 2, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            for (Encoding encoding : new Encoding[]{Encoding.PLAIN, Encoding.RLE_DICTIONARY}) {
                ByteBuffer raw = ByteBuffer.allocateDirect(first.length + second.length);
                raw.put(first).put(second).flip();
                ColumnPageDecoder decoder = new ColumnPageDecoder(descriptor);
                java.util.ArrayList<Page> sources = new java.util.ArrayList<>();
                if (encoding == Encoding.RLE_DICTIONARY) {
                    Page.DictionaryPage dictionary = new Page.DictionaryPage(raw.asReadOnlyBuffer(), 2, Encoding.PLAIN);
                    decoder.setDictionary(dictionary);
                    sources.add(dictionary);
                }
                Page source = DecodingTestSupport.page(v2, descriptor, encoding,
                        new int[]{2, 1, 2}, new int[3], encoding == Encoding.PLAIN ? raw.asReadOnlyBuffer()
                                : ByteBuffer.wrap(new byte[]{1, 3, 2}));
                sources.add(source);
                DecodedPage page = decoder.decode(source);
                assertArrayEquals(first, (byte[]) page.physicalValue(0));
                assertArrayEquals(second, (byte[]) page.physicalValue(1));
                if (encoding == Encoding.PLAIN) {
                    BinaryValues values = assertInstanceOf(BinaryValues.class, page.values());
                    assertArrayEquals(new int[]{0, first.length, first.length + second.length}, values.offsets());
                    if (v2) assertTrue(values.data().isDirect());
                }
                ColumnValues column = new ColumnValues(type, sources, descriptor, null);
                List<byte[]> values = type == Type.INT96 ? column.decodeAsInt96() : column.decodeAsFixedByteArray();
                assertArrayEquals(first, values.get(0));
                assertNull(values.get(1));
                assertArrayEquals(second, values.get(2));
                assertArrayEquals(first, column.decodePrimitiveColumn(byte[].class).get(0));
            }
        }
    }
}
