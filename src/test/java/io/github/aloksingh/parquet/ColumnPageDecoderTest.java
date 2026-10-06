package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnPageDecoder;
import io.github.aloksingh.parquet.model.DecodedPage;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

class ColumnPageDecoderTest {
    @Test
    void plainBinaryPageUsesOneReadOnlyPayloadWithOffsets() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            DecodedPage page = new ColumnPageDecoder(descriptor).decode(DecodingTestSupport.page(
                    v2, descriptor, Encoding.PLAIN, new int[]{2, 1, 2, 2}, new int[]{0, 0, 0, 0},
                    DecodingTestSupport.plain(Type.BYTE_ARRAY, new byte[]{'a'}, new byte[0],
                            new byte[]{'b', 'c', 'd'})));
            io.github.aloksingh.parquet.model.BinaryValues values = assertInstanceOf(
                    io.github.aloksingh.parquet.model.BinaryValues.class, page.values());
            assertEquals(3, values.size());
            assertArrayEquals(new int[]{0, 1, 1, 4}, values.offsets());
            assertArrayEquals(new byte[]{'a', 'b', 'c', 'd'}, bytes(values.data()));
            assertTrue(values.data().isReadOnly());
            assertTrue(values.byteBuffer(2).isReadOnly());
            assertArrayEquals(new byte[]{'b', 'c', 'd'}, values.bytesAt(2));
            assertArrayEquals(new byte[]{'b', 'c', 'd'}, bytes(values.byteBuffer(2)));
            assertArrayEquals(new byte[0], values.bytesAt(1));
            assertArrayEquals(new byte[]{'a'}, (byte[]) page.physicalValue(0));
            byte[] copy = values.bytesAt(2);
            copy[0] = 0;
            assertArrayEquals(new byte[]{'b', 'c', 'd'}, values.bytesAt(2));
            int[] offsets = values.offsets();
            offsets[1] = 100;
            assertArrayEquals(new int[]{0, 1, 1, 4}, values.offsets());
            assertThrows(IndexOutOfBoundsException.class, () -> values.bytesAt(3));
        }
    }

    private static byte[] bytes(ByteBuffer data) {
        byte[] bytes = new byte[data.remaining()];
        data.get(bytes);
        return bytes;
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(value = Type.class,
            names = {"INT32", "INT64", "FLOAT", "DOUBLE"})
    void v1AndV2DecodeNestedNumericEventsWithTheSamePrimitiveType(Type type) {
        Object first = switch (type) {
            case INT32 -> -17;
            case INT64 -> -9_000_000_000L;
            case FLOAT -> 1.25f;
            case DOUBLE -> 1.25;
            default -> throw new AssertionError(type);
        };
        Object second = switch (type) {
            case INT32 -> 23;
            case INT64 -> 9_000_000_000L;
            case FLOAT -> 2.5f;
            case DOUBLE -> 2.5;
            default -> throw new AssertionError(type);
        };
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 2, 1);
        for (boolean v2 : new boolean[]{false, true}) {
            DecodedPage page = new ColumnPageDecoder(descriptor).decode(DecodingTestSupport.page(
                    v2, descriptor, Encoding.PLAIN, new int[]{2, 1, 2}, new int[]{0, 1, 0},
                    DecodingTestSupport.plain(type, first, second)));
            assertEquals(3, page.numValues());
            assertEquals(2, page.nonNullCount());
            assertEquals(first, page.physicalValue(0));
            assertEquals(second, page.physicalValue(1));
            assertEquals(1, page.definitionLevel(1));
            assertEquals(1, page.repetitionLevel(1));
            Class<?> arrayType = switch (type) {
                case INT32 -> int[].class;
                case INT64 -> long[].class;
                case FLOAT -> float[].class;
                case DOUBLE -> double[].class;
                default -> throw new AssertionError(type);
            };
            assertInstanceOf(arrayType, page.values());
        }
    }

    @Test
    void optionalFloatPageKeepsOnlyPresentValuesInPrimitiveStorage() {
        ColumnDescriptor descriptor = new ColumnDescriptor(Type.FLOAT, new String[]{"f"}, 1, 0, 0);
        ByteBuffer data = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        data.putFloat(1.25f).putFloat(2.5f).flip();
        ByteBuffer definitions = ByteBuffer.wrap(new byte[]{2, 1, 2, 0, 2, 1});
        DecodedPage page = new ColumnPageDecoder(descriptor).decode(new Page.DataPageV2(
                data, 3, 1, 3, Encoding.PLAIN, definitions, ByteBuffer.allocate(0), false));

        assertEquals(3, page.numValues());
        assertEquals(2, page.nonNullCount());
        assertArrayEquals(new float[]{1.25f, 2.5f}, assertInstanceOf(float[].class, page.values()));
        assertEquals(1, page.definitionLevel(0));
        assertEquals(0, page.definitionLevel(1));
        assertEquals(1, page.definitionLevel(2));
        assertEquals(0, page.repetitionLevel(2));
        assertEquals(2.5f, page.physicalValue(1));
        assertNull(page.dictionaryIndices());
        assertNull(page.dictionary());
        assertEquals(0, data.position());
        assertEquals(0, definitions.position());
        assertThrows(IndexOutOfBoundsException.class, () -> page.physicalValue(2));
        assertThrows(IndexOutOfBoundsException.class, () -> page.definitionLevel(3));
    }
}
