package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnPageDecoder;
import io.github.aloksingh.parquet.model.DecodedPage;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ColumnPageDecoderByteStreamSplitTest {
    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64", "FLOAT", "DOUBLE"})
    void byteStreamSplitMatchesPlainPrimitiveArraysIncludingNullEvents(Type type) {
        Object first = switch (type) {
            case INT32 -> Integer.MIN_VALUE;
            case INT64 -> Long.MIN_VALUE;
            case FLOAT -> Float.intBitsToFloat(0x7fc12345);
            case DOUBLE -> -0.0;
            default -> throw new AssertionError(type);
        };
        Object second = switch (type) {
            case INT32 -> 123456;
            case INT64 -> 9_000_000_000L;
            case FLOAT -> -0.0f;
            case DOUBLE -> Double.longBitsToDouble(0x7ff8123456789012L);
            default -> throw new AssertionError(type);
        };
        int width = type == Type.INT32 || type == Type.FLOAT ? 4 : 8;
        for (boolean v2 : new boolean[]{false, true}) {
            for (int maximum : new int[]{0, 2}) {
                ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, maximum, 0);
                int[] definitions = maximum == 0 ? new int[2] : new int[]{2, 1, 2};
                ByteBuffer plain = DecodingTestSupport.plain(type, first, second);
                ByteBuffer split = ByteBuffer.allocate(plain.remaining());
                for (int lane = 0; lane < width; lane++) {
                    for (int index = 0; index < 2; index++) split.put(plain.get(index * width + lane));
                }
                split.flip();
                DecodedPage page = new ColumnPageDecoder(descriptor).decode(DecodingTestSupport.page(
                        v2, descriptor, Encoding.BYTE_STREAM_SPLIT, definitions, new int[definitions.length], split));
                assertEquals(2, page.nonNullCount());
                assertEquals(first, page.physicalValue(0));
                assertEquals(second, page.physicalValue(1));
                switch (type) {
                    case INT32 ->
                            assertArrayEquals(new int[]{(Integer) first, (Integer) second}, (int[]) page.values());
                    case INT64 -> assertArrayEquals(new long[]{(Long) first, (Long) second}, (long[]) page.values());
                    case FLOAT -> {
                        float[] values = assertInstanceOf(float[].class, page.values());
                        assertEquals(0x7fc12345, Float.floatToRawIntBits(values[0]));
                        assertEquals(0x80000000, Float.floatToRawIntBits(values[1]));
                    }
                    case DOUBLE -> {
                        double[] values = assertInstanceOf(double[].class, page.values());
                        assertEquals(0x8000000000000000L, Double.doubleToRawLongBits(values[0]));
                        assertEquals(0x7ff8123456789012L, Double.doubleToRawLongBits(values[1]));
                    }
                    default -> throw new AssertionError(type);
                }
            }
        }
    }
}
