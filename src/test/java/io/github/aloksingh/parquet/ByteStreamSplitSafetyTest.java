package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ParquetException;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ByteStreamSplitSafetyTest {
    @Test
    void validatesCountsWidthsAndCompletePayloadsBeforeAllocation() {
        assertThrows(IllegalArgumentException.class, () -> new ByteStreamSplitDecoder(ByteBuffer.allocate(0), -1, 4));
        assertThrows(IllegalArgumentException.class, () -> new ByteStreamSplitDecoder(ByteBuffer.allocate(0), 0, 3));
        assertThrows(ParquetException.class, () -> new ByteStreamSplitDecoder(ByteBuffer.allocate(0), Integer.MAX_VALUE, 8));
        assertThrows(ParquetException.class, () -> new ByteStreamSplitDecoder(ByteBuffer.allocate(3), 1, 4));
        ByteBuffer input = ByteBuffer.allocate(4);
        var decoder = new ByteStreamSplitDecoder(input, 1, 4);
        input.limit(3);
        assertThrows(ParquetException.class, decoder::decodeFloat);
        assertEquals(0, input.position());
    }

    @Test
    void reconstructsFloatRawBitsAcrossEveryBufferKind() {
        int[] bits = {0, Integer.MIN_VALUE, 0x3f800000, 0xbf800000, 0x7f800000, 0xff800000, 0x7fc12345, 0xffc54321, 1};
        long[] widened = new long[bits.length];
        for (int i = 0; i < bits.length; i++) widened[i] = bits[i] & 0xffffffffL;
        byte[] encoded = split(widened, 4);
        for (String kind : new String[]{"heap", "readOnly", "direct", "slice"}) {
            ByteBuffer buffer = CodecBufferTest.buffer(encoded, kind).order(ByteOrder.BIG_ENDIAN);
            int position = buffer.position();
            int limit = buffer.limit();
            float[] actual = new ByteStreamSplitDecoder(buffer, bits.length, 4).decodeFloat();
            assertEquals(bits.length, actual.length);
            for (int i = 0; i < bits.length; i++) assertEquals(bits[i], Float.floatToRawIntBits(actual[i]));
            assertEquals(position + encoded.length, buffer.position());
            assertEquals(limit, buffer.limit());
            assertEquals(ByteOrder.BIG_ENDIAN, buffer.order());
        }
    }

    @Test
    void reconstructsDoubleRawBitsAcrossEveryBufferKind() {
        long[] bits = {0, Long.MIN_VALUE, 0x3ff0000000000000L, 0xbff0000000000000L,
                0x7ff0000000000000L, 0xfff0000000000000L, 0x7ff8123456789abcL, 0xfff8abc123456789L, 1};
        byte[] encoded = split(bits, 8);
        for (String kind : new String[]{"heap", "readOnly", "direct", "slice"}) {
            ByteBuffer buffer = CodecBufferTest.buffer(encoded, kind).order(ByteOrder.BIG_ENDIAN);
            int position = buffer.position();
            double[] actual = new ByteStreamSplitDecoder(buffer, bits.length, 8).decodeDouble();
            assertEquals(bits.length, actual.length);
            for (int i = 0; i < bits.length; i++) assertEquals(bits[i], Double.doubleToRawLongBits(actual[i]));
            assertEquals(position + encoded.length, buffer.position());
            assertEquals(ByteOrder.BIG_ENDIAN, buffer.order());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 8})
    void allocatesOnlyTheOutputArrayRatherThanArraysAndWrappersPerValue(int width) {
        var bean = ManagementFactory.getThreadMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(bean instanceof com.sun.management.ThreadMXBean);
        var allocation = (com.sun.management.ThreadMXBean) bean;
        org.junit.jupiter.api.Assumptions.assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        int count = 50_000;
        byte[] encoded = new byte[count * width];
        ByteBuffer input = ByteBuffer.wrap(encoded);
        for (int i = 0; i < 10; i++) {
            var decoder = new ByteStreamSplitDecoder(input.position(0), count, width);
            if (width == 4) decoder.decodeFloat();
            else decoder.decodeDouble();
        }
        long thread = Thread.currentThread().threadId();
        input.position(0);
        long before = allocation.getThreadAllocatedBytes(thread);
        var decoder = new ByteStreamSplitDecoder(input, count, width);
        int decoded = width == 4 ? decoder.decodeFloat().length : decoder.decodeDouble().length;
        long allocated = allocation.getThreadAllocatedBytes(thread) - before;
        System.out.println("BYTE_STREAM_SPLIT width=" + width + " allocated bytes=" + allocated + ", values=" + count);
        assertEquals(count, decoded);
        assertTrue(allocated < (width + 4L) * count + 4096, "Per-value allocation remains: " + allocated);
    }

    static byte[] split(long[] bits, int width) {
        byte[] encoded = new byte[bits.length * width];
        for (int plane = 0; plane < width; plane++) {
            for (int value = 0; value < bits.length; value++)
                encoded[plane * bits.length + value] = (byte) (bits[value] >>> (8 * plane));
        }
        return encoded;
    }
}
