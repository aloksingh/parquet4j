package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.junit.jupiter.api.Test;

class RleEncoderSafetyTest {
    @Test
    void writesWidth32BitPackedValuesAsIndependentLittleEndianBytesWithTheV1Prefix() throws Exception {
        int[] values = {-1, Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 1, 0x12345678, -2, -1234567};
        ByteBuffer expected = ByteBuffer.allocate(4 + 1 + 4 * values.length).order(ByteOrder.LITTLE_ENDIAN);
        expected.putInt(1 + 4 * values.length).put((byte) 3);
        for (int value : values) expected.putInt(value);
        assertArrayEquals(expected.array(), new RleEncoder(32).encode(values));
    }

    @Test
    void exposesRawV2RunsWithoutChangingEitherV1Overload() throws Exception {
        var method = assertDoesNotThrow(() -> RleEncoder.class.getMethod("encodeRaw", int[].class));
        RleEncoder encoder = new RleEncoder(2);
        int[] values = {0, 1, 2, 3, 0, 1, 2, 3};
        byte[] raw = (byte[]) method.invoke(encoder, (Object) values);
        assertArrayEquals(new byte[]{3, (byte) 0xe4, (byte) 0xe4}, raw);
        ByteBuffer prefixed = ByteBuffer.allocate(4 + raw.length).order(ByteOrder.LITTLE_ENDIAN).putInt(raw.length).put(raw);
        assertArrayEquals(prefixed.array(), encoder.encode(values));
        assertArrayEquals(prefixed.array(), encoder.encode(List.of(0, 1, 2, 3, 0, 1, 2, 3)));
        assertArrayEquals(new byte[0], (byte[]) method.invoke(encoder, (Object) new int[0]));
        assertArrayEquals(new byte[4], encoder.encode(new int[0]));
        assertArrayEquals(prefixed.array(), encoder.encode(values), "Encoder reuse must not retain older runs");
    }

    @Test
    void rejectsValuesThatCannotBeRepresentedByTheConfiguredWidth() {
        assertThrows(IllegalArgumentException.class, () -> new RleEncoder(0).encode(new int[]{1}));
        assertThrows(IllegalArgumentException.class, () -> new RleEncoder(1).encode(new int[]{2}));
        assertThrows(IllegalArgumentException.class, () -> new RleEncoder(2).encode(new int[]{-1}));
        assertThrows(IllegalArgumentException.class, () -> new RleEncoder(31).encode(new int[]{Integer.MIN_VALUE}));
    }

    @Test
    void encodesPrimitiveArraysWithoutPerValueBoxingOrIntermediateIntArrays() throws Exception {
        var bean = ManagementFactory.getThreadMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(bean instanceof com.sun.management.ThreadMXBean);
        var allocation = (com.sun.management.ThreadMXBean) bean;
        org.junit.jupiter.api.Assumptions.assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        int[] values = new int[50_000];
        for (int i = 0; i < values.length; i++) values[i] = i & 1023;
        RleEncoder encoder = new RleEncoder(10);
        for (int i = 0; i < 10; i++) encoder.encode(values);
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        byte[] encoded = encoder.encode(values);
        long allocated = allocation.getThreadAllocatedBytes(thread) - before;
        System.out.println("RLE primitive array allocated bytes=" + allocated + ", values=" + values.length);
        assertTrue(encoded.length > 0);
        assertTrue(allocated < 12L * values.length, "Per-value boxing/intermediate array allocation: " + allocated);
    }
}
