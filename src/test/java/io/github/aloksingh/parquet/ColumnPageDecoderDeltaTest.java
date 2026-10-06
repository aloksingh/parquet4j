package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnPageDecoder;
import io.github.aloksingh.parquet.model.DecodedPage;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ColumnPageDecoderDeltaTest {
    @org.junit.jupiter.api.Test
    void deltaPrefixesReconstructExactValuesIntoASinglePayload() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            java.nio.ByteBuffer prefixes = DecodingTestSupport.delta(0, 4, 3);
            java.nio.ByteBuffer lengths = DecodingTestSupport.delta(5, 1, 0);
            java.nio.ByteBuffer encoded = java.nio.ByteBuffer.allocate(prefixes.remaining() + lengths.remaining() + 6);
            encoded.put(prefixes).put(lengths).put(new byte[]{'a', 'p', 'p', 'l', 'e', 'y'}).flip();
            DecodedPage page = new ColumnPageDecoder(descriptor).decode(DecodingTestSupport.page(
                    v2, descriptor, Encoding.DELTA_BYTE_ARRAY, new int[]{2, 1, 2, 2}, new int[4], encoded));
            io.github.aloksingh.parquet.model.BinaryValues values = assertInstanceOf(
                    io.github.aloksingh.parquet.model.BinaryValues.class, page.values());
            assertArrayEquals(new int[]{0, 5, 10, 13}, values.offsets());
            assertArrayEquals(new byte[]{'a', 'p', 'p', 'l', 'e'}, values.bytesAt(0));
            assertArrayEquals(new byte[]{'a', 'p', 'p', 'l', 'y'}, values.bytesAt(1));
            assertArrayEquals(new byte[]{'a', 'p', 'p'}, values.bytesAt(2));
        }
    }

    @org.junit.jupiter.api.Test
    void deltaLengthsKeepVariableBinaryValuesAsSharedDirectBufferViews() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            java.nio.ByteBuffer lengths = DecodingTestSupport.consecutiveDelta(2, 1, 3);
            int payloadStart = lengths.remaining();
            java.nio.ByteBuffer encoded = java.nio.ByteBuffer.allocateDirect(payloadStart + 9);
            encoded.put(lengths).put(new byte[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'}).flip();
            // V1 includes inline levels, so use V2 to verify borrowing the original direct payload.
            DecodedPage page = new ColumnPageDecoder(descriptor).decode(DecodingTestSupport.page(
                    v2, descriptor, Encoding.DELTA_LENGTH_BYTE_ARRAY, new int[]{2, 1, 2, 2}, new int[4],
                    encoded.asReadOnlyBuffer()));
            io.github.aloksingh.parquet.model.BinaryValues values = assertInstanceOf(
                    io.github.aloksingh.parquet.model.BinaryValues.class, page.values());
            assertArrayEquals(new int[]{0, 2, 5, 9}, values.offsets());
            assertArrayEquals(new byte[]{'a', 'b'}, values.bytesAt(0));
            assertArrayEquals(new byte[]{'c', 'd', 'e'}, values.bytesAt(1));
            assertArrayEquals(new byte[]{'f', 'g', 'h', 'i'}, values.bytesAt(2));
            assertTrue(values.data().isReadOnly());
            if (v2) {
                assertTrue(values.data().isDirect());
                encoded.put(payloadStart, (byte) 'z');
                assertArrayEquals(new byte[]{'z', 'b'}, values.bytesAt(0));
            }
            assertEquals(0, encoded.position());
        }
    }

    @ParameterizedTest
    @EnumSource(value = Type.class, names = {"INT32", "INT64"})
    void deltaBinaryPackedProducesPrimitiveArraysForPresentEventsInBothVersions(Type type) {
        long first = type == Type.INT32 ? -100 : -9_000_000_000L;
        for (boolean v2 : new boolean[]{false, true}) {
            for (int maximum : new int[]{0, 2}) {
                ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, maximum, 0);
                int[] definitions = maximum == 0 ? new int[3] : new int[]{2, 1, 2, 2};
                DecodedPage page = new ColumnPageDecoder(descriptor).decode(DecodingTestSupport.page(
                        v2, descriptor, Encoding.DELTA_BINARY_PACKED, definitions, new int[definitions.length],
                        DecodingTestSupport.consecutiveDelta(first, 3, 3)));
                assertEquals(3, page.nonNullCount());
                if (type == Type.INT32) {
                    assertArrayEquals(new int[]{-100, -97, -94}, assertInstanceOf(int[].class, page.values()));
                } else {
                    assertArrayEquals(new long[]{-9_000_000_000L, -8_999_999_997L, -8_999_999_994L},
                            assertInstanceOf(long[].class, page.values()));
                }
            }
        }
    }
}
