package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnPageDecoder;
import io.github.aloksingh.parquet.model.DecodedPage;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Type;
import io.github.aloksingh.parquet.model.Page;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class ColumnPageDecoderBooleanTest {
    @org.junit.jupiter.api.Test
    void legacyV2ZeroWidthLevelsAreValidatedWithoutCreatingLevelArrays() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BOOLEAN, 0, 0);
        Page source = new Page.DataPageV2(ByteBuffer.wrap(new byte[]{5}), 3, 0, 3, Encoding.PLAIN,
                ByteBuffer.wrap(new byte[]{6}), ByteBuffer.wrap(new byte[]{6}), false);
        DecodedPage decoded = new ColumnPageDecoder(descriptor).decode(source);
        assertArrayEquals(new boolean[]{true, false, true}, assertInstanceOf(boolean[].class, decoded.values()));
        assertEquals(0, decoded.definitionLevel(2));
        assertEquals(0, decoded.repetitionLevel(2));
    }

    @Test
    void rleBooleanValuesUseAFourByteLengthPrefixInBothPageVersions() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BOOLEAN, 1, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            java.nio.ByteBuffer data = java.nio.ByteBuffer.allocate(6)
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(2).put((byte) 3).put((byte) 5).flip();
            DecodedPage page = new ColumnPageDecoder(descriptor).decode(DecodingTestSupport.page(
                    v2, descriptor, Encoding.RLE, new int[]{1, 0, 1, 1, 1}, new int[5], data));
            assertArrayEquals(new boolean[]{true, false, true, false},
                    assertInstanceOf(boolean[].class, page.values()));
        }
    }

    @Test
    void legacyBooleanAdapterAcceptsStandardV1RleFraming() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BOOLEAN, 1, 0);
        java.nio.ByteBuffer data = java.nio.ByteBuffer.allocate(6)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(2).put((byte) 3).put((byte) 5).flip();
        io.github.aloksingh.parquet.model.Page page = DecodingTestSupport.page(false, descriptor,
                Encoding.RLE, new int[]{1, 0, 1, 1, 1}, new int[5], data);
        io.github.aloksingh.parquet.model.ColumnValues column = new io.github.aloksingh.parquet.model.ColumnValues(
                Type.BOOLEAN, java.util.List.of(page), descriptor, null);
        assertEquals(java.util.Arrays.asList(true, null, false, true, false), column.decodeAsBoolean());
    }

    @Test
    void plainBooleanIsLsbPackedAcrossBytesAndOnlyPresentEventsConsumeBits() {
        boolean[] expected = {true, false, true, true, false, false, true, false, true};
        Object[] boxed = {true, false, true, true, false, false, true, false, true};
        for (boolean v2 : new boolean[]{false, true}) {
            for (int maximum : new int[]{0, 2}) {
                ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BOOLEAN, maximum, 0);
                int[] definitions = maximum == 0 ? new int[9] : new int[]{2, 2, 2, 1, 2, 2, 2, 2, 2, 2};
                DecodedPage page = new ColumnPageDecoder(descriptor).decode(DecodingTestSupport.page(
                        v2, descriptor, Encoding.PLAIN, definitions, new int[definitions.length],
                        DecodingTestSupport.plain(Type.BOOLEAN, boxed)));
                assertArrayEquals(expected, assertInstanceOf(boolean[].class, page.values()));
                assertEquals(9, page.nonNullCount());
                assertEquals(true, page.physicalValue(8));
                assertEquals(0, page.repetitionLevel(0));
                assertEquals(maximum == 0 ? 0 : 1, page.definitionLevel(3));
            }
        }
    }
}
