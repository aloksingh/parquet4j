package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ParquetException;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StorageDeltaValidationTest {
    @Test
    void boundsWireCountsBeforeAnyValueOrMiniblockAllocation() {
        byte[] header = stream(128, 4, PageReadOptions.DEFAULT.maxValuesPerPage() + 1, 0, new byte[0]);
        assertThrows(ParquetException.class, () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(header), false));
        byte[] maximal = stream(128, 4, Integer.MAX_VALUE, 0, new byte[0]);
        assertThrows(ParquetException.class, () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(maximal), false));
    }

    @Test
    void exposesACallerBoundWithoutRejectingAStandardBlockLargerThanThePageCount() throws Exception {
        var constructor = assertDoesNotThrow(() -> DeltaBinaryPackedDecoder.class.getConstructor(
                ByteBuffer.class, boolean.class, int.class));
        byte[] bytes = stream(128, 4, 2, 1, new byte[]{2, 0, 0, 0, 0});
        InvocationTargetException tooMany = assertThrows(InvocationTargetException.class,
                () -> constructor.newInstance(ByteBuffer.wrap(bytes), false, 1));
        assertInstanceOf(ParquetException.class, tooMany.getCause());
        assertTrue(tooMany.getCause().getMessage().contains("value count"));
        DeltaBinaryPackedDecoder decoder = constructor.newInstance(ByteBuffer.wrap(bytes), false, 2);
        assertArrayEquals(new int[]{1, 2}, decoder.decodeInt32(2));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidShapes")
    void validatesBlockAndMiniblockShapesBeforeDivisionOrAllocation(String label, int block, int mini) {
        byte[] bytes = stream(block, mini, 1, 0, new byte[0]);
        assertThrows(ParquetException.class, () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(bytes), false), label);
    }

    static Stream<Arguments> invalidShapes() {
        return Stream.of(Arguments.of("zero block", 0, 1), Arguments.of("not multiple of 128", 5, 1),
                Arguments.of("zero miniblocks", 128, 0), Arguments.of("miniblocks do not divide block", 128, 5),
                Arguments.of("miniblocks too small", 128, 8), Arguments.of("overflowed unsigned block", -128, 4));
    }

    @Test
    void validatesAllUsedMiniblockBytesBeforeAllocatingTheRequestedOutput() {
        // A huge padded miniblock but only two logical values: do not allocate by block size.
        byte[] bytes = stream(1024 * 1024, 1, 2, 0, new byte[]{0, 1});
        assertThrows(ParquetException.class, () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(bytes), true));
        byte[] shortWidths = stream(128, 4, 2, 0, new byte[]{0, 0});
        assertThrows(ParquetException.class, () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(shortWidths), false));
    }

    @Test
    void rejectsUsedBitWidthsBeyondThePhysicalIntegerWidth() {
        for (boolean wide : new boolean[]{false, true}) {
            int width = wide ? 65 : 33;
            byte[] body = new byte[5 + 32 * width / 8];
            body[1] = (byte) width;
            byte[] bytes = stream(128, 4, 2, 0, body);
            assertThrows(ParquetException.class, () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(bytes), wide)
                    .decodeInt64(2));
        }
    }

    @Test
    void rejectsCallerCountsLargerThanTheWireCountRatherThanZeroFilling() {
        byte[] bytes = stream(128, 4, 1, 5, new byte[0]);
        assertThrows(ParquetException.class,
                () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(bytes), false).decodeInt32(2));
        assertThrows(IllegalArgumentException.class,
                () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(bytes), false).decodeInt32(-1));
    }

    @Test
    void rejectsTruncatedOrOverflowingVarintsAndOutOfRangeInt32FirstValues() {
        assertThrows(ParquetException.class, () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(new byte[]{(byte) 0x80}), false));
        byte[] overflowing = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x10, 1, 1, 0};
        assertThrows(ParquetException.class, () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(overflowing), false));
        byte[] wrongFirst = stream(128, 4, 1, (long) Integer.MAX_VALUE + 1, new byte[0]);
        assertThrows(ParquetException.class, () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(wrongFirst), false));
    }

    @Test
    void acceptsArbitraryUnusedMiniblockWidthBytesWithoutConsumingANextStream() {
        byte[] encoded = stream(128, 4, 2, 1, new byte[]{2, 0, (byte) 255, (byte) 255, (byte) 255});
        for (String kind : new String[]{"heap", "readOnly", "direct", "slice"}) {
            ByteBuffer bytes = CodecBufferTest.buffer(encoded, kind);
            int initial = bytes.position();
            DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(bytes, false);
            assertArrayEquals(new int[]{1, 2}, decoder.decodeInt32(2));
            assertEquals(initial + encoded.length, bytes.position());
            assertEquals(encoded.length, decoder.getBytesConsumed());
        }
    }

    static byte[] stream(int block, int mini, int count, long first, byte[] body) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        unsigned(output, block & 0xffffffffL);
        unsigned(output, mini & 0xffffffffL);
        unsigned(output, count & 0xffffffffL);
        unsigned(output, (first << 1) ^ (first >> 63));
        output.writeBytes(body);
        return output.toByteArray();
    }

    static void unsigned(ByteArrayOutputStream output, long value) {
        while ((value & ~0x7fL) != 0) {
            output.write((int) (value & 0x7f) | 0x80);
            value >>>= 7;
        }
        output.write((int) value);
    }
}
