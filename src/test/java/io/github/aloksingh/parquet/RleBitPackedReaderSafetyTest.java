package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ParquetException;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class RleBitPackedReaderSafetyTest {
    @Test
    void calculatesBooleanByteCountsWithoutSignedIntegerOverflow() {
        assertEquals((Integer.MAX_VALUE + 7L) / 8, BitPackedReader.bytesForBits(Integer.MAX_VALUE));
        assertEquals(0, BitPackedReader.bytesForBits(0));
        assertThrows(IllegalArgumentException.class, () -> BitPackedReader.bytesForBits(-1));
    }

    @Test
    void validatesBooleanCountsAndPayloadBeforeAllocatingOrConsuming() {
        assertThrows(IllegalArgumentException.class, () -> BitPackedReader.readBooleans(ByteBuffer.allocate(0), -1));
        ByteBuffer shortBuffer = ByteBuffer.wrap(new byte[]{0x4d});
        assertThrows(ParquetException.class, () -> BitPackedReader.readBooleans(shortBuffer, 9));
        assertEquals(0, shortBuffer.position(), "Underflow must be detected before consuming the payload");
    }

    @Test
    void supportsReadOnlyDirectAndSlicedBooleanBuffersIncludingAPartialFinalByte() {
        byte[] encoded = {0x4d, 0x01};
        boolean[] expected = {true, false, true, true, false, false, true, false, true, false};
        for (String kind : new String[]{"heap", "readOnly", "direct", "slice"}) {
            ByteBuffer input = CodecBufferTest.buffer(encoded, kind);
            int initial = input.position();
            assertArrayEquals(expected, BitPackedReader.readBooleans(input, expected.length));
            assertEquals(initial + encoded.length, input.position());
        }
    }
}
