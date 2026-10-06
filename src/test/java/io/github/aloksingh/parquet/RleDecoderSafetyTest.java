package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ParquetException;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RleDecoderSafetyTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("malformed")
    void rejectsMalformedRunsInsteadOfZeroFilling(String label, byte[] bytes, int width) {
        RleDecoder decoder = new RleDecoder(ByteBuffer.wrap(bytes), width, 1);
        assertThrows(ParquetException.class, decoder::readNext, label);
    }

    static Stream<Arguments> malformed() {
        return Stream.of(
                Arguments.of("bit-packed header without payload", new byte[]{3}, 1),
                Arguments.of("partial bit-packed group must still have a full payload", new byte[]{3, 1}, 3),
                Arguments.of("RLE header without one-byte payload", new byte[]{2}, 1),
                Arguments.of("repeated value exceeds bit width", new byte[]{2, 3}, 1),
                Arguments.of("RLE header without multi-byte payload", new byte[]{2}, 16),
                Arguments.of("partially present RLE value", new byte[]{2, 1}, 16),
                Arguments.of("unterminated varint at width zero", new byte[]{(byte) 0x82}, 0),
                Arguments.of("zero-length RLE run", new byte[]{0}, 0),
                Arguments.of("zero-length bit-packed run", new byte[]{1}, 0),
                Arguments.of("overflowed bit-packed value count", new byte[]{(byte) 0xff, (byte) 0xff,
                        (byte) 0xff, (byte) 0xff, 0x0f}, 1),
                Arguments.of("varint larger than unsigned 32 bits", new byte[]{(byte) 0x82, (byte) 0x80,
                        (byte) 0x80, (byte) 0x80, 0x10}, 0));
    }

    @Test
    void decodesWidth32RepeatedValuesFromIndependentBytes() {
        byte[] encoded = {6, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        assertArrayEquals(new int[]{-1, -1, -1}, new RleDecoder(ByteBuffer.wrap(encoded), 32, 3).readAll());
    }

    @Test
    void decodesWidth32BitPackedValuesWithoutConfusingMinusOneWithEof() {
        int[] expected = {-1, Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 1, 0x12345678, -2, -1234567};
        ByteBuffer encoded = ByteBuffer.allocate(1 + 32).order(ByteOrder.LITTLE_ENDIAN).put((byte) 3);
        for (int value : expected) encoded.putInt(value);
        encoded.flip();
        RleDecoder decoder = new RleDecoder(encoded, 32, expected.length);
        assertArrayEquals(expected, decoder.readAll());
        assertEquals(-1, decoder.readNext());
    }

    @Test
    void readsALegalLargeRepeatedRunWithoutAllocatingForItsAdvertisedLength() {
        byte[] encoded = {(byte) 0xfe, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0f, 7};
        assertArrayEquals(new int[]{7, 7}, new RleDecoder(ByteBuffer.wrap(encoded), 3, 2).readAll());
    }

    @Test
    void readAllReturnsTheUndecodedRemainderAfterIncrementalReads() {
        RleDecoder decoder = new RleDecoder(ByteBuffer.wrap(new byte[]{6, 2}), 2, 3);
        assertEquals(2, decoder.readNext());
        assertArrayEquals(new int[]{2, 2}, decoder.readAll());
        assertArrayEquals(new int[0], decoder.readAll());
        assertEquals(-1, decoder.readNext());
    }

    @Test
    void validatesWidthAndCountAtConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new RleDecoder(ByteBuffer.allocate(0), -1, 1));
        assertThrows(IllegalArgumentException.class, () -> new RleDecoder(ByteBuffer.allocate(0), 33, 1));
        assertThrows(IllegalArgumentException.class, () -> new RleDecoder(ByteBuffer.allocate(0), 1, -1));
    }

    @ParameterizedTest(name = "width {0}")
    @MethodSource("widths")
    void agreesWithAnIndependentBitwiseOracleIncludingCheckedTails(int width) {
        int[] expected = new int[40];
        Random random = new Random(width);
        long mask = width == 32 ? 0xffffffffL : (1L << width) - 1;
        for (int i = 0; i < expected.length; i++) expected[i] = (int) (random.nextInt() & mask);
        if (width == 32) expected[0] = -1;
        byte[] packed = independentPack(expected, width);
        byte[] encoded = new byte[1 + packed.length];
        encoded[0] = 11; // five groups of eight values
        System.arraycopy(packed, 0, encoded, 1, packed.length);
        for (String kind : new String[]{"heap", "readOnly", "direct", "slice"}) {
            ByteBuffer buffer = CodecBufferTest.buffer(encoded, kind);
            int position = buffer.position();
            assertArrayEquals(expected, new RleDecoder(buffer, width, expected.length).readAll());
            assertEquals(position, buffer.position());
            assertArrayEquals(java.util.Arrays.copyOf(expected, 13), new RleDecoder(buffer, width, 13).readAll());
        }
    }

    static Stream<Integer> widths() {
        return IntStream.rangeClosed(0, 32).boxed();
    }

    static byte[] independentPack(int[] values, int width) {
        byte[] packed = new byte[(values.length * width + 7) / 8];
        for (int i = 0; i < values.length; i++) {
            for (int bit = 0; bit < width; bit++) {
                int index = i * width + bit;
                if (((values[i] >>> bit) & 1) != 0) packed[index / 8] |= (byte) (1 << (index % 8));
            }
        }
        return packed;
    }
}
