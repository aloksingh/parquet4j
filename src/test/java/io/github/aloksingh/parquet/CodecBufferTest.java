package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CodecBufferTest {
    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("cases")
    void usesOnlyTheRemainingRangeWithoutMutatingTheInput(CompressionCodec codec, String kind) throws Exception {
        byte[] expected = new byte[65];
        for (int i = 0; i < expected.length; i++) expected[i] = (byte) (i * 7 + 3);
        byte[] encoded = Compressor.create(codec).compress(expected);
        ByteBuffer input = buffer(encoded, kind);
        int position = input.position();
        int limit = input.limit();
        ByteBuffer decoded = Decompressor.create(codec).decompress(input, expected.length);
        assertArrayEquals(expected, PageReaderSafetyTest.bytes(decoded));
        assertEquals(position, input.position(), "codec must not consume caller buffer");
        assertEquals(limit, input.limit());
        assertEquals(0, decoded.position());
        assertEquals(expected.length, decoded.limit());
        assertTrue(decoded.isReadOnly());
    }

    static Stream<Arguments> cases() {
        return Stream.of(CompressionCodec.UNCOMPRESSED, CompressionCodec.GZIP, CompressionCodec.SNAPPY,
                        CompressionCodec.ZSTD, CompressionCodec.LZ4, CompressionCodec.LZ4_RAW)
                .flatMap(codec -> Stream.of("heap", "readOnly", "direct", "slice").map(kind -> Arguments.of(codec, kind)));
    }

    static ByteBuffer buffer(byte[] bytes, String kind) {
        ByteBuffer buffer = kind.equals("direct") ? ByteBuffer.allocateDirect(bytes.length + 8)
                : ByteBuffer.allocate(bytes.length + 8);
        buffer.position(3).put(bytes).putInt(0xdeadbeef);
        buffer.position(3).limit(3 + bytes.length).order(ByteOrder.LITTLE_ENDIAN);
        return switch (kind) {
            case "readOnly" -> buffer.asReadOnlyBuffer();
            case "slice" -> {
                buffer.position(2);
                ByteBuffer slice = buffer.slice();
                slice.position(1);
                yield slice;
            }
            default -> buffer;
        };
    }
}
