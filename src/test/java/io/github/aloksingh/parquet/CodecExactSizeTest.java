package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CodecExactSizeTest {
    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"UNCOMPRESSED", "GZIP", "SNAPPY", "ZSTD", "LZ4", "LZ4_RAW"})
    void rejectsOutputMismatchesAndTruncationWithoutChangingInputState(CompressionCodec codec) throws Exception {
        byte[] expected = new byte[64];
        for (int i = 0; i < expected.length; i++) expected[i] = (byte) (i * 7 + 3);
        byte[] encoded = Compressor.create(codec).compress(expected);
        for (int declared : new int[]{0, 1, expected.length - 1, expected.length + 1}) {
            ByteBuffer input = CodecBufferTest.buffer(encoded, "readOnly");
            int position = input.position();
            int limit = input.limit();
            assertThrows(IOException.class, () -> Decompressor.create(codec).decompress(input, declared));
            assertEquals(position, input.position());
            assertEquals(limit, input.limit());
        }
        byte[] truncated = java.util.Arrays.copyOf(encoded, encoded.length - 1);
        assertThrows(IOException.class,
                () -> Decompressor.create(codec).decompress(ByteBuffer.wrap(truncated), expected.length));
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"UNCOMPRESSED", "GZIP", "SNAPPY", "ZSTD", "LZ4", "LZ4_RAW"})
    void enforcesHardDefaultLimitsBeforeAnyOutputAllocation(CompressionCodec codec) {
        assertThrows(IOException.class, () -> Decompressor.create(codec).decompress(ByteBuffer.allocate(0), Integer.MAX_VALUE));
        assertThrows(IOException.class, () -> Decompressor.create(codec).decompress(ByteBuffer.allocate(0),
                PageReadOptions.DEFAULT.maxUncompressedPageBytes() + 1));
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"UNCOMPRESSED", "GZIP", "SNAPPY", "ZSTD", "LZ4", "LZ4_RAW"})
    void supportsCorrectlyEncodedEmptyBodies(CompressionCodec codec) throws Exception {
        byte[] encoded = Compressor.create(codec).compress(new byte[0]);
        ByteBuffer decoded = Decompressor.create(codec).decompress(ByteBuffer.wrap(encoded), 0);
        assertEquals(0, decoded.remaining());
        assertTrue(decoded.isReadOnly());
    }
}
