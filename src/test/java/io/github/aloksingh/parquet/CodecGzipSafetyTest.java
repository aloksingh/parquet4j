package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CodecGzipSafetyTest {
    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1, 63, 65})
    void rejectsEveryDeclaredOutputSizeMismatch(int declared) throws Exception {
        byte[] encoded = Compressor.create(CompressionCodec.GZIP).compress(new byte[64]);
        assertThrows(IOException.class,
                () -> Decompressor.create(CompressionCodec.GZIP).decompress(ByteBuffer.wrap(encoded), declared));
    }

    @Test
    void reconstructsExactlyTheDeclaredSizeAcrossConcatenatedMembers() throws Exception {
        var compressor = Compressor.create(CompressionCodec.GZIP);
        byte[] first = compressor.compress(new byte[]{1, 2});
        byte[] second = compressor.compress(new byte[]{3, 4});
        ByteBuffer joined = ByteBuffer.allocate(first.length + second.length).put(first).put(second).flip();
        assertArrayEquals(new byte[]{1, 2, 3, 4}, PageReaderSafetyTest.bytes(
                Decompressor.create(CompressionCodec.GZIP).decompress(joined, 4)));
    }
}
