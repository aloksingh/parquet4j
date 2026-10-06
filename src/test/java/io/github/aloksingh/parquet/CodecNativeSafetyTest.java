package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class CodecNativeSafetyTest {
    @Test
    void checksSnappysEncodedLengthBeforeCallingNativeDecompression() throws Exception {
        byte[] compressed = Compressor.create(CompressionCodec.SNAPPY).compress(new byte[64]);
        IOException error = assertThrows(IOException.class,
                () -> Decompressor.create(CompressionCodec.SNAPPY).decompress(ByteBuffer.wrap(compressed), 1));
        assertTrue(error.getMessage().contains("Snappy size mismatch"), error.toString());
        assertTrue(error.getMessage().contains("64"), "Report the encoded size rather than only a native failure");
    }

    @Test
    void distinguishesAZstdNativeErrorCodeFromAnActualOutputByteCount() {
        IOException error = assertThrows(IOException.class,
                () -> Decompressor.create(CompressionCodec.ZSTD).decompress(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}), 4));
        assertTrue(error.getMessage().contains("ZSTD decompression failed"), error.toString());
    }
}
