package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class CodecBoundsTest {
    @Test
    void offersAnExplicitBoundedFactoryThatHonorsCustomInputAndOutputLimits() throws Exception {
        var factory = assertDoesNotThrow(() -> Decompressor.class.getMethod("create",
                CompressionCodec.class, PageReadOptions.class));
        byte[] gzip = Compressor.create(CompressionCodec.GZIP).compress(new byte[4]);
        var outputLimited = (Decompressor) factory.invoke(null, CompressionCodec.GZIP,
                new PageReadOptions(64, 1024, 3, 10, false));
        assertThrows(IOException.class, () -> outputLimited.decompress(ByteBuffer.wrap(gzip), 4));
        var inputLimited = (Decompressor) factory.invoke(null, CompressionCodec.GZIP,
                new PageReadOptions(64, 1, 1024, 10, false));
        assertThrows(IOException.class, () -> inputLimited.decompress(ByteBuffer.wrap(gzip), 4));
        var permitted = (Decompressor) factory.invoke(null, CompressionCodec.GZIP,
                new PageReadOptions(64, 1024, 1024, 10, false));
        assertEquals(4, permitted.decompress(ByteBuffer.wrap(gzip), 4).remaining());
    }

    @ParameterizedTest
    @EnumSource(value = CompressionCodec.class, names = {"UNCOMPRESSED", "GZIP", "SNAPPY", "ZSTD", "LZ4"})
    void rejectsNegativeOutputSizesBeforeAllocating(CompressionCodec codec) throws Exception {
        byte[] encoded = Compressor.create(codec).compress(new byte[4]);
        assertThrows(IOException.class, () -> Decompressor.create(codec).decompress(ByteBuffer.wrap(encoded), -1));
    }
}
