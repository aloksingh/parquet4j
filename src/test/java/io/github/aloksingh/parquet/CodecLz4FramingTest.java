package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.CompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import net.jpountz.lz4.LZ4Factory;
import org.junit.jupiter.api.Test;

class CodecLz4FramingTest {
    // Independent format bytes: Hadoop BlockCompressorStream.rawWriteInt uses BE integers.
    // One uncompressed block length is followed by one or more BE compressed-chunk lengths.
    // https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/BlockCompressorStream.java
    private static final byte[] ABC = "abc".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] RAW_ABC = {0x30, 'a', 'b', 'c'};
    private static final byte[] HADOOP_ABC = {0, 0, 0, 3, 0, 0, 0, 4, 0x30, 'a', 'b', 'c'};

    @Test
    void writesTheApacheHadoopGoldenFramingInsteadOfAPrivateLittleEndianPrefix() throws Exception {
        assertArrayEquals(HADOOP_ABC, Compressor.create(CompressionCodec.LZ4).compress(ABC));
    }

    @Test
    void readsMultipleHadoopCompressedChunksAndOuterBlocks() throws Exception {
        byte[] golden = {0, 0, 0, 5,
                0, 0, 0, 3, 0x20, 'a', 'b',
                0, 0, 0, 4, 0x30, 'c', 'd', 'e',
                0, 0, 0, 2,
                0, 0, 0, 3, 0x20, 'f', 'g'};
        for (String kind : Arrays.asList("heap", "readOnly", "direct", "slice")) {
            assertArrayEquals("abcdefg".getBytes(StandardCharsets.US_ASCII), PageReaderSafetyTest.bytes(
                    Decompressor.create(CompressionCodec.LZ4).decompress(CodecBufferTest.buffer(golden, kind), 7)));
        }
    }

    @Test
    void doesNotGuessRawDataForAColumnDeclaredAsLegacyLz4() {
        assertThrows(IOException.class, () -> Decompressor.create(CompressionCodec.LZ4)
                .decompress(ByteBuffer.wrap(RAW_ABC), 3));
    }

    @Test
    void offersRawCompressionAsAnUnframedBlockVerifiedByTheIndependentBlockDecoder() throws Exception {
        Compressor raw = assertDoesNotThrow(() -> Compressor.create(CompressionCodec.LZ4_RAW));
        assertArrayEquals(RAW_ABC, raw.compress(ABC));
        byte[] repetitive = new byte[256 * 1024 + 17];
        Arrays.fill(repetitive, (byte) 42);
        byte[] encoded = raw.compress(repetitive);
        assertTrue(encoded.length < repetitive.length, "Compression must actually be retained");
        byte[] independentlyDecoded = new byte[repetitive.length];
        int count = LZ4Factory.fastestJavaInstance().safeDecompressor().decompress(encoded, 0, encoded.length,
                independentlyDecoded, 0, independentlyDecoded.length);
        assertEquals(repetitive.length, count);
        assertArrayEquals(repetitive, independentlyDecoded);
    }

    @Test
    void rawDecoderRejectsBothHadoopAndOldPrivateFramingRatherThanFallingBack() {
        var raw = Decompressor.create(CompressionCodec.LZ4_RAW);
        assertThrows(IOException.class, () -> raw.decompress(ByteBuffer.wrap(HADOOP_ABC), 3));
        byte[] oldPrivate = {4, 0, 0, 0, 0x30, 'a', 'b', 'c'};
        assertThrows(IOException.class, () -> raw.decompress(ByteBuffer.wrap(oldPrivate), 3));
    }

    @Test
    void legacyEmptyStreamIsTheHadoopZeroLengthBlock() throws Exception {
        assertArrayEquals(new byte[4], Compressor.create(CompressionCodec.LZ4).compress(new byte[0]));
        assertEquals(0, Decompressor.create(CompressionCodec.LZ4).decompress(ByteBuffer.wrap(new byte[4]), 0).remaining());
    }

    @Test
    void emittedLargeLegacyBlockStreamDecodesUsingIndependentChunkDecoders() throws Exception {
        byte[] expected = new byte[256 * 1024 + 17];
        Arrays.fill(expected, (byte) 42);
        byte[] encoded = Compressor.create(CompressionCodec.LZ4).compress(expected);
        assertTrue(encoded.length < expected.length);
        ByteBuffer framing = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        assertEquals(expected.length, framing.getInt());
        byte[] actual = new byte[expected.length];
        int written = 0;
        int chunks = 0;
        while (framing.hasRemaining()) {
            int length = framing.getInt();
            assertTrue(length > 0 && length <= framing.remaining());
            byte[] block = new byte[length];
            framing.get(block);
            written += LZ4Factory.fastestJavaInstance().safeDecompressor()
                    .decompress(block, 0, block.length, actual, written, actual.length - written);
            chunks++;
        }
        assertTrue(chunks > 1, "Large input is split into bounded compressed chunks");
        assertEquals(expected.length, written);
        assertArrayEquals(expected, actual);
    }
}
