package io.github.aloksingh.parquet.codec;

import io.github.aloksingh.parquet.PageReadOptions;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

/**
 * Parquet LZ4_RAW decoder: exactly one unframed block, never Hadoop or private framing.
 */
public class Lz4RawDecompressor extends BoundedDecompressor {
    private static final LZ4SafeDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor();

    /**
     * Constructs an unframed block decoder with safe default allocation bounds.
     */
    public Lz4RawDecompressor() {
    }

    /**
     * Constructs an unframed block decoder with explicit allocation bounds.
     *
     * @param options input and output byte limits
     */
    public Lz4RawDecompressor(PageReadOptions options) {
        super(options);
    }

    @Override
    protected ByteBuffer decompressBounded(ByteBuffer compressed, int uncompressedSize) throws IOException {
        ByteBuffer output = ByteBuffer.allocate(uncompressedSize);
        int count = DECOMPRESSOR.decompress(compressed, compressed.position(), compressed.remaining(),
                output, 0, uncompressedSize);
        if (count != uncompressedSize) {
            throw new IOException("Raw LZ4 output size mismatch: expected " + uncompressedSize + ", got " + count);
        }
        return output;
    }
}
