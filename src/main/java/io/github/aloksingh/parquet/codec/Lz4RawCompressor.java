package io.github.aloksingh.parquet.codec;

import io.github.aloksingh.parquet.Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * Parquet LZ4_RAW: a single unframed LZ4 block, with no size prefix or padding.
 */
public class Lz4RawCompressor implements Compressor {
    private static final net.jpountz.lz4.LZ4Compressor COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();

    /**
     * Constructs an unframed LZ4 block compressor.
     */
    public Lz4RawCompressor() {
    }

    @Override
    public byte[] compress(byte[] uncompressed) {
        return COMPRESSOR.compress(uncompressed);
    }
}
