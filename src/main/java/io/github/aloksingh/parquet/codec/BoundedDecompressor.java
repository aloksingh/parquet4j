package io.github.aloksingh.parquet.codec;

import io.github.aloksingh.parquet.Decompressor;
import io.github.aloksingh.parquet.PageReadOptions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Shared allocation and output invariants, also applied to direct codec use outside PageReader.
 */
abstract class BoundedDecompressor implements Decompressor {
    private final PageReadOptions options;

    BoundedDecompressor() {
        this(PageReadOptions.DEFAULT);
    }

    BoundedDecompressor(PageReadOptions options) {
        this.options = Objects.requireNonNull(options, "options");
    }

    @Override
    public final ByteBuffer decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
        Objects.requireNonNull(compressed, "compressed");
        if (uncompressedSize < 0 || uncompressedSize > options.maxUncompressedPageBytes()) {
            throw new IOException("Invalid uncompressed size: " + uncompressedSize
                    + " (limit " + options.maxUncompressedPageBytes() + ")");
        }
        if (compressed.remaining() > options.maxCompressedPageBytes()) {
            throw new IOException("Compressed input exceeds byte limit " + options.maxCompressedPageBytes());
        }
        if (compressed.remaining() == 0) {
            // An empty stored values section (e.g. an all-null DataPageV2) is a valid empty
            // block for every codec; a non-empty declared output from empty input is corrupt.
            if (uncompressedSize != 0) {
                throw new IOException("Decompressed size mismatch: expected " + uncompressedSize
                        + ", got 0 (empty compressed input)");
            }
            return ByteBuffer.allocate(0).asReadOnlyBuffer();
        }
        try {
            ByteBuffer output = decompressBounded(compressed.duplicate(), uncompressedSize);
            if (output.remaining() != uncompressedSize) {
                throw new IOException("Decompressed size mismatch: expected " + uncompressedSize
                        + ", got " + output.remaining());
            }
            return output.slice().asReadOnlyBuffer();
        } catch (RuntimeException e) {
            // Native and block codecs report malformed input as several unchecked exception types.
            throw new IOException("Invalid " + getClass().getSimpleName() + " compressed data", e);
        }
    }

    protected abstract ByteBuffer decompressBounded(ByteBuffer compressed, int uncompressedSize) throws IOException;
}
