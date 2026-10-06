package io.github.aloksingh.parquet.model;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Binary values in a single shared payload, with one terminal offset.
 * Buffers are read-only views; callers must keep the underlying page storage alive
 * and must not modify it while these views are in use.
 */
public final class BinaryValues {
    private final int[] offsets;
    private final ByteBuffer data;

    BinaryValues(int[] offsets, ByteBuffer data) {
        this.offsets = offsets;
        this.data = data.slice().asReadOnlyBuffer();
    }

    /**
     * Returns a copy of the offsets, including the terminal payload offset.
     */
    public int[] offsets() {
        return offsets.clone();
    }

    /**
     * Returns an independent read-only view of the complete payload.
     */
    public ByteBuffer data() {
        return data.asReadOnlyBuffer();
    }

    public int size() {
        return offsets.length - 1;
    }

    /**
     * Copies one value.
     */
    public byte[] bytesAt(int index) {
        Objects.checkIndex(index, size());
        int start = offsets[index];
        byte[] bytes = new byte[offsets[index + 1] - start];
        data.get(start, bytes);
        return bytes;
    }

    /**
     * Returns an independent read-only view of one value.
     */
    public ByteBuffer byteBuffer(int index) {
        Objects.checkIndex(index, size());
        return data.slice(offsets[index], offsets[index + 1] - offsets[index]).asReadOnlyBuffer();
    }
}
