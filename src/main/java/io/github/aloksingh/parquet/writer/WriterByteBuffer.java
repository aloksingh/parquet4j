package io.github.aloksingh.parquet.writer;

import java.util.Arrays;

/**
 * Growing little-endian storage; no temporary arrays for individual numbers.
 */
final class WriterByteBuffer {
    private static final int MAX_CAPACITY = Integer.MAX_VALUE - 8;
    byte[] data = new byte[64];
    int size;

    void clear() {
        size = 0;
    }

    void reserve(int additional) {
        long required = (long) size + additional;
        if (additional < 0 || required > MAX_CAPACITY) {
            throw new IllegalArgumentException("Encoded page exceeds the supported byte-array limit");
        }
        if (required > data.length) {
            long grown = Math.max(required, data.length + (long) (data.length >>> 1) + 1);
            data = Arrays.copyOf(data, (int) Math.min(MAX_CAPACITY, grown));
        }
    }

    void putByte(int value) {
        reserve(1);
        data[size++] = (byte) value;
    }

    void putInt(int value) {
        reserve(4);
        data[size++] = (byte) value;
        data[size++] = (byte) (value >>> 8);
        data[size++] = (byte) (value >>> 16);
        data[size++] = (byte) (value >>> 24);
    }

    void putLong(long value) {
        reserve(8);
        for (int shift = 0; shift < 64; shift += 8) data[size++] = (byte) (value >>> shift);
    }

    void putBytes(byte[] bytes) {
        putBytes(bytes, 0, bytes.length);
    }

    void putBytes(byte[] bytes, int offset, int length) {
        reserve(length);
        System.arraycopy(bytes, offset, data, size, length);
        size += length;
    }

    void putUnsignedVarint(long value) {
        while ((value & ~0x7fL) != 0) {
            putByte((int) (value & 0x7f) | 0x80);
            value >>>= 7;
        }
        putByte((int) value);
    }

    byte[] bytes() {
        return Arrays.copyOf(data, size);
    }
}
