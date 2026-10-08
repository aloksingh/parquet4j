package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.Type;

final class WriterPlainBuffer {
    private final Type type;
    private final WriterByteBuffer bytes = new WriterByteBuffer();
    private int booleanCount;

    WriterPlainBuffer(Type type) {
        this.type = type;
    }

    void clear() {
        bytes.clear();
        booleanCount = 0;
    }

    void add(Object value) {
        switch (type) {
            case BOOLEAN -> booleanValue((Boolean) value);
            case INT32 -> bytes.putInt(((Number) value).intValue());
            case INT64 -> bytes.putLong(((Number) value).longValue());
            case FLOAT -> bytes.putInt(Float.floatToRawIntBits(((Number) value).floatValue()));
            case DOUBLE -> bytes.putLong(Double.doubleToRawLongBits(((Number) value).doubleValue()));
            case BYTE_ARRAY -> {
                byte[] binary = (byte[]) value;
                bytes.reserve(Math.addExact(4, binary.length));
                bytes.putInt(binary.length);
                bytes.putBytes(binary);
            }
            case FIXED_LEN_BYTE_ARRAY -> bytes.putBytes((byte[]) value);
            case INT96 -> throw new IllegalArgumentException("INT96 writing is not supported");
        }
    }

    private void booleanValue(boolean value) {
        int bit = booleanCount & 7;
        if (bit == 0) bytes.putByte(0);
        if (value) bytes.data[bytes.size - 1] |= (byte) (1 << bit);
        booleanCount = Math.addExact(booleanCount, 1);
    }

    void append(WriterPlainBuffer row) {
        if (type == Type.BOOLEAN) {
            for (int i = 0; i < row.booleanCount; i++) {
                booleanValue((row.bytes.data[i >>> 3] & (1 << (i & 7))) != 0);
            }
        } else {
            bytes.putBytes(row.bytes.data, 0, row.bytes.size);
        }
    }

    /**
     * Backing array of the raw PLAIN stream (only for non-BOOLEAN types).
     */
    byte[] data() {
        if (type == Type.BOOLEAN) {
            throw new IllegalStateException("Boolean values are bit-packed, not raw slices");
        }
        return bytes.data;
    }

    /**
     * Appends one raw PLAIN value slice (only for non-BOOLEAN types).
     */
    void appendRaw(byte[] data, int offset, int length) {
        if (type == Type.BOOLEAN) {
            throw new IllegalStateException("Boolean values are bit-packed, not raw slices");
        }
        bytes.putBytes(data, offset, length);
    }

    long projectedSize(WriterPlainBuffer row) {
        return type == Type.BOOLEAN ? ((long) booleanCount + row.booleanCount + 7) / 8
                : (long) bytes.size + row.bytes.size;
    }

    int size() {
        return bytes.size;
    }

    byte[] bytes() {
        return bytes.bytes();
    }
}
