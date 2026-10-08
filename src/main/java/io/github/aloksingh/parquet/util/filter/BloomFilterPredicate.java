package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.bloom.SplitBlockBloomFilter;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.nio.charset.StandardCharsets;

/**
 * Converts filter constants to their plain-encoded bytes for bloom filter lookup.
 * Returns true when the value might be present (bloom filter does NOT exclude it).
 * Returns false when the bloom filter definitively excludes the value.
 *
 * <p>Encoding follows Parquet's PLAIN encoding rules:
 * <ul>
 *   <li>BYTE_ARRAY: raw bytes, omit the 4-byte length prefix</li>
 *   <li>FIXED_LEN_BYTE_ARRAY: raw bytes</li>
 *   <li>INT32/INT64/DATE/TIME: little-endian four/eight bytes</li>
 *   <li>FLOAT/DOUBLE: IEEE 754 little-endian</li>
 *   <li>BOOLEAN: single byte 0 or 1</li>
 * </ul>
 */
final class BloomFilterPredicate {

    private BloomFilterPredicate() {
    }

    /**
     * Check whether the bloom filter might contain the given constant value.
     *
     * @return true if the value might be present (including on error/unsupported type);
     * false only when the bloom filter definitively excludes it
     */
    static boolean mightContain(SplitBlockBloomFilter bf, ColumnDescriptor descriptor, Object constant) {
        if (bf == null || constant == null) return true;
        byte[] plain = toPlainBytes(descriptor.physicalType(), constant);
        if (plain == null) return true; // unsupported type: don't drop
        return bf.mightContain(plain);
    }

    /**
     * Convert a Java value to its PLAIN-encoded byte representation.
     * Returns null for types that cannot be bloom-filter-checked.
     */
    static byte[] toPlainBytes(Type type, Object value) {
        return switch (type) {
            case BOOLEAN -> {
                if (!(value instanceof Boolean)) yield null;
                yield new byte[]{(byte) (((Boolean) value) ? 1 : 0)};
            }
            case INT32 -> {
                if (!(value instanceof Integer)) yield null;
                yield int32Bytes((Integer) value);
            }
            case INT64 -> {
                if (!(value instanceof Long)) yield null;
                yield int64Bytes((Long) value);
            }
            case FLOAT -> {
                if (!(value instanceof Float)) yield null;
                yield int32Bytes(Float.floatToRawIntBits((Float) value));
            }
            case DOUBLE -> {
                if (!(value instanceof Double)) yield null;
                yield int64Bytes(Double.doubleToRawLongBits((Double) value));
            }
            case BYTE_ARRAY -> {
                if (value instanceof String s) yield s.getBytes(StandardCharsets.UTF_8);
                if (value instanceof byte[] b) yield b.clone();
                yield null;
            }
            case FIXED_LEN_BYTE_ARRAY -> {
                if (value instanceof byte[] b) yield b.clone();
                yield null;
            }
            default -> null;
        };
    }

    private static byte[] int32Bytes(int v) {
        byte[] b = new byte[4];
        b[0] = (byte) v;
        b[1] = (byte) (v >>> 8);
        b[2] = (byte) (v >>> 16);
        b[3] = (byte) (v >>> 24);
        return b;
    }

    private static byte[] int64Bytes(long v) {
        byte[] b = new byte[8];
        b[0] = (byte) v;
        b[1] = (byte) (v >>> 8);
        b[2] = (byte) (v >>> 16);
        b[3] = (byte) (v >>> 24);
        b[4] = (byte) (v >>> 32);
        b[5] = (byte) (v >>> 40);
        b[6] = (byte) (v >>> 48);
        b[7] = (byte) (v >>> 56);
        return b;
    }
}