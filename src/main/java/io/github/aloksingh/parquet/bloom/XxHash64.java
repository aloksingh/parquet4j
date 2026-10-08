package io.github.aloksingh.parquet.bloom;

/**
 * XXH64 (xxHash 64-bit) implementation per specification v0.7.0, with seed 0.
 * Used by Parquet split-block bloom filters to hash column values before
 * insertion and membership check.
 *
 * <p>Algorithm from:
 * <a href="https://github.com/Cyan4973/xxHash/blob/v0.7.0/doc/xxhash_spec.md">xxHash spec v0.7.0</a>
 */
public final class XxHash64 {

    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long PRIME64_3 = 0x165667B19E3779F9L;
    private static final long PRIME64_4 = 0x85EBCA77C2B2AE63L;
    private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

    private XxHash64() {
    }

    public static long hash(byte[] data) {
        return hash(data, 0, data.length, 0);
    }

    public static long hash(byte[] data, int off, int len, long seed) {
        if (len < 0) throw new IllegalArgumentException("Negative length");
        long h64;
        int end = off + len;
        int pos = off;

        if (len >= 32) {
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed;
            long v4 = seed - PRIME64_1;

            int limit = end - 32;
            do {
                v1 = round(v1, readLE64(data, pos));
                v2 = round(v2, readLE64(data, pos + 8));
                v3 = round(v3, readLE64(data, pos + 16));
                v4 = round(v4, readLE64(data, pos + 24));
                pos += 32;
            } while (pos <= limit);

            h64 = Long.rotateLeft(v1, 1)
                    + Long.rotateLeft(v2, 7)
                    + Long.rotateLeft(v3, 12)
                    + Long.rotateLeft(v4, 18);

            h64 = mergeAv(h64, v1);
            h64 = mergeAv(h64, v2);
            h64 = mergeAv(h64, v3);
            h64 = mergeAv(h64, v4);
        } else {
            h64 = seed + PRIME64_5;
        }

        h64 += len;

        while (pos + 8 <= end) {
            long k1 = round(0, readLE64(data, pos));
            h64 ^= k1;
            h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            pos += 8;
        }

        if (pos + 4 <= end) {
            h64 ^= (readLE32(data, pos) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            pos += 4;
        }

        while (pos < end) {
            h64 ^= (data[pos++] & 0xFFL) * PRIME64_5;
            h64 = Long.rotateLeft(h64, 11) * PRIME64_1;
        }

        return avalanche(h64);
    }

    /**
     * round(acc, lane): acc = (acc + lane * PRIME64_2) <<< 31 * PRIME64_1
     */
    private static long round(long acc, long lane) {
        acc += lane * PRIME64_2;
        acc = Long.rotateLeft(acc, 31);
        acc *= PRIME64_1;
        return acc;
    }

    /**
     * mergeAccumulator: h64 = (h64 ^ round(0, v)) * PRIME64_1 + PRIME64_4
     */
    private static long mergeAv(long h64, long v) {
        h64 ^= round(0, v);
        h64 = h64 * PRIME64_1 + PRIME64_4;
        return h64;
    }

    private static long avalanche(long h64) {
        h64 ^= h64 >>> 33;
        h64 *= PRIME64_2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME64_3;
        h64 ^= h64 >>> 32;
        return h64;
    }

    private static long readLE64(byte[] data, int off) {
        return (data[off] & 0xFFL)
                | ((data[off + 1] & 0xFFL) << 8)
                | ((data[off + 2] & 0xFFL) << 16)
                | ((data[off + 3] & 0xFFL) << 24)
                | ((data[off + 4] & 0xFFL) << 32)
                | ((data[off + 5] & 0xFFL) << 40)
                | ((data[off + 6] & 0xFFL) << 48)
                | ((data[off + 7] & 0xFFL) << 56);
    }

    private static int readLE32(byte[] data, int off) {
        return (data[off] & 0xFF)
                | ((data[off + 1] & 0xFF) << 8)
                | ((data[off + 2] & 0xFF) << 16)
                | ((data[off + 3] & 0xFF) << 24);
    }
}