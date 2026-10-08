package io.github.aloksingh.parquet.bloom;

import java.util.Arrays;

/**
 * Parquet split-block Bloom filter (SBBF) per the format specification.
 * Each block is 256 bits (eight 32-bit words). The filter consists of {@code numBlocks}
 * independent blocks, each holding 32 bytes.
 *
 * <p>A present value hashes to a block index and then sets 8 specific bits
 * (one per word) in that block. A membership check returns false only when
 * the value is definitively absent; true means "probably present". Nulls are
 * NOT inserted or checked — they must be handled at the caller level.
 *
 * <p>Algorithm and constants from
 * <a href="https://github.com/apache/parquet-format/blob/master/BloomFilter.md">BloomFilter.md</a>.
 */
public final class SplitBlockBloomFilter {

    /**
     * Salt constants from the spec.
     */
    static final int[] SALT = {
            0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
            0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };

    private final int[] bitset;  // numBlocks * 8 ints; each int is one 32-bit word
    private final int numBlocks;
    private final int numBytes;

    /**
     * Create an empty filter with the given number of blocks.
     */
    public SplitBlockBloomFilter(int numBlocks) {
        if (numBlocks < 1) throw new IllegalArgumentException("numBlocks must be >= 1");
        this.numBlocks = numBlocks;
        this.numBytes = numBlocks * 32;
        this.bitset = new int[numBlocks * 8];
    }

    /**
     * Create a filter from a serialized bitset and a BloomFilterHeader giving numBytes.
     * The numBytes must match {@code numBlocks * 32} for some numBlocks >= 1.
     *
     * @throws IllegalArgumentException if bitset length doesn't match numBytes
     */
    public SplitBlockBloomFilter(byte[] bitsetBytes, int numBytes) {
        if (numBytes <= 0 || numBytes % 32 != 0) {
            throw new IllegalArgumentException("numBytes must be a positive multiple of 32, got " + numBytes);
        }
        if (bitsetBytes.length < numBytes) {
            throw new IllegalArgumentException("bitset too short: expected " + numBytes + ", got " + bitsetBytes.length);
        }
        this.numBytes = numBytes;
        this.numBlocks = numBytes / 32;
        this.bitset = new int[numBlocks * 8];
        for (int i = 0; i < numBytes; i += 4) {
            bitset[i / 4] = (bitsetBytes[i] & 0xFF)
                    | ((bitsetBytes[i + 1] & 0xFF) << 8)
                    | ((bitsetBytes[i + 2] & 0xFF) << 16)
                    | ((bitsetBytes[i + 3] & 0xFF) << 24);
        }
    }

    public int numBlocks() {
        return numBlocks;
    }

    public int numBytes() {
        return numBytes;
    }

    // ------------------------------------------------------------------ block ops

    /**
     * Compute mask(int32 x): sets one bit in each of the 8 words.
     */
    static int[] mask(int x) {
        int[] m = new int[8];
        for (int i = 0; i < 8; i++) {
            int y = x * SALT[i];           // implicit unsigned 32-bit multiply, keep low 32
            m[i] = 1 << (y >>> 27);        // bit 27..31 → bit index 0..31
        }
        return m;
    }

    /**
     * Insert a 32-bit hash into one block.
     */
    static void blockInsert(int[] block, int base, int x) {
        int[] m = mask(x);
        for (int i = 0; i < 8; i++) {
            block[base + i] |= m[i];
        }
    }

    /**
     * Check a 32-bit hash in one block. Returns false = definitely absent.
     */
    static boolean blockCheck(int[] block, int base, int x) {
        int[] m = mask(x);
        for (int i = 0; i < 8; i++) {
            if ((block[base + i] & m[i]) == 0) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------ filter ops

    /**
     * Insert a 64-bit hash.
     * Block index = ((hash >>> 32) * numBlocks) >>> 32  (unsigned multiply trick).
     * Block receives the low 32 bits of the hash.
     */
    public void insert(long hash) {
        int blockIndex = blockIndex(hash);
        blockInsert(bitset, blockIndex * 8, (int) hash);
    }

    /**
     * Check a 64-bit hash. Returns false when the value was definitely never inserted.
     */
    public boolean check(long hash) {
        int blockIndex = blockIndex(hash);
        return blockCheck(bitset, blockIndex * 8, (int) hash);
    }

    /**
     * Hash a plain-encoded column value (byte array) and check membership.
     * Null is NOT a valid input — callers must handle nulls before calling.
     * BYTE_ARRAY values are hashed without their 4-byte length prefix (raw bytes only).
     *
     * @return false if the value is definitively absent from the filter
     */
    public boolean mightContain(byte[] plainValue) {
        long hash = XxHash64.hash(plainValue, 0, plainValue.length, 0);
        return check(hash);
    }

    /**
     * Serialize the bitset to bytes (little-endian words).
     */
    public byte[] toBytes() {
        byte[] out = new byte[numBytes];
        for (int i = 0; i < bitset.length; i++) {
            int word = bitset[i];
            int off = i * 4;
            out[off] = (byte) word;
            out[off + 1] = (byte) (word >>> 8);
            out[off + 2] = (byte) (word >>> 16);
            out[off + 3] = (byte) (word >>> 24);
        }
        return out;
    }

    /**
     * Select a block index from the upper 32 bits of the hash, per spec.
     */
    private int blockIndex(long hash) {
        long hi = hash >>> 32;                    // upper 32 bits as unsigned 64-bit
        return (int) ((hi * (numBlocks & 0xFFFFFFFFL)) >>> 32);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplitBlockBloomFilter that)) return false;
        return numBlocks == that.numBlocks && Arrays.equals(bitset, that.bitset);
    }

    @Override
    public int hashCode() {
        return numBlocks * 31 + Arrays.hashCode(bitset);
    }

    @Override
    public String toString() {
        return "SBBF{blocks=" + numBlocks + ", bytes=" + numBytes + "}";
    }
}