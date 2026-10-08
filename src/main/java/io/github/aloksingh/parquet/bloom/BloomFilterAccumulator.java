package io.github.aloksingh.parquet.bloom;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Accumulates present column values during writing and builds a split-block
 * bloom filter at row-group flush time. Null values are NOT accumulated.
 *
 * <p>Values are collected as raw PLAIN-encoded bytes. BYTE_ARRAY values strip the
 * 4-byte length prefix, matching the Parquet bloom filter spec: only the raw bytes
 * are hashed.
 *
 * <p>Block count is derived from the false-positive probability target using the
 * bits-per-insert table from BloomFilter.md.
 */
public final class BloomFilterAccumulator {

    private final ColumnDescriptor descriptor;
    private final int targetBlocks;
    private final List<byte[]> presentValues = new ArrayList<>();

    /**
     * @param descriptor the column descriptor
     * @param ndv        expected number of distinct values per row group
     * @param fpp        target false-positive probability (0.0–1.0)
     */
    public BloomFilterAccumulator(ColumnDescriptor descriptor, int ndv, double fpp) {
        if (ndv < 1) throw new IllegalArgumentException("ndv must be >= 1");
        if (fpp <= 0.0 || fpp >= 1.0) throw new IllegalArgumentException("fpp must be in (0, 1)");
        this.descriptor = descriptor;
        this.targetBlocks = blocksForFpp(ndv, fpp);
    }

    /**
     * Record a present (non-null) plain-encoded value.
     */
    public void addPresent(byte[] plainBytes) {
        // Strip BYTE_ARRAY's 4-byte length prefix per spec
        if (descriptor.physicalType() == Type.BYTE_ARRAY && plainBytes.length >= 4) {
            int len = plainBytes.length - 4;
            byte[] stripped = new byte[len];
            System.arraycopy(plainBytes, 4, stripped, 0, len);
            presentValues.add(stripped);
        } else {
            presentValues.add(plainBytes.clone());
        }
    }

    /**
     * Build and return the bloom filter, or null if no values were accumulated.
     */
    public SplitBlockBloomFilter build() {
        if (presentValues.isEmpty()) return null;
        SplitBlockBloomFilter bf = new SplitBlockBloomFilter(targetBlocks);
        for (byte[] value : presentValues) {
            bf.insert(XxHash64.hash(value));
        }
        return bf;
    }

    /**
     * Number of present values accumulated so far.
     */
    public int valueCount() {
        return presentValues.size();
    }

    /**
     * Compute the minimum number of 256-bit blocks needed for the given NDV
     * and false-positive probability, using the BloomFilter.md sizing table.
     *
     * <p>Approximates: bitsPerInsert = 1.44 * log2(1/fpp) / ln(2)
     * = 1.44 * log2(1/fpp) / 0.693 = 2.08 * log2(1/fpp).
     * Then numBlocks = ceil(ndv * bitsPerInsert / 256).
     */
    static int blocksForFpp(int ndv, double fpp) {
        double bitsPerInsert = 2.08 * (Math.log(1.0 / fpp) / Math.log(2.0));
        int totalBits = (int) Math.ceil(ndv * bitsPerInsert);
        return Math.max(1, (totalBits + 255) / 256);
    }
}