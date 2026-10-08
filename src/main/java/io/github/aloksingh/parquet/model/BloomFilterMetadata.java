package io.github.aloksingh.parquet.model;

import java.util.Objects;

/**
 * Parsed bloom filter information for a column chunk.
 * Contains the Thrift header fields and (when loaded) the bitset.
 *
 * @param numBytes    size of the bitset in bytes
 * @param algorithm   algorithm name ("BLOCK" for split-block)
 * @param hash        hash function name ("XXHASH")
 * @param compression compression name ("UNCOMPRESSED")
 */
public record BloomFilterMetadata(int numBytes, String algorithm, String hash, String compression) {

    public BloomFilterMetadata {
        if (numBytes <= 0) throw new IllegalArgumentException("numBytes must be positive");
        Objects.requireNonNull(algorithm, "algorithm");
        Objects.requireNonNull(hash, "hash");
        Objects.requireNonNull(compression, "compression");
    }

    /**
     * True when the algorithm/hash/compression are the expected split-block values.
     */
    public boolean isSupported() {
        return "BLOCK".equals(algorithm) && "XXHASH".equals(hash) && "UNCOMPRESSED".equals(compression);
    }
}