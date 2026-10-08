package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.CompressionCodec;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable writer configuration. Controls compression, page/group sizing,
 * conditional-compression thresholds, and bloom filter generation.
 *
 * <p>Bloom filters are generated per-column per row-group. Use
 * {@link Builder#bloomFilter(String, int, double)} to enable them for a
 * column path (the physical path dot-separated, e.g. {@code "name"} or
 * {@code "address.street"}).
 */
public final class WriteOptions {

    public static final WriteOptions DEFAULTS = builder().build();

    private final CompressionCodec compressionCodec;
    private final int pageSize;
    private final int rowGroupSize;
    private final double minCompressionRatio;
    private final Map<String, BloomFilterConfig> bloomFilters;

    private WriteOptions(Builder builder) {
        compressionCodec = builder.compressionCodec;
        pageSize = builder.pageSize;
        rowGroupSize = builder.rowGroupSize;
        minCompressionRatio = builder.minCompressionRatio;
        bloomFilters = Collections.unmodifiableMap(new LinkedHashMap<>(builder.bloomFilters));
    }

    public static Builder builder() {
        return new Builder();
    }

    public CompressionCodec compressionCodec() {
        return compressionCodec;
    }

    public int pageSize() {
        return pageSize;
    }

    public int rowGroupSize() {
        return rowGroupSize;
    }

    public double minCompressionRatio() {
        return minCompressionRatio;
    }

    /**
     * Map from physical column path to bloom filter config. Never null.
     */
    public Map<String, BloomFilterConfig> bloomFilters() {
        return bloomFilters;
    }

    /**
     * Bloom filter configuration for one column.
     */
    public record BloomFilterConfig(int ndv, double fpp) {
        public BloomFilterConfig {
            if (ndv < 1) throw new IllegalArgumentException("ndv must be >= 1");
            if (fpp <= 0.0 || fpp >= 1.0) throw new IllegalArgumentException("fpp must be in (0, 1)");
        }
    }

    public static final class Builder {
        private CompressionCodec compressionCodec = CompressionCodec.UNCOMPRESSED;
        private int pageSize = 1024 * 1024;
        private int rowGroupSize = 128 * 1024 * 1024;
        private double minCompressionRatio = 0.90;
        private final Map<String, BloomFilterConfig> bloomFilters = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder compressionCodec(CompressionCodec codec) {
            if (codec == null) throw new IllegalArgumentException("codec must not be null");
            compressionCodec = codec;
            return this;
        }

        public Builder pageSize(int bytes) {
            if (bytes <= 0) throw new IllegalArgumentException("pageSize must be positive");
            pageSize = bytes;
            return this;
        }

        public Builder rowGroupSize(int bytes) {
            if (bytes <= 0) throw new IllegalArgumentException("rowGroupSize must be positive");
            rowGroupSize = bytes;
            return this;
        }

        public Builder minCompressionRatio(double ratio) {
            if (!Double.isFinite(ratio) || ratio < 0 || ratio > 1) {
                throw new IllegalArgumentException("Compression ratio must be finite [0, 1]");
            }
            minCompressionRatio = ratio;
            return this;
        }

        /**
         * Enable a bloom filter for the given physical column path.
         *
         * @param columnPath the physical column path (e.g. {@code "name"} or {@code "address.street"})
         * @param ndv        expected number of distinct values per row group
         * @param fpp        target false-positive probability (e.g. 0.01 for 1%)
         */
        public Builder bloomFilter(String columnPath, int ndv, double fpp) {
            Objects.requireNonNull(columnPath, "columnPath");
            bloomFilters.put(columnPath, new BloomFilterConfig(ndv, fpp));
            return this;
        }

        public WriteOptions build() {
            return new WriteOptions(this);
        }
    }
}