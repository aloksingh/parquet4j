package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilter;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * Immutable scan options. A projection is expressed in logical column names.
 */
public final class ReadOptions {
    public static final ReadOptions DEFAULT = builder().build();

    private final List<String> projection;
    private final int batchSize;
    private final long maxBatchBytes;
    private final long limit;
    private final RowColumnGroupFilter filter;
    private final boolean logicalTypes;
    private final boolean pruning;
    private final boolean verifyChecksums;
    private final int maxHeaderBytes;
    private final int maxCompressedPageBytes;
    private final int maxUncompressedPageBytes;
    private final int maxValuesPerPage;

    private ReadOptions(Builder builder) {
        projection = builder.projection == null ? null : List.copyOf(builder.projection);
        batchSize = builder.batchSize;
        maxBatchBytes = builder.maxBatchBytes;
        limit = builder.limit;
        filter = builder.filter;
        logicalTypes = builder.logicalTypes;
        pruning = builder.pruning;
        verifyChecksums = builder.verifyChecksums;
        maxHeaderBytes = builder.maxHeaderBytes;
        maxCompressedPageBytes = builder.maxCompressedPageBytes;
        maxUncompressedPageBytes = builder.maxUncompressedPageBytes;
        maxValuesPerPage = builder.maxValuesPerPage;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<String> projection() {
        return projection;
    }

    public boolean projectsAllColumns() {
        return projection == null;
    }

    /**
     * Maximum rows decoded and retained per batch; row iteration never holds more.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Approximate materialized-value budget per batch; one oversized row is always allowed.
     */
    public long maxBatchBytes() {
        return maxBatchBytes;
    }

    public long limit() {
        return limit;
    }

    /**
     * Residual row predicate; its extra columns join the scan but never surface in rows.
     */
    public RowColumnGroupFilter filter() {
        return filter;
    }

    public boolean logicalTypes() {
        return logicalTypes;
    }

    /**
     * Whether statistics may skip row groups that no row can match (default enabled).
     */
    public boolean pruning() {
        return pruning;
    }

    public boolean verifyChecksums() {
        return verifyChecksums;
    }

    public int maxHeaderBytes() {
        return maxHeaderBytes;
    }

    public int maxCompressedPageBytes() {
        return maxCompressedPageBytes;
    }

    public int maxUncompressedPageBytes() {
        return maxUncompressedPageBytes;
    }

    public int maxValuesPerPage() {
        return maxValuesPerPage;
    }

    public static final class Builder {
        private List<String> projection;
        private int batchSize = 1024;
        private long maxBatchBytes = 8L * 1024 * 1024;
        private long limit = Long.MAX_VALUE;
        private RowColumnGroupFilter filter;
        private boolean logicalTypes = true;
        private boolean pruning = true;
        private boolean verifyChecksums;
        private int maxHeaderBytes = 1024 * 1024;
        private int maxCompressedPageBytes = 64 * 1024 * 1024;
        private int maxUncompressedPageBytes = 128 * 1024 * 1024;
        private int maxValuesPerPage = 16 * 1024 * 1024;

        private Builder() {
        }

        /**
         * Select only these logical columns; an empty projection selects no values.
         */
        public Builder project(String... names) {
            Objects.requireNonNull(names, "projection");
            List<String> copy = List.of(names.clone());
            for (String name : copy) {
                if (name.isBlank()) throw new IllegalArgumentException("Projection names must not be blank");
            }
            if (new HashSet<>(copy).size() != copy.size()) {
                throw new IllegalArgumentException("Duplicate projection columns");
            }
            projection = copy;
            return this;
        }

        public Builder batchSize(int value) {
            if (value < 1) throw new IllegalArgumentException("batchSize must be positive");
            batchSize = value;
            return this;
        }

        /**
         * Target result-buffer budget; one oversized row is subject to page limits.
         */
        public Builder maxBatchBytes(long value) {
            if (value < 1) throw new IllegalArgumentException("maxBatchBytes must be positive");
            maxBatchBytes = value;
            return this;
        }

        public Builder limit(long value) {
            if (value < 0) throw new IllegalArgumentException("limit must not be negative");
            limit = value;
            return this;
        }

        public Builder filter(RowColumnGroupFilter value) {
            filter = Objects.requireNonNull(value, "filter");
            return this;
        }

        public Builder logicalTypes(boolean value) {
            logicalTypes = value;
            return this;
        }

        public Builder pruning(boolean value) {
            pruning = value;
            return this;
        }

        public Builder verifyChecksums(boolean value) {
            verifyChecksums = value;
            return this;
        }

        public Builder pageLimits(int headerBytes, int compressedBytes, int uncompressedBytes, int values) {
            if (headerBytes < 1 || compressedBytes < 1 || uncompressedBytes < 1 || values < 1) {
                throw new IllegalArgumentException("Page limits must be positive");
            }
            maxHeaderBytes = headerBytes;
            maxCompressedPageBytes = compressedBytes;
            maxUncompressedPageBytes = uncompressedBytes;
            maxValuesPerPage = values;
            return this;
        }

        public ReadOptions build() {
            return new ReadOptions(this);
        }
    }
}
