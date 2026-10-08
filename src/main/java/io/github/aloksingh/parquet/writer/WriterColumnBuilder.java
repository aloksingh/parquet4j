package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.bloom.BloomFilterAccumulator;
import io.github.aloksingh.parquet.bloom.SplitBlockBloomFilter;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * Byte-sized, row-boundary column pages for one row group; no row objects are retained.
 * When dictionary encoding is enabled for the column, one chunk-scoped dictionary is
 * shared by all pages of the row group and reset at row-group boundaries; BOOLEAN
 * columns are never dictionary encoded. Flush decisions use estimated encoded sizes.
 *
 * <p>When a {@link BloomFilterAccumulator} is set, each appended row's present values
 * are also fed to the accumulator so a bloom filter can be built at row-group flush time.
 */
public final class WriterColumnBuilder {
    public static final int MAX_PAGE_BODY_SIZE = Integer.MAX_VALUE - 8;
    private final ColumnDescriptor descriptor;
    private final WriterDictionary dictionary;
    private final int pageTarget;
    private final int byteLimit;
    private final int valueLimit;
    private final WriterColumnBuffer current;
    private final List<WriterPage> pages = new ArrayList<>();
    private final WriterStatistics statistics;
    private final BloomFilterAccumulator bloomAccumulator;
    private long completedBytes;
    private long numValues;

    public WriterColumnBuilder(ColumnDescriptor descriptor, int pageTarget) {
        this(descriptor, pageTarget, MAX_PAGE_BODY_SIZE, Integer.MAX_VALUE, DictionaryOptions.disabled());
    }

    public WriterColumnBuilder(ColumnDescriptor descriptor, int pageTarget, int byteLimit, int valueLimit,
                               DictionaryOptions dictionaryOptions) {
        this(descriptor, pageTarget, byteLimit, valueLimit, dictionaryOptions, null);
    }

    public WriterColumnBuilder(ColumnDescriptor descriptor, int pageTarget, int byteLimit, int valueLimit,
                               DictionaryOptions dictionaryOptions, BloomFilterAccumulator bloomAccumulator) {
        if (pageTarget <= 0) throw new IllegalArgumentException("Page target must be positive");
        if (dictionaryOptions == null) throw new IllegalArgumentException("Dictionary options must not be null");
        this.descriptor = descriptor;
        this.pageTarget = Math.min(pageTarget, byteLimit);
        this.byteLimit = byteLimit;
        this.valueLimit = valueLimit;
        this.dictionary = dictionaryOptions.dictionaryEnabled()
                && descriptor.physicalType() != Type.BOOLEAN
                ? new WriterDictionary(dictionaryOptions.maxDictionaryBytes()) : null;
        current = new WriterColumnBuffer(descriptor, byteLimit, valueLimit, dictionary);
        statistics = new WriterStatistics(descriptor);
        this.bloomAccumulator = bloomAccumulator;
    }

    public void clear() {
        current.clear();
        pages.clear();
        statistics.clear();
        if (dictionary != null) dictionary.reset();
        completedBytes = 0;
        numValues = 0;
        // Bloom accumulator is NOT cleared — it persists across pages within a row group.
        // The writer clears/rebuilds it at row-group boundaries.
    }

    private boolean splits(WriterColumnBuffer row) {
        return current.numRows() > 0 && (current.projectedPayloadSize(row) > pageTarget
                || (long) current.numValues() + row.numValues() > valueLimit
                || current.numRows() == Integer.MAX_VALUE);
    }

    public long projectedPayloadSize(WriterColumnBuffer row) {
        if (row.payloadSize() > byteLimit || row.numValues() > valueLimit) {
            throw new IllegalArgumentException("One row exceeds the supported Parquet page byte limit");
        }
        return Math.addExact(completedBytes, current.projectedPayloadSize(row));
    }

    public void appendRow(WriterColumnBuffer row) {
        projectedPayloadSize(row); // Check the entire row before changing this builder.
        if (splits(row)) finishPage();
        current.appendRow(row);
        statistics.merge(row.statistics());
        numValues = Math.addExact(numValues, row.numValues());
        if (bloomAccumulator != null) {
            row.forEachPresentValue(bloomAccumulator::addPresent);
        }
    }

    /**
     * Build the bloom filter from accumulated present values and clear the accumulator.
     * Returns null when no bloom filter is configured or no values were accumulated.
     */
    public SplitBlockBloomFilter buildBloomFilter() {
        if (bloomAccumulator == null) return null;
        return bloomAccumulator.build();
    }

    private void finishPage() {
        if (current.numRows() == 0) return;
        WriterPage page = current.snapshotPage();
        pages.add(page);
        completedBytes = Math.addExact(completedBytes, page.payloadSize());
        current.clear();
    }

    public List<WriterPage> finishAndGetPages() {
        finishPage();
        return List.copyOf(pages);
    }

    public ColumnDescriptor descriptor() {
        return descriptor;
    }

    /**
     * The chunk-scoped dictionary for this column, or null when the column is
     * written PLAIN. Valid until {@link #clear()} resets it.
     *
     * @return the dictionary, or null
     */
    public WriterDictionary dictionary() {
        return dictionary;
    }

    public long numValues() {
        return numValues;
    }

    public WriterStatistics statistics() {
        return statistics;
    }
}
