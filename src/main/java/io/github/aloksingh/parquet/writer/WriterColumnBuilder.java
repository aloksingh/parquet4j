package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.ColumnDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Byte-sized, row-boundary column pages for one row group; no row objects are retained.
 */
public final class WriterColumnBuilder {
    public static final int MAX_PAGE_BODY_SIZE = Integer.MAX_VALUE - 8;
    private final ColumnDescriptor descriptor;
    private final int pageTarget;
    private final int byteLimit;
    private final int valueLimit;
    private final WriterColumnBuffer current;
    private final List<WriterPage> pages = new ArrayList<>();
    private final WriterStatistics statistics;
    private long completedBytes;
    private long numValues;

    public WriterColumnBuilder(ColumnDescriptor descriptor, int pageTarget) {
        this(descriptor, pageTarget, MAX_PAGE_BODY_SIZE, Integer.MAX_VALUE);
    }

    public WriterColumnBuilder(ColumnDescriptor descriptor, int pageTarget, int byteLimit, int valueLimit) {
        if (pageTarget <= 0) throw new IllegalArgumentException("Page target must be positive");
        this.descriptor = descriptor;
        this.pageTarget = Math.min(pageTarget, byteLimit);
        this.byteLimit = byteLimit;
        this.valueLimit = valueLimit;
        current = new WriterColumnBuffer(descriptor, byteLimit, valueLimit);
        statistics = new WriterStatistics(descriptor);
    }

    public void clear() {
        current.clear();
        pages.clear();
        statistics.clear();
        completedBytes = 0;
        numValues = 0;
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
        long prospective = splits(row) ? current.payloadSize() + row.payloadSize()
                : current.projectedPayloadSize(row);
        return Math.addExact(completedBytes, prospective);
    }

    public void appendRow(WriterColumnBuffer row) {
        projectedPayloadSize(row); // Check the entire row before changing this builder.
        if (splits(row)) finishPage();
        current.appendRow(row);
        statistics.merge(row.statistics());
        numValues = Math.addExact(numValues, row.numValues());
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

    public long numValues() {
        return numValues;
    }

    public WriterStatistics statistics() {
        return statistics;
    }
}
