package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.ColumnDescriptor;

/**
 * One encoded column page (also reused for isolated, not-yet-accepted row staging).
 */
public final class WriterColumnBuffer {
    private final ColumnDescriptor descriptor;
    private final WriterPlainBuffer values;
    private final WriterLevelBuffer definitions = new WriterLevelBuffer();
    private final WriterLevelBuffer repetitions = new WriterLevelBuffer();
    private final WriterStatistics statistics;
    private final int byteLimit;
    private final int valueLimit;
    private int numValues;
    private int numRows;

    public WriterColumnBuffer(ColumnDescriptor descriptor) {
        this(descriptor, WriterColumnBuilder.MAX_PAGE_BODY_SIZE, Integer.MAX_VALUE);
    }

    public WriterColumnBuffer(ColumnDescriptor descriptor, int byteLimit, int valueLimit) {
        if (byteLimit <= 0 || byteLimit > WriterColumnBuilder.MAX_PAGE_BODY_SIZE || valueLimit <= 0) {
            throw new IllegalArgumentException("Invalid hard page limits");
        }
        this.descriptor = descriptor;
        this.byteLimit = byteLimit;
        this.valueLimit = valueLimit;
        values = new WriterPlainBuffer(descriptor.physicalType());
        statistics = new WriterStatistics(descriptor);
    }

    public void clear() {
        values.clear();
        definitions.clear();
        repetitions.clear();
        statistics.clear();
        numValues = 0;
        numRows = 0;
    }

    public Object add(Object value, int definition, int repetition, boolean nullable) {
        if (numValues == valueLimit) throw new IllegalArgumentException("One row exceeds the Parquet page event limit");
        Object snapshot = WriterValues.snapshot(descriptor, value, nullable);
        if (definition < 0 || definition > descriptor.maxDefinitionLevel()
                || repetition < 0 || repetition > descriptor.maxRepetitionLevel()
                || (snapshot != null && definition != descriptor.maxDefinitionLevel())) {
            throw new IllegalArgumentException("Invalid definition/repetition event for " + descriptor.getPathString());
        }
        numValues = Math.addExact(numValues, 1);
        if (descriptor.maxDefinitionLevel() > 0) definitions.add(definition);
        if (descriptor.maxRepetitionLevel() > 0) repetitions.add(repetition);
        if (snapshot != null) values.add(snapshot);
        statistics.add(snapshot);
        checkByteLimit();
        return snapshot;
    }

    public void appendRow(WriterColumnBuffer row) {
        if ((long) numValues + row.numValues > valueLimit) {
            throw new IllegalArgumentException("Parquet page event limit exceeded");
        }
        numValues = Math.addExact(numValues, row.numValues);
        numRows = Math.addExact(numRows, 1);
        values.append(row.values);
        definitions.append(row.definitions);
        repetitions.append(row.repetitions);
        statistics.merge(row.statistics);
        checkByteLimit();
    }

    private void checkByteLimit() {
        if (payloadSize() > byteLimit)
            throw new IllegalArgumentException("One row exceeds the Parquet page byte limit");
    }

    public long projectedPayloadSize(WriterColumnBuffer row) {
        long size = values.projectedSize(row.values);
        if (descriptor.maxDefinitionLevel() > 0) size += definitions.projectedRawSize(row.definitions);
        if (descriptor.maxRepetitionLevel() > 0) size += repetitions.projectedRawSize(row.repetitions);
        return size;
    }

    public long payloadSize() {
        return (long) values.size() + definitions.rawSize() + repetitions.rawSize();
    }

    public WriterPage snapshotPage() {
        return new WriterPage(numValues, Math.toIntExact(statistics.nullCount()), numRows,
                repetitions.raw(), definitions.raw(), values.bytes(), statistics.toParquet());
    }

    public ColumnDescriptor descriptor() {
        return descriptor;
    }

    public int numValues() {
        return numValues;
    }

    public int numRows() {
        return numRows;
    }

    public WriterStatistics statistics() {
        return statistics;
    }
}
