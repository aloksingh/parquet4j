package io.github.aloksingh.parquet.writer;

import org.apache.parquet.format.Statistics;

/**
 * Detached page data. Level sections are raw RLE (no V1 length prefix).
 */
public record WriterPage(int numValues, int numNulls, int numRows,
                         byte[] repetitions, byte[] definitions, byte[] values,
                         Statistics statistics) {
    public long payloadSize() {
        return (long) repetitions.length + definitions.length + values.length;
    }
}
