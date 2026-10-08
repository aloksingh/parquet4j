package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.Encoding;
import org.apache.parquet.format.Statistics;
/**
 * Detached page data. Level sections are raw RLE (no V1 length prefix). Values
 * are PLAIN bytes, or a {@code u8} bit-width byte followed by unprefixed
 * RLE/bit-packed hybrid dictionary indexes for {@link Encoding#RLE_DICTIONARY}.
 * {@code dictionaryBytes} counts dictionary entries first seen in this page; the
 * chunk's dictionary page carries them once.
 */
public record WriterPage(int numValues, int numNulls, int numRows,
                         byte[] repetitions, byte[] definitions, byte[] values,
                         Statistics statistics, Encoding encoding, long dictionaryBytes) {
    /**
     * Contribution of this page to the chunk's encoded size: the page body plus
     * the dictionary entries this page introduced.
     *
     * @return encoded size in bytes
     */
    public long payloadSize() {
        return (long) repetitions.length + definitions.length + values.length + dictionaryBytes;
    }
}
