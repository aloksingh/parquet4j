package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.RleEncoder;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.ParquetException;

import java.io.IOException;
import java.util.Arrays;

/**
 * One encoded column page (also reused for isolated, not-yet-accepted row staging).
 *
 * <p>Page-role buffers may hold a chunk-scoped {@link WriterDictionary}: present
 * values are recorded as dictionary ids while the dictionary accepts, and as raw
 * PLAIN slices after fallback. Staging buffers never touch the dictionary, so a
 * row staged before a row-group flush cannot leak entries into the closing chunk.
 * Flush sizing uses estimated encoded sizes (an upper bound of the index stream
 * plus dictionary entries and un-indexed PLAIN bytes); the hard byte limits bound
 * that same estimated contribution of a single row.</p>
 */
public final class WriterColumnBuffer {
    private static final int[] NO_IDS = new int[0];

    private final ColumnDescriptor descriptor;
    private final WriterPlainBuffer values;
    private final WriterLevelBuffer definitions = new WriterLevelBuffer();
    private final WriterLevelBuffer repetitions = new WriterLevelBuffer();
    private final WriterStatistics statistics;
    private final WriterDictionary dictionary;
    private final int byteLimit;
    private final int valueLimit;
    private int numValues;
    private int numRows;
    private int presentCount;
    private int[] ids = NO_IDS;
    private int idCount;
    private int maxId = -1;
    private long newEntryBytes;

    public WriterColumnBuffer(ColumnDescriptor descriptor) {
        this(descriptor, WriterColumnBuilder.MAX_PAGE_BODY_SIZE, Integer.MAX_VALUE);
    }

    public WriterColumnBuffer(ColumnDescriptor descriptor, int byteLimit, int valueLimit) {
        this(descriptor, byteLimit, valueLimit, null);
    }

    WriterColumnBuffer(ColumnDescriptor descriptor, int byteLimit, int valueLimit, WriterDictionary dictionary) {
        if (byteLimit <= 0 || byteLimit > WriterColumnBuilder.MAX_PAGE_BODY_SIZE || valueLimit <= 0) {
            throw new IllegalArgumentException("Invalid hard page limits");
        }
        this.descriptor = descriptor;
        this.byteLimit = byteLimit;
        this.valueLimit = valueLimit;
        this.dictionary = dictionary;
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
        presentCount = 0;
        idCount = 0;
        maxId = -1;
        newEntryBytes = 0;
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
        if (snapshot != null) {
            values.add(snapshot);
            presentCount = Math.addExact(presentCount, 1);
        }
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
        mergeValues(row);
        definitions.append(row.definitions);
        repetitions.append(row.repetitions);
        statistics.merge(row.statistics);
        checkByteLimit();
    }

    private void mergeValues(WriterColumnBuffer row) {
        if (dictionary == null) {
            values.append(row.values);
            presentCount = Math.addExact(presentCount, row.presentCount);
            return;
        }
        byte[] data = row.values.data();
        int offset = 0;
        for (int i = 0; i < row.presentCount; i++) {
            int length = WriterDictionary.sliceLength(descriptor.physicalType(), descriptor.typeLength(), data, offset);
            if (dictionary.accepting()) {
                long before = dictionary.entryBytes();
                int id = dictionary.idFor(data, offset, length);
                if (id >= 0) {
                    newEntryBytes = Math.addExact(newEntryBytes, dictionary.entryBytes() - before);
                    recordId(id);
                    offset += length;
                    continue;
                }
            }
            values.appendRaw(data, offset, length);
            offset += length;
        }
        presentCount = Math.addExact(presentCount, row.presentCount);
    }

    private void recordId(int id) {
        if (idCount == ids.length) {
            ids = Arrays.copyOf(ids, Math.max(8, idCount * 2));
        }
        ids[idCount++] = id;
        if (id > maxId) {
            maxId = id;
        }
    }

    private void checkByteLimit() {
        if (payloadSize() > byteLimit)
            throw new IllegalArgumentException("One row exceeds the Parquet page byte limit");
    }

    /**
     * Estimated encoded size of this buffer merged with one staged row: an upper
     * bound of the hybrid index stream (packed bits plus one byte per value and
     * one width byte), the PLAIN bytes of un-indexed values, and the dictionary
     * entries the merge would introduce.
     */
    public long projectedPayloadSize(WriterColumnBuffer row) {
        long levels = 0;
        if (descriptor.maxDefinitionLevel() > 0) levels += definitions.projectedRawSize(row.definitions);
        if (descriptor.maxRepetitionLevel() > 0) levels += repetitions.projectedRawSize(row.repetitions);
        if (dictionary == null) {
            return levels + values.projectedSize(row.values);
        }
        long virtualEntries = dictionary.entryBytes();
        boolean accepting = dictionary.accepting();
        int rowIds = 0;
        long rowPlain = 0;
        long rowNew = 0;
        int simInserts = 0;
        int simMax = maxId;
        byte[] data = row.values.data();
        int offset = 0;
        for (int i = 0; i < row.presentCount; i++) {
            int length = WriterDictionary.sliceLength(descriptor.physicalType(), descriptor.typeLength(), data, offset);
            if (accepting) {
                int id = dictionary.find(data, offset, length);
                if (id >= 0) {
                    rowIds++;
                    if (id > simMax) simMax = id;
                    offset += length;
                    continue;
                }
                if (virtualEntries + (long) length <= dictionary.maxBytes()) {
                    virtualEntries += length;
                    rowNew += length;
                    rowIds++;
                    int newId = dictionary.size() + simInserts++;
                    if (newId > simMax) simMax = newId;
                    offset += length;
                    continue;
                }
                accepting = false;
            }
            rowPlain += length;
            offset += length;
        }
        int mergedMax = Math.max(maxId, simMax);
        return levels + encodedIndexEstimate(idCount + rowIds, mergedMax)
                + values.size() + rowPlain + newEntryBytes + rowNew;
    }

    /**
     * Estimated encoded size of the buffered values and level sections. For
     * PLAIN-only buffers this is the exact payload size.
     */
    public long payloadSize() {
        return (long) values.size() + definitions.rawSize() + repetitions.rawSize()
                + encodedIndexEstimate(idCount, maxId) + newEntryBytes;
    }

    private static long encodedIndexEstimate(int ids, int maxId) {
        if (ids == 0) return 0;
        int width = RleEncoder.bitWidth(maxId);
        return 1 + (long) ids * ((width + 7) / 8 + 1);
    }

    public WriterPage snapshotPage() {
        byte[] valueRegion;
        Encoding encoding;
        if (dictionary != null && idCount == presentCount && presentCount > 0) {
            encoding = Encoding.RLE_DICTIONARY;
            valueRegion = encodeIndexes();
        } else {
            encoding = Encoding.PLAIN;
            valueRegion = plainValues();
        }
        return new WriterPage(numValues, Math.toIntExact(statistics.nullCount()), numRows,
                repetitions.raw(), definitions.raw(), valueRegion, statistics.toParquet(),
                encoding, newEntryBytes);
    }

    private byte[] encodeIndexes() {
        int width = RleEncoder.bitWidth(maxId);
        byte[] raw;
        try {
            raw = new RleEncoder(width).encodeRaw(Arrays.copyOf(ids, idCount));
        } catch (IOException failure) {
            throw new ParquetException("Failed to encode dictionary indexes", failure);
        }
        byte[] stream = new byte[raw.length + 1];
        stream[0] = (byte) width;
        System.arraycopy(raw, 0, stream, 1, raw.length);
        return stream;
    }

    private byte[] plainValues() {
        if (idCount == 0) return values.bytes();
        WriterByteBuffer out = new WriterByteBuffer();
        for (int i = 0; i < idCount; i++) {
            out.putBytes(dictionary.entry(ids[i]));
        }
        out.putBytes(values.bytes());
        return out.bytes();
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

    /**
     * Feed each present (non-null) PLAIN-encoded value to the consumer.
     * Staging buffers only use PLAIN encoding — dictionary ids are not materialized here.
     */
    void forEachPresentValue(java.util.function.Consumer<byte[]> consumer) {
        if (presentCount == 0) return;
        values.forEachPresent(consumer);
    }

    public WriterStatistics statistics() {
        return statistics;
    }
}
