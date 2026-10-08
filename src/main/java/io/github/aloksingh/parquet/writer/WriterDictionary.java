package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.Type;

import java.util.*;

/**
 * Chunk-scoped dictionary of PLAIN-encoded value slices for one column chunk.
 *
 * <p>Keys are the exact PLAIN encoding of each value (injective per physical
 * type, so FLOAT/DOUBLE raw bit patterns and byte-array contents are preserved
 * verbatim). Entries are detached copies and their concatenation is the
 * dictionary page body. When an entry would push the total past the byte
 * budget, the dictionary stops accepting and the writer falls back to PLAIN
 * for the remaining values of the chunk.</p>
 */
public final class WriterDictionary {
    private final int maxBytes;
    private final Map<DictKey, Integer> ids = new HashMap<>();
    private final List<byte[]> entries = new ArrayList<>();
    private long entryBytes;
    private boolean fallback;

    WriterDictionary(int maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("Dictionary byte limit must be positive");
        }
        this.maxBytes = maxBytes;
    }

    /**
     * Whether the dictionary still accepts new entries; once the byte budget is
     * exceeded it stops accepting for the remainder of the chunk.
     *
     * @return true while new values may still be added
     */
    public boolean accepting() {
        return !fallback;
    }

    /**
     * Number of distinct entries in the dictionary.
     *
     * @return entry count
     */
    public int size() {
        return entries.size();
    }

    /**
     * Total PLAIN size of all entries (the uncompressed dictionary page body size).
     *
     * @return body size in bytes
     */
    public long entryBytes() {
        return entryBytes;
    }

    /**
     * The dictionary byte budget.
     *
     * @return maximum total entry bytes
     */
    public int maxBytes() {
        return maxBytes;
    }

    /**
     * Looks up a PLAIN-encoded value slice without inserting.
     *
     * @param data   buffer holding the PLAIN encoding
     * @param offset start of the value slice
     * @param length length of the value slice
     * @return the entry id, or -1 when the value is not in the dictionary
     */
    int find(byte[] data, int offset, int length) {
        Integer id = ids.get(new DictKey(data, offset, length));
        return id == null ? -1 : id;
    }

    /**
     * Returns the id for a PLAIN-encoded value slice, inserting it when new.
     *
     * @param data   buffer holding the PLAIN encoding
     * @param offset start of the value slice
     * @param length length of the value slice
     * @return the entry id, or -1 when the budget is exceeded (fallback armed)
     */
    int idFor(byte[] data, int offset, int length) {
        int found = find(data, offset, length);
        if (found >= 0) {
            return found;
        }
        if (entryBytes + (long) length > maxBytes) {
            fallback = true;
            return -1;
        }
        byte[] copy = Arrays.copyOfRange(data, offset, offset + length);
        int id = entries.size();
        entries.add(copy);
        ids.put(new DictKey(copy, 0, copy.length), id);
        entryBytes += length;
        return id;
    }

    /**
     * Returns the detached PLAIN encoding of one entry (for page reconstruction).
     *
     * @param id entry id
     * @return the PLAIN value bytes
     */
    public byte[] entry(int id) {
        return entries.get(id);
    }

    /**
     * Builds the dictionary page body: every entry's PLAIN encoding concatenated
     * in id order.
     *
     * @return the dictionary page body
     */
    public byte[] pageBody() {
        WriterByteBuffer body = new WriterByteBuffer();
        body.reserve(Math.toIntExact(entryBytes));
        for (byte[] entry : entries) {
            body.putBytes(entry);
        }
        return body.bytes();
    }

    /**
     * Clears all entries and re-arms the budget for the next column chunk.
     */
    void reset() {
        ids.clear();
        entries.clear();
        entryBytes = 0;
        fallback = false;
    }

    /**
     * Length of one PLAIN-encoded value slice in a value stream.
     *
     * @param type       physical type (BOOLEAN streams are never sliced)
     * @param typeLength FIXED_LEN_BYTE_ARRAY width
     * @param data       buffer holding the PLAIN encoding
     * @param offset     start of the value slice
     * @return the slice length in bytes
     */
    static int sliceLength(Type type, int typeLength, byte[] data, int offset) {
        return switch (type) {
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            case FIXED_LEN_BYTE_ARRAY -> typeLength;
            case BYTE_ARRAY -> {
                int length = (data[offset] & 0xff) | ((data[offset + 1] & 0xff) << 8)
                        | ((data[offset + 2] & 0xff) << 16) | ((data[offset + 3] & 0xff) << 24);
                if (length < 0) {
                    throw new IllegalArgumentException("Invalid byte array value length: " + length);
                }
                yield 4 + length;
            }
            default -> throw new IllegalArgumentException("Values of type " + type + " are not dictionary encoded");
        };
    }

    /**
     * Content-addressed key over one PLAIN value slice.
     */
    private static final class DictKey {
        private final byte[] data;
        private final int offset;
        private final int length;
        private final int hash;

        DictKey(byte[] data, int offset, int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
            int value = 1;
            for (int i = offset; i < offset + length; i++) {
                value = 31 * value + data[i];
            }
            this.hash = value;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof DictKey key) || key.length != length) {
                return false;
            }
            return Arrays.mismatch(data, offset, offset + length, key.data, key.offset, key.offset + key.length) < 0;
        }
    }
}
