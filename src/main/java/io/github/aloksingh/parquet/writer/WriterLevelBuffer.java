package io.github.aloksingh.parquet.writer;

/**
 * Streaming RLE runs: repeated null/definition events do not consume per-row storage.
 */
final class WriterLevelBuffer {
    private final WriterByteBuffer completed = new WriterByteBuffer();
    private int lastLevel = -1;
    private int runLength;

    void clear() {
        completed.clear();
        lastLevel = -1;
        runLength = 0;
    }

    void add(int level) {
        if (level < 0 || level > 3) throw new IllegalArgumentException("Unsupported writer level: " + level);
        addRun(level, 1);
    }

    private void addRun(int level, int count) {
        if (count == 0) return;
        if (lastLevel == level) {
            runLength = Math.addExact(runLength, count);
        } else {
            if (runLength != 0) {
                completed.putUnsignedVarint((long) runLength << 1);
                completed.putByte(lastLevel);
            }
            lastLevel = level;
            runLength = count;
        }
    }

    void append(WriterLevelBuffer row) {
        for (int offset = 0; offset < row.completed.size; ) {
            long header = readHeader(row.completed, offset);
            offset = (int) (header >>> 32);
            int count = (int) ((header & 0xffff_ffffL) >>> 1);
            addRun(row.completed.data[offset++], count);
        }
        addRun(row.lastLevel, row.runLength);
    }

    long rawSize() {
        return completed.size + (runLength == 0 ? 0L : runSize(runLength));
    }

    long projectedRawSize(WriterLevelBuffer row) {
        long size = completed.size;
        int last = lastLevel;
        long run = runLength;
        for (int offset = 0; offset < row.completed.size; ) {
            long header = readHeader(row.completed, offset);
            offset = (int) (header >>> 32);
            int count = (int) ((header & 0xffff_ffffL) >>> 1);
            int level = row.completed.data[offset++];
            if (level == last) run += count;
            else {
                if (run != 0) size += runSize(run);
                last = level;
                run = count;
            }
        }
        if (row.runLength != 0) {
            if (row.lastLevel == last) run += row.runLength;
            else {
                if (run != 0) size += runSize(run);
                run = row.runLength;
            }
        }
        return size + (run == 0 ? 0 : runSize(run));
    }

    byte[] raw() {
        WriterByteBuffer encoded = new WriterByteBuffer();
        encoded.reserve(Math.toIntExact(rawSize()));
        encoded.putBytes(completed.data, 0, completed.size);
        if (runLength != 0) {
            encoded.putUnsignedVarint((long) runLength << 1);
            encoded.putByte(lastLevel);
        }
        return encoded.bytes();
    }

    // The upper word carries the next byte offset; the lower word carries the unsigned run header.
    private static long readHeader(WriterByteBuffer bytes, int start) {
        long value = 0;
        int shift = 0;
        int offset = start;
        int next;
        do {
            next = bytes.data[offset++] & 0xff;
            value |= (long) (next & 0x7f) << shift;
            shift += 7;
        } while ((next & 0x80) != 0);
        return ((long) offset << 32) | value;
    }

    private static int runSize(long length) {
        long header = length << 1;
        int size = 2;
        while ((header >>>= 7) != 0) size++;
        return size;
    }
}
