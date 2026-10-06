package io.github.aloksingh.parquet;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A buffered, bounded positional stream that tracks bytes actually consumed by Thrift.
 */
final class PageHeaderInputStream extends InputStream {
    private static final int BUFFER_SIZE = 4096;
    private final ChunkReader reader;
    private final long start;
    private final int limit;
    private ByteBuffer buffered = ByteBuffer.allocate(0);
    private int fetched;
    private int consumed;

    PageHeaderInputStream(ChunkReader reader, long start, int limit) {
        this.reader = reader;
        this.start = start;
        this.limit = limit;
    }

    int bytesConsumed() {
        return consumed;
    }

    private void refill() throws IOException {
        if (buffered.hasRemaining()) return;
        if (fetched == limit) {
            throw new EOFException("Page header exceeds its byte limit or column chunk boundary (" + limit + " bytes)");
        }
        int requested = Math.min(BUFFER_SIZE, limit - fetched);
        buffered = reader.readBytes(start + fetched, requested).duplicate();
        int count = buffered.remaining();
        if (count == 0) {
            throw new EOFException("Unexpected EOF while reading page header at " + (start + fetched));
        }
        if (count > requested) {
            throw new IOException("ChunkReader returned more bytes than requested");
        }
        fetched += count;
    }

    @Override
    public int read() throws IOException {
        refill();
        consumed++;
        return buffered.get() & 0xff;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, bytes.length);
        if (length == 0) return 0;
        refill();
        int count = Math.min(length, buffered.remaining());
        buffered.get(bytes, offset, count);
        consumed += count;
        return count;
    }
}
