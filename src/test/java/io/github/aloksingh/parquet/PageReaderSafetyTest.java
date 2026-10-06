package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetMetadata.ColumnChunkMetadata;
import io.github.aloksingh.parquet.model.Type;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Statistics;
import org.junit.jupiter.api.Test;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

class PageReaderSafetyTest {
    private static final ColumnDescriptor REQUIRED =
            new ColumnDescriptor(Type.INT32, new String[]{"value"}, 0, 0, 0);

    @Test
    void streamsAHeaderLargerThan1049BytesAndStopsAtChunkBoundary() throws Exception {
        byte[] body = new byte[]{42, 0, 0, 0};
        PageHeader header = v1Header(body.length, body.length, 1);
        header.getData_page_header().setStatistics(new Statistics().setMax(new byte[1049]));
        byte[] bytes = pageBytes(header, body);
        assertTrue(bytes.length - body.length > 1049);
        ArrayChunk chunk = new ArrayChunk(bytes, Integer.MAX_VALUE);
        PageReader reader = new PageReader(chunk, metadata(bytes.length, CompressionCodec.UNCOMPRESSED), REQUIRED);
        Page.DataPage page = assertInstanceOf(Page.DataPage.class, reader.readNextPage());
        assertArrayEquals(body, bytes(page.data()));
        assertEquals(1, page.numValues());
        assertNull(reader.readNextPage());
        for (long[] request : chunk.requests) {
            assertTrue(request[0] + request[1] <= bytes.length, "read stays in column chunk");
        }
    }

    @Test
    void completesShortHeaderAndBodyReads() throws Exception {
        byte[] body = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
        byte[] bytes = pageBytes(v1Header(body.length, body.length, 2), body);
        PageReader reader = new PageReader(new ArrayChunk(bytes, 1),
                metadata(bytes.length, CompressionCodec.UNCOMPRESSED), REQUIRED);
        assertArrayEquals(body, bytes(assertInstanceOf(Page.DataPage.class, reader.readNextPage()).data()));
        assertNull(reader.readNextPage());
    }

    @Test
    void reportsEofInsteadOfAcceptingAnIncompletePageBody() throws Exception {
        byte[] body = new byte[]{1, 2, 3, 4};
        PageHeader header = v1Header(body.length, body.length, 1);
        byte[] bytes = pageBytes(header, body);
        int bodyStart = pageBytes(header, new byte[0]).length;
        ArrayChunk chunk = new ArrayChunk(bytes, 1) {
            @Override
            public ByteBuffer readBytes(long position, int length) throws IOException {
                if (position >= bodyStart + 2) return ByteBuffer.allocate(0);
                return super.readBytes(position, length);
            }
        };
        PageReader reader = new PageReader(chunk, metadata(bytes.length, CompressionCodec.UNCOMPRESSED), REQUIRED);
        assertThrows(java.io.EOFException.class, reader::readNextPage);
    }

    @Test
    void enforcesTheConfiguredHeaderByteLimitIncludingThriftBinaryLengths() throws Exception {
        byte[] body = {42, 0, 0, 0};
        PageHeader header = v1Header(body.length, body.length, 1);
        header.getData_page_header().setStatistics(new Statistics().setMax(new byte[1049]));
        byte[] bytes = pageBytes(header, body);
        ArrayChunk chunk = new ArrayChunk(bytes, Integer.MAX_VALUE);
        PageReader reader = new PageReader(chunk, metadata(bytes.length, CompressionCodec.UNCOMPRESSED),
                REQUIRED, new PageReadOptions(64, 1024, 1024, 100, false));
        assertThrows(io.github.aloksingh.parquet.model.ParquetException.class, reader::readNextPage);
        for (long[] request : chunk.requests) assertTrue(request[0] + request[1] <= 64);
    }

    static PageHeader v1Header(int compressedSize, int uncompressedSize, int numValues) {
        return new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize)
                .setData_page_header(new DataPageHeader(numValues, Encoding.PLAIN, Encoding.RLE, Encoding.RLE));
    }

    static byte[] pageBytes(PageHeader header, byte[] body) throws TException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        header.write(new TCompactProtocol(new TIOStreamTransport(stream)));
        stream.writeBytes(body);
        return stream.toByteArray();
    }

    static ColumnChunkMetadata metadata(long chunkSize, CompressionCodec codec) {
        return new ColumnChunkMetadata(Type.INT32, new String[]{"value"}, codec,
                0, 0, chunkSize, chunkSize, 1, null);
    }

    static byte[] bytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.duplicate().get(result);
        return result;
    }

    static class ArrayChunk implements ChunkReader {
        final byte[] data;
        final int maxRead;
        final List<long[]> requests = new java.util.ArrayList<>();

        ArrayChunk(byte[] data, int maxRead) {
            this.data = data;
            this.maxRead = maxRead;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public ByteBuffer readBytes(long position, int length) throws IOException {
            if (position < 0 || length < 0 || position > data.length) throw new IOException("Invalid range");
            requests.add(new long[]{position, length});
            int count = Math.min(Math.min(length, maxRead), data.length - (int) position);
            return ByteBuffer.wrap(Arrays.copyOfRange(data, (int) position, (int) position + count));
        }
    }
}
