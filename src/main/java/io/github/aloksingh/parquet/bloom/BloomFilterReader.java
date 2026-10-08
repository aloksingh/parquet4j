package io.github.aloksingh.parquet.bloom;

import io.github.aloksingh.parquet.ChunkReader;
import io.github.aloksingh.parquet.model.BloomFilterMetadata;
import io.github.aloksingh.parquet.model.ParquetException;
import org.apache.parquet.format.BloomFilterHeader;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reads Parquet bloom filters from file offsets.
 *
 * <p>A bloom filter on disk consists of a Thrift {@link BloomFilterHeader} followed
 * by the bitset bytes. When {@code bloomFilterLength} is known (from ColumnMetaData),
 * both header and bitset can be read in a single I/O. When only the offset is known,
 * the header must be read first to determine the bitset size, then the bitset is read.
 */
public final class BloomFilterReader {

    private BloomFilterReader() {
    }

    /**
     * Read a bloom filter given offset and (optionally) total length.
     *
     * @param reader      the chunk reader positioned on the file
     * @param offset      absolute byte offset to the bloom filter header
     * @param totalLength total length including header and bitset, or -1 if unknown
     * @return the parsed bloom filter, or null if the algorithm/hash/compression is unsupported
     * @throws IOException if reading fails
     */
    public static SplitBlockBloomFilter readBloomFilter(ChunkReader reader, long offset,
                                                        int totalLength) throws IOException {
        if (totalLength > 0) {
            // Read header + bitset in one shot
            ByteBuffer buf = reader.readBytes(offset, totalLength);
            byte[] raw = new byte[buf.remaining()];
            buf.get(raw);
            return parseFromBytes(raw);
        }

        // Read just the header first to determine bitset size
        // BloomFilterHeader has 4 required fields: numBytes (i32), algorithm (struct),
        // hash (struct), compression (struct). With TCompactProtocol, min size is ~10 bytes.
        // Read a generous chunk (256 bytes) to cover the header.
        ByteBuffer headerBuf = reader.readBytes(offset, 256);
        byte[] headerBytes = new byte[headerBuf.remaining()];
        headerBuf.get(headerBytes);
        BloomFilterHeader header = readHeader(headerBytes);
        int headerSize = consumedHeaderBytes(headerBytes);
        long bitsetOffset = offset + headerSize;
        int numBytes = header.getNumBytes();

        ByteBuffer bitsetBuf = reader.readBytes(bitsetOffset, numBytes);
        byte[] bitset = new byte[bitsetBuf.remaining()];
        bitsetBuf.get(bitset);
        return buildFilter(header, bitset);
    }

    /**
     * Parse a bloom filter from a byte array containing [header][bitset].
     */
    public static SplitBlockBloomFilter parseFromBytes(byte[] data) throws IOException {
        BloomFilterHeader header = readHeader(data);
        int headerSize = consumedHeaderBytes(data);
        int numBitsetBytes = header.getNumBytes();
        byte[] bitset = new byte[numBitsetBytes];
        System.arraycopy(data, headerSize, bitset, 0, Math.min(numBitsetBytes, data.length - headerSize));
        return buildFilter(header, bitset);
    }

    /**
     * Parse only the BloomFilterHeader from raw bytes; returns the header with its metadata
     * and the number of header bytes consumed. The bitset starts at (offset + headerLen).
     */
    public static ParsedHeader parseHeader(ChunkReader reader, long offset) throws IOException {
        ByteBuffer buf = reader.readBytes(offset, 256);
        byte[] raw = new byte[buf.remaining()];
        buf.get(raw);
        BloomFilterHeader header = readHeader(raw);
        int headerLen = consumedHeaderBytes(raw);
        BloomFilterMetadata meta = new BloomFilterMetadata(
                header.getNumBytes(),
                algorithmName(header),
                hashName(header),
                compressionName(header));
        return new ParsedHeader(meta, headerLen);
    }

    /**
     * Parsed bloom filter header with the byte count consumed by the Thrift struct.
     */
    public record ParsedHeader(BloomFilterMetadata metadata, int headerLength) {
    }

    // ------------------------------------------------------------------ internal

    static BloomFilterHeader readHeader(byte[] data) throws IOException {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            TCompactProtocol proto = new TCompactProtocol(new TIOStreamTransport(bais));
            BloomFilterHeader header = new BloomFilterHeader();
            header.read(proto);
            return header;
        } catch (TException e) {
            throw new ParquetException("Failed to deserialize BloomFilterHeader", e);
        }
    }

    /**
     * Count bytes consumed when deserializing the Thrift BloomFilterHeader.
     */
    static int consumedHeaderBytes(byte[] data) throws IOException {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            TCompactProtocol proto = new TCompactProtocol(new TIOStreamTransport(bais));
            BloomFilterHeader header = new BloomFilterHeader();
            header.read(proto);
            return data.length - bais.available();
        } catch (TException e) {
            throw new ParquetException("Failed to deserialize BloomFilterHeader", e);
        }
    }

    static SplitBlockBloomFilter buildFilter(BloomFilterHeader header, byte[] bitset) {
        String algo = algorithmName(header);
        String hash = hashName(header);
        String comp = compressionName(header);
        if (!"BLOCK".equals(algo) || !"XXHASH".equals(hash) || !"UNCOMPRESSED".equals(comp)) {
            return null; // unsupported combination
        }
        return new SplitBlockBloomFilter(bitset, header.getNumBytes());
    }

    static String algorithmName(BloomFilterHeader header) {
        if (header.getAlgorithm() != null && header.getAlgorithm().isSetBLOCK()) return "BLOCK";
        return "UNKNOWN";
    }

    static String hashName(BloomFilterHeader header) {
        if (header.getHash() != null && header.getHash().isSetXXHASH()) return "XXHASH";
        return "UNKNOWN";
    }

    static String compressionName(BloomFilterHeader header) {
        if (header.getCompression() != null && header.getCompression().isSetUNCOMPRESSED()) return "UNCOMPRESSED";
        return "UNKNOWN";
    }
}