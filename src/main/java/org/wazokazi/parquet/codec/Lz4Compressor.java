package org.wazokazi.parquet.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.wazokazi.parquet.Compressor;

/**
 * LZ4 compressor implementation using the Hadoop LZ4 block format.
 *
 * <p>This compressor uses the LZ4 compression algorithm with the Hadoop-specific
 * format that includes a 4-byte little-endian length prefix before each compressed block.
 * This format is compatible with Parquet files that use LZ4 compression.
 *
 * <p>The implementation uses the fastest available LZ4 compressor instance from
 * the LZ4Factory for optimal performance.
 *
 * @see Compressor
 */
public class Lz4Compressor implements Compressor {

  /**
   * Constructs a new LZ4 compressor.
   */
  public Lz4Compressor() {
  }

  /** Factory instance for creating LZ4 compressors */
  private static final LZ4Factory factory = LZ4Factory.fastestInstance();

  /** Fast LZ4 compressor instance for compression operations */
  private static final LZ4Compressor compressor = factory.fastCompressor();

  /**
   * Compresses the input data using LZ4 compression with Hadoop block format.
   *
   * <p>The output format consists of:
   * <ul>
   *   <li>4-byte little-endian integer: length of compressed data</li>
   *   <li>Compressed data bytes</li>
   * </ul>
   *
   * @param uncompressed the uncompressed input data to compress
   * @return byte array containing the 4-byte length prefix followed by compressed data
   * @throws IOException if an I/O error occurs during compression
   */
  @Override
  public byte[] compress(byte[] uncompressed) throws IOException {
    // Compress the data
    int maxCompressedLength = compressor.maxCompressedLength(uncompressed.length);
    byte[] compressed = new byte[maxCompressedLength];
    int compressedLength = compressor.compress(uncompressed, 0, uncompressed.length,
        compressed, 0, maxCompressedLength);

    // Write in Hadoop LZ4 format: 4-byte length prefix + compressed data
    ByteArrayOutputStream output = new ByteArrayOutputStream(4 + compressedLength);

    // Write compressed block size (little-endian)
    output.write(compressedLength & 0xFF);
    output.write((compressedLength >> 8) & 0xFF);
    output.write((compressedLength >> 16) & 0xFF);
    output.write((compressedLength >> 24) & 0xFF);

    // Write compressed data
    output.write(compressed, 0, compressedLength);

    return output.toByteArray();
  }
}
