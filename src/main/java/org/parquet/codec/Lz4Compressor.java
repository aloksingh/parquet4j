package org.parquet.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.parquet.Compressor;

/**
 * LZ4 compressor using Hadoop LZ4 format (with 4-byte length prefixes)
 */
public class Lz4Compressor implements Compressor {
  private static final LZ4Factory factory = LZ4Factory.fastestInstance();
  private static final LZ4Compressor compressor = factory.fastCompressor();

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
