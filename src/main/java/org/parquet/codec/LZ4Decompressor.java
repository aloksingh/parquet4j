package org.parquet.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.parquet.Decompressor;

/**
 * LZ4 decompressor supporting both Hadoop LZ4 and raw LZ4 formats.
 * <p>
 * Parquet supports two LZ4 variants:
 * 1. Hadoop LZ4 (CompressionCodecName.LZ4): Uses 4-byte length prefixes per block
 * 2. Raw LZ4: Direct LZ4 block compression
 * <p>
 * This implementation handles both formats by detecting the presence of length prefixes.
 */
public class LZ4Decompressor implements Decompressor {

  /**
   * Constructs a new LZ4 decompressor that supports both Hadoop LZ4 and raw LZ4 formats.
   */
  public LZ4Decompressor() {
  }
  private static final LZ4Factory factory = LZ4Factory.fastestInstance();
  private static final LZ4SafeDecompressor decompressor = factory.safeDecompressor();

  @Override
  public ByteBuffer decompress(ByteBuffer compressed, int uncompressedSize) throws IOException {
    byte[] compressedBytes = new byte[compressed.remaining()];
    compressed.get(compressedBytes);

    // Detect format by checking if the first 4 bytes look like a reasonable block size
    // Hadoop LZ4 has 4-byte length prefix (little-endian) for each block
    // Raw LZ4 starts directly with compressed data
    if (compressedBytes.length >= 4) {
      int potentialBlockSize = ((compressedBytes[0] & 0xFF)) |
          ((compressedBytes[1] & 0xFF) << 8) |
          ((compressedBytes[2] & 0xFF) << 16) |
          ((compressedBytes[3] & 0xFF) << 24);

      // Heuristic: if the potential block size is reasonable (> 0 and <= compressed data size),
      // try Hadoop LZ4 first. Otherwise, try raw LZ4.
      boolean likelyHadoopLZ4 = potentialBlockSize > 0 &&
          potentialBlockSize < compressedBytes.length &&
          potentialBlockSize < uncompressedSize * 2; // compressed shouldn't be 2x larger

      if (likelyHadoopLZ4) {
        try {
          return decompressHadoopLZ4(compressedBytes, uncompressedSize);
        } catch (Exception e) {
          // Fallback to raw LZ4
          try {
            return decompressRawLZ4(compressedBytes, uncompressedSize);
          } catch (Exception e2) {
            throw new IOException(
                "Failed to decompress LZ4 data (tried Hadoop): " + e.getMessage() +
                    " (also tried raw LZ4: " + e2.getMessage() + ")");
          }
        }
      } else {
        // Try raw LZ4 first
        try {
          return decompressRawLZ4(compressedBytes, uncompressedSize);
        } catch (Exception e) {
          // Fallback to Hadoop LZ4
          try {
            return decompressHadoopLZ4(compressedBytes, uncompressedSize);
          } catch (Exception e2) {
            throw new IOException("Failed to decompress LZ4 data (tried raw): " + e.getMessage() +
                " (also tried Hadoop LZ4: " + e2.getMessage() + ")");
          }
        }
      }
    } else {
      // Too small for Hadoop LZ4, must be raw
      return decompressRawLZ4(compressedBytes, uncompressedSize);
    }
  }

  /**
   * Decompress Hadoop LZ4 format (with 4-byte length prefixes per block)
   */
  private ByteBuffer decompressHadoopLZ4(byte[] compressedBytes, int uncompressedSize)
      throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream(uncompressedSize);
    int offset = 0;

    while (offset < compressedBytes.length) {
      // Read 4-byte block size (little-endian)
      if (offset + 4 > compressedBytes.length) {
        break;
      }

      int blockSize = ((compressedBytes[offset] & 0xFF)) |
          ((compressedBytes[offset + 1] & 0xFF) << 8) |
          ((compressedBytes[offset + 2] & 0xFF) << 16) |
          ((compressedBytes[offset + 3] & 0xFF) << 24);

      offset += 4;

      // Validate block size
      if (blockSize <= 0 || blockSize > compressedBytes.length - offset) {
        throw new IOException("Invalid Hadoop LZ4 block size: " + blockSize);
      }

      // Decompress this block
      // For LZ4, we need to estimate the decompressed size for this block
      // Use remaining uncompressed size as upper bound
      int maxDecompressedSize = uncompressedSize - output.size();
      byte[] blockCompressed = new byte[blockSize];
      System.arraycopy(compressedBytes, offset, blockCompressed, 0, blockSize);

      byte[] blockDecompressed = new byte[maxDecompressedSize];
      // LZ4SafeDecompressor.decompress() returns the number of decompressed bytes written
      int actualDecompressed = decompressor.decompress(blockCompressed, 0, blockSize,
          blockDecompressed, 0, maxDecompressedSize);

      output.write(blockDecompressed, 0, actualDecompressed);
      offset += blockSize;
    }

    byte[] result = output.toByteArray();
    if (result.length != uncompressedSize) {
      throw new IOException(String.format(
          "Hadoop LZ4 decompressed size mismatch: expected %d, got %d",
          uncompressedSize, result.length));
    }

    return ByteBuffer.wrap(result);
  }

  /**
   * Decompress raw LZ4 format (single compressed block)
   */
  private ByteBuffer decompressRawLZ4(byte[] compressedBytes, int uncompressedSize)
      throws IOException {
    byte[] uncompressed = new byte[uncompressedSize];

    // LZ4SafeDecompressor.decompress() returns the number of decompressed bytes written
    int actualSize = decompressor.decompress(compressedBytes, 0, compressedBytes.length,
        uncompressed, 0, uncompressedSize);

    if (actualSize != uncompressedSize) {
      throw new IOException(String.format(
          "Raw LZ4 decompressed size mismatch: expected %d, got %d",
          uncompressedSize, actualSize));
    }

    return ByteBuffer.wrap(uncompressed);
  }
}
