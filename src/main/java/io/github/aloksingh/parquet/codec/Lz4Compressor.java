package io.github.aloksingh.parquet.codec;

import io.github.aloksingh.parquet.Compressor;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import net.jpountz.lz4.LZ4Factory;

/**
 * The deprecated Parquet LZ4 codec, using Apache Hadoop's block stream framing.
 * Each outer block has a big-endian uncompressed byte count, followed by one or
 * more big-endian compressed chunk lengths and their raw LZ4 blocks. New files
 * should prefer {@code CompressionCodec.LZ4_RAW} for interoperable unframed LZ4.
 *
 * @see <a href="https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/BlockCompressorStream.java">Hadoop BlockCompressorStream</a>
 */
public class Lz4Compressor implements Compressor {
  private static final int CHUNK_SIZE = 64 * 1024;
  private static final net.jpountz.lz4.LZ4Compressor COMPRESSOR = LZ4Factory.fastestInstance().fastCompressor();

  /** Constructs a legacy Hadoop-framed LZ4 compressor. */
  public Lz4Compressor() {
  }

  @Override
  public byte[] compress(byte[] uncompressed) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    output.writeInt(uncompressed.length);
    if (uncompressed.length != 0) {
      byte[] compressed = new byte[COMPRESSOR.maxCompressedLength(CHUNK_SIZE)];
      for (int offset = 0; offset < uncompressed.length;) {
        int length = Math.min(CHUNK_SIZE, uncompressed.length - offset);
        int compressedLength = COMPRESSOR.compress(uncompressed, offset, length,
            compressed, 0, compressed.length);
        output.writeInt(compressedLength);
        output.write(compressed, 0, compressedLength);
        offset += length;
      }
    }
    return bytes.toByteArray();
  }
}
