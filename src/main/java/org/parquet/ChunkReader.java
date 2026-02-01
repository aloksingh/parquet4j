package org.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for reading chunks of Parquet data.
 * Allows multiple decoders to read concurrently from different locations
 * in the same file.
 */
public interface ChunkReader {
  /**
   * Returns the total length of the data source in bytes
   */
  long length() throws IOException;

  /**
   * Read bytes from the specified position
   *
   * @param position Starting position to read from
   * @param length   Number of bytes to read
   * @return ByteBuffer containing the read data
   */
  ByteBuffer readBytes(long position, int length) throws IOException;
}
