package io.github.aloksingh.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for reading chunks of Parquet data.
 * <p>
 * This interface provides random access to data within a Parquet file,
 * allowing multiple decoders to read concurrently from different locations
 * in the same file without interfering with each other.
 * </p>
 * <p>
 * Implementations must be thread-safe to support concurrent access patterns
 * common in Parquet column reading.
 * </p>
 */
public interface ChunkReader {
  /**
   * Returns the total length of the data source in bytes.
   *
   * @return the total size of the data source in bytes
   * @throws IOException if an I/O error occurs while determining the length
   */
  long length() throws IOException;

  /**
   * Reads bytes from the specified position in the data source.
   * <p>
   * This method performs a random access read, allowing callers to read
   * from any position within the data source without affecting the position
   * of other concurrent reads.
   * </p>
   *
   * @param position the starting position to read from (0-based offset)
   * @param length the number of bytes to read
   * @return a {@link ByteBuffer} containing the read data, positioned at 0
   *         with limit set to the number of bytes read
   * @throws IOException if an I/O error occurs during reading
   * @throws IllegalArgumentException if position is negative or length is invalid
   */
  ByteBuffer readBytes(long position, int length) throws IOException;

  /**
   * Fills the destination's remaining range with an exact positional read.
   * Short reads are retried, and EOF (including a zero-progress read) is an error.
   * The destination's position advances; its limit is unchanged.
   *
   * @param position starting byte offset
   * @param destination buffer range to fill
   * @throws IOException if reading fails or the source ends before the range is filled
   * @throws IllegalArgumentException if the range is negative or overflows
   */
  default void readInto(long position, ByteBuffer destination) throws IOException {
    if (position < 0 || position > Long.MAX_VALUE - destination.remaining()) {
      throw new IllegalArgumentException("Invalid byte range at " + position);
    }
    long offset = position;
    while (destination.hasRemaining()) {
      int requested = destination.remaining();
      ByteBuffer bytes = readBytes(offset, requested).duplicate();
      int count = bytes.remaining();
      if (count == 0) {
        throw new java.io.EOFException("Unexpected EOF at " + offset + "; needed " + requested + " more bytes");
      }
      if (count > requested) {
        throw new IOException("ChunkReader returned more bytes than requested");
      }
      destination.put(bytes);
      offset += count;
    }
  }
}
