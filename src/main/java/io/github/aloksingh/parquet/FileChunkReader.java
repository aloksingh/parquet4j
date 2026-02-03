package io.github.aloksingh.parquet;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * ChunkReader implementation for reading from local files.
 *
 * <p>This class provides thread-safe random access to files using
 * {@link RandomAccessFile}. It is designed for reading Parquet files
 * from the local filesystem.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (FileChunkReader reader = new FileChunkReader("data.parquet")) {
 *   ByteBuffer chunk = reader.readBytes(0, 1024);
 * }
 * }</pre>
 *
 * @see ChunkReader
 */
public class FileChunkReader implements ChunkReader, AutoCloseable {
  private final RandomAccessFile file;
  private final long length;

  /**
   * Creates a new FileChunkReader for the specified path.
   *
   * @param path the path to the file to read
   * @throws IOException if the file cannot be opened or read
   */
  public FileChunkReader(Path path) throws IOException {
    this.file = new RandomAccessFile(path.toFile(), "r");
    this.length = file.length();
  }

  /**
   * Creates a new FileChunkReader for the specified path string.
   *
   * @param path the path string to the file to read
   * @throws IOException if the file cannot be opened or read
   */
  public FileChunkReader(String path) throws IOException {
    this(Path.of(path));
  }

  /**
   * Returns the total length of the file in bytes.
   *
   * @return the file length in bytes
   */
  @Override
  public long length() {
    return length;
  }

  /**
   * Reads a chunk of bytes from the file at the specified position.
   *
   * <p>This method is thread-safe. If the requested length exceeds the
   * available bytes from the position to the end of file, it will read
   * only the available bytes.
   *
   * @param position the byte position to start reading from (0-based)
   * @param length the number of bytes to read
   * @return a ByteBuffer containing the read bytes
   * @throws IOException if position is negative, beyond file length,
   *                     or if an I/O error occurs during reading
   */
  @Override
  public ByteBuffer readBytes(long position, int length) throws IOException {
    if (position < 0) {
      throw new IOException(String.format(
          "Invalid position: %d", position));
    }

    if (position >= this.length) {
      throw new IOException(String.format(
          "Position %d is beyond file length %d", position, this.length));
    }

    // Clamp length to available bytes (for reading headers from small files)
    int availableBytes = (int) Math.min(length, this.length - position);
    byte[] buffer = new byte[availableBytes];

    synchronized (file) {
      file.seek(position);
      int bytesRead = file.read(buffer);
      if (bytesRead != availableBytes) {
        throw new IOException(String.format(
            "Expected to read %d bytes, but only read %d bytes",
            availableBytes, bytesRead));
      }
    }
    return ByteBuffer.wrap(buffer);
  }

  /**
   * Closes the underlying file handle.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    file.close();
  }
}
