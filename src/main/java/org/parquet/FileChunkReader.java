package org.parquet;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * ChunkReader implementation for reading from local files
 */
public class FileChunkReader implements ChunkReader, AutoCloseable {
  private final RandomAccessFile file;
  private final long length;

  public FileChunkReader(Path path) throws IOException {
    this.file = new RandomAccessFile(path.toFile(), "r");
    this.length = file.length();
  }

  public FileChunkReader(String path) throws IOException {
    this(Path.of(path));
  }

  @Override
  public long length() {
    return length;
  }

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

  @Override
  public void close() throws IOException {
    file.close();
  }
}
