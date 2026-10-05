package io.github.aloksingh.parquet;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Thread-safe local-file reads using {@link FileChannel} positional I/O. Concurrent
 * readers do not share or move a file cursor. The file length is captured on open.
 *
 * <p>{@link #readBytes(long, int)} reads completely up to the captured EOF, clamping
 * a request that extends beyond it. A file truncated after opening fails explicitly.
 */
public class FileChunkReader implements ChunkReader, AutoCloseable {
  private final Path path;
  private final FileChannel channel;
  private final long length;

  /**
   * Opens a file for reading.
   * @param path the path to read
   * @throws IOException if opening or determining the length fails
   */
  public FileChunkReader(Path path) throws IOException {
    FileChannel opened = FileChannel.open(path, StandardOpenOption.READ);
    try {
      this.length = opened.size();
      this.channel = opened;
      this.path = path;
    } catch (IOException | RuntimeException e) {
      try {
        opened.close();
      } catch (IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }

  /**
   * Opens a file for reading.
   * @param path the path string to read
   * @throws IOException if opening or determining the length fails
   */
  public FileChunkReader(String path) throws IOException {
    this(Path.of(path));
  }

  /** The path this reader was opened from; used for error context. */
  public Path getPath() {
    return path;
  }

  @Override
  public long length() throws IOException {
    ensureOpen();
    return length;
  }

  /**
   * Reads the requested range completely, clamping only at EOF.
   * @param position starting byte offset (an offset equal to EOF is allowed)
   * @param length requested byte count
   * @return a buffer positioned at zero with its limit equal to bytes read
   * @throws IllegalArgumentException if the range is negative or overflows
   * @throws IOException if closed, positioned beyond EOF, or truncated while reading
   */
  @Override
  public ByteBuffer readBytes(long position, int length) throws IOException {
    ensureOpen();
    if (position < 0 || length < 0 || position > Long.MAX_VALUE - length) {
      throw new IllegalArgumentException("Invalid byte range: position=" + position + ", length=" + length);
    }
    if (position > this.length) {
      throw new IOException("Position " + position + " is beyond file length " + this.length);
    }
    int availableBytes = (int) Math.min((long) length, this.length - position);
    ByteBuffer buffer = ByteBuffer.allocate(availableBytes);
    readInto(position, buffer);
    return buffer.flip();
  }

  /** Fills the destination by complete positional reads without an intermediate buffer. */
  @Override
  public void readInto(long position, ByteBuffer destination) throws IOException {
    ensureOpen();
    if (position < 0 || position > Long.MAX_VALUE - destination.remaining()) {
      throw new IllegalArgumentException("Invalid byte range at " + position);
    }
    if (position > length) {
      throw new IOException("Position " + position + " is beyond file length " + length);
    }
    long offset = position;
    while (destination.hasRemaining()) {
      int read = channel.read(destination, offset);
      if (read < 0) {
        throw new EOFException("Unexpected EOF at " + offset + "; needed " + destination.remaining() + " more bytes");
      }
      if (read == 0) {
        throw new IOException("File read made no progress at " + offset);
      }
      offset += read;
    }
  }

  private void ensureOpen() throws ClosedChannelException {
    if (!channel.isOpen()) {
      throw new ClosedChannelException();
    }
  }

  /** @throws IOException if closing the channel fails */
  @Override
  public void close() throws IOException {
    channel.close();
  }
}
