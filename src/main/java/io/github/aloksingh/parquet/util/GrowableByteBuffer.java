package io.github.aloksingh.parquet.util;

import java.util.ArrayList;
import java.util.List;

/**
 * A ByteBuffer like class, that is backed by one or more byte arrays. To increase the capacity a new
 * byte array added to the pool of byte arrays backing this buffer. The new byte array will be of size capacityIncrement.
 * New arrays will be added until sufficient capacity exists to match the ensureCapacity() request
 *
 */
public class GrowableByteBuffer implements AutoCloseable{
  private final List<byte[]> arrays;
  private final int capacityIncrement;
  private int position;
  private int totalCapacity;

  // Which backing array we are currently writing to
  private int currentArrayIndex;
  // Offset within the current backing array where next write goes
  private int currentArrayOffset;

  public GrowableByteBuffer(int initialCapacity) {
    this(initialCapacity, initialCapacity);
  }

  public GrowableByteBuffer(int initialCapacity, int capacityIncrement) {
    if (initialCapacity <= 0 || capacityIncrement <= 0) {
      throw new IllegalArgumentException("initialCapacity and capacityIncrement must be positive");
    }
    this.capacityIncrement = capacityIncrement;
    this.arrays = new ArrayList<>();
    this.arrays.add(new byte[initialCapacity]);
    this.totalCapacity = initialCapacity;
    this.position = 0;
    this.currentArrayIndex = 0;
    this.currentArrayOffset = 0;
  }

  public void put(byte b) {
    ensureCapacity(1);
    byte[] current = arrays.get(currentArrayIndex);
    if (currentArrayOffset == current.length) {
      currentArrayIndex++;
      currentArrayOffset = 0;
      current = arrays.get(currentArrayIndex);
    }
    current[currentArrayOffset++] = b;
    position++;
  }

  public void put(byte[] b) {
    put(b, 0, b.length);
  }

  public void put(byte[] b, int off, int len) {
    ensureCapacity(len);
    int remaining = len;
    int srcOff = off;
    while (remaining > 0) {
      byte[] current = arrays.get(currentArrayIndex);
      if (currentArrayOffset == current.length) {
        currentArrayIndex++;
        currentArrayOffset = 0;
        current = arrays.get(currentArrayIndex);
      }
      int space = current.length - currentArrayOffset;
      int toCopy = Math.min(remaining, space);
      System.arraycopy(b, srcOff, current, currentArrayOffset, toCopy);
      currentArrayOffset += toCopy;
      position += toCopy;
      srcOff += toCopy;
      remaining -= toCopy;
    }
  }

  public void ensureCapacity(int needed) {
    int available = totalCapacity - position;
    while (available < needed) {
      byte[] newArray = new byte[capacityIncrement];
      arrays.add(newArray);
      totalCapacity += capacityIncrement;
      available += capacityIncrement;
    }
  }

  public int position() {
    return position;
  }

  public byte[] array() {
    return getArray(0, position);
  }

  public byte[] getArray(int off, int len) {
    byte[] result = new byte[len];
    int destOff = 0;
    int remaining = len;
    int skipBytes = off;

    for (byte[] array : arrays) {
      if (skipBytes >= array.length) {
        skipBytes -= array.length;
        continue;
      }
      int srcOff = skipBytes;
      skipBytes = 0;
      int available = array.length - srcOff;
      int toCopy = Math.min(remaining, available);
      System.arraycopy(array, srcOff, result, destOff, toCopy);
      destOff += toCopy;
      remaining -= toCopy;
      if (remaining == 0) {
        break;
      }
    }
    return result;
  }

  public void clear() {
    this.position = 0;
    this.currentArrayIndex = 0;
    this.currentArrayOffset = 0;
  }

  @Override
  public void close() throws Exception {
    this.arrays.clear();
  }

}
