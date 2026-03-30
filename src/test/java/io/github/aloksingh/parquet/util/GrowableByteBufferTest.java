package io.github.aloksingh.parquet.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class GrowableByteBufferTest {

  @Test
  void putSingleBytesAndReadBack() {
    GrowableByteBuffer buf = new GrowableByteBuffer(4, 4);
    buf.put((byte) 1);
    buf.put((byte) 2);
    buf.put((byte) 3);

    assertEquals(3, buf.position());
    assertArrayEquals(new byte[] {1, 2, 3}, buf.getArray(0, 3));
  }

  @Test
  void putByteArrayAndReadBack() {
    GrowableByteBuffer buf = new GrowableByteBuffer(8, 8);
    byte[] data = {10, 20, 30, 40, 50};
    buf.put(data, 0, data.length);

    assertEquals(5, buf.position());
    assertArrayEquals(data, buf.getArray(0, 5));
  }

  @Test
  void putByteArrayWithOffset() {
    GrowableByteBuffer buf = new GrowableByteBuffer(8, 8);
    byte[] data = {10, 20, 30, 40, 50};
    buf.put(data, 1, 3); // puts 20, 30, 40

    assertEquals(3, buf.position());
    assertArrayEquals(new byte[] {20, 30, 40}, buf.getArray(0, 3));
  }

  @Test
  void growsAcrossMultipleArrays() {
    // Start with capacity 4, increment by 4
    GrowableByteBuffer buf = new GrowableByteBuffer(4, 4);
    byte[] data = new byte[10];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i + 1);
    }
    buf.ensureCapacity(10);
    buf.put(data, 0, data.length);

    assertEquals(10, buf.position());
    assertArrayEquals(data, buf.getArray(0, 10));
  }

  @Test
  void singleByteWritesAcrossBoundary() {
    GrowableByteBuffer buf = new GrowableByteBuffer(2, 2);
    // Write 5 bytes one at a time, crossing array boundaries
    for (int i = 0; i < 5; i++) {
      buf.ensureCapacity(1);
      buf.put((byte) (i + 1));
    }

    assertEquals(5, buf.position());
    assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, buf.getArray(0, 5));
  }

  @Test
  void getArrayWithOffset() {
    GrowableByteBuffer buf = new GrowableByteBuffer(8, 8);
    byte[] data = {10, 20, 30, 40, 50};
    buf.put(data, 0, data.length);

    // Read a sub-range from the middle
    assertArrayEquals(new byte[] {20, 30, 40}, buf.getArray(1, 3));
  }

  @Test
  void getArrayWithOffsetAcrossArrayBoundaries() {
    GrowableByteBuffer buf = new GrowableByteBuffer(3, 3);
    byte[] data = new byte[9];
    for (int i = 0; i < 9; i++) {
      data[i] = (byte) (i + 1);
    }
    buf.ensureCapacity(9);
    buf.put(data, 0, 9);

    // Read across the boundary of first and second backing array
    assertArrayEquals(new byte[] {2, 3, 4, 5}, buf.getArray(1, 4));
    // Read spanning all three backing arrays
    assertArrayEquals(new byte[] {3, 4, 5, 6, 7}, buf.getArray(2, 5));
  }

  @Test
  void singleArgConstructorWorks() {
    GrowableByteBuffer buf = new GrowableByteBuffer(16);
    buf.put((byte) 42);
    assertEquals(1, buf.position());
    assertArrayEquals(new byte[] {42}, buf.getArray(0, 1));
  }

  @Test
  void ensureCapacityMultipleIncrements() {
    GrowableByteBuffer buf = new GrowableByteBuffer(2, 3);
    // Need 10 bytes total: initial 2 + ceil((10-2)/3)*3 = 2 + 9 = 11 capacity
    buf.ensureCapacity(10);
    byte[] data = new byte[10];
    for (int i = 0; i < 10; i++) {
      data[i] = (byte) i;
    }
    buf.put(data, 0, 10);

    assertEquals(10, buf.position());
    assertArrayEquals(data, buf.getArray(0, 10));
  }

  @Test
  void emptyBufferPositionIsZero() {
    GrowableByteBuffer buf = new GrowableByteBuffer(8, 8);
    assertEquals(0, buf.position());
  }

  @Test
  void getArrayEmptyRange() {
    GrowableByteBuffer buf = new GrowableByteBuffer(8, 8);
    buf.put((byte) 1);
    assertArrayEquals(new byte[0], buf.getArray(0, 0));
  }

  @Test
  void invalidConstructorArguments() {
    assertThrows(IllegalArgumentException.class, () -> new GrowableByteBuffer(0, 4));
    assertThrows(IllegalArgumentException.class, () -> new GrowableByteBuffer(4, 0));
    assertThrows(IllegalArgumentException.class, () -> new GrowableByteBuffer(-1, 4));
  }

  @Test
  void largeWriteSpanningManyArrays() {
    GrowableByteBuffer buf = new GrowableByteBuffer(4, 4);
    byte[] data = new byte[100];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 127);
    }
    buf.ensureCapacity(100);
    buf.put(data, 0, data.length);

    assertEquals(100, buf.position());
    assertArrayEquals(data, buf.getArray(0, 100));
  }

  @Test
  void clearResetsPositionAndAllowsRewrite() {
    GrowableByteBuffer buf = new GrowableByteBuffer(4, 4);
    buf.ensureCapacity(6);
    buf.put(new byte[] {1, 2, 3, 4, 5, 6}, 0, 6);
    assertEquals(6, buf.position());

    buf.clear();
    assertEquals(0, buf.position());

    // Write new data after clear
    buf.put(new byte[] {10, 20, 30}, 0, 3);
    assertEquals(3, buf.position());
    assertArrayEquals(new byte[] {10, 20, 30}, buf.getArray(0, 3));
  }

  @Test
  void clearRetainsBackingArrays() {
    GrowableByteBuffer buf = new GrowableByteBuffer(4, 4);
    buf.ensureCapacity(10);
    buf.put(new byte[10], 0, 10);

    buf.clear();

    // Can write up to previous capacity without calling ensureCapacity
    byte[] data = new byte[10];
    for (int i = 0; i < 10; i++) {
      data[i] = (byte) (i + 1);
    }
    buf.put(data, 0, 10);
    assertEquals(10, buf.position());
    assertArrayEquals(data, buf.getArray(0, 10));
  }

  @Test
  void mixedSingleAndBulkWrites() {
    GrowableByteBuffer buf = new GrowableByteBuffer(3, 3);

    buf.put((byte) 1);
    buf.ensureCapacity(4);
    buf.put(new byte[] {2, 3, 4, 5}, 0, 4);
    buf.ensureCapacity(1);
    buf.put((byte) 6);

    assertEquals(6, buf.position());
    assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 6}, buf.getArray(0, 6));
  }
}
