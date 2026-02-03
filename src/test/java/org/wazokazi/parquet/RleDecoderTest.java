package org.wazokazi.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;
import org.wazokazi.parquet.model.ParquetException;

/**
 * Tests for RLE/Bit-Packing Hybrid decoder
 */
public class RleDecoderTest {

  @Test
  void testRleRunSimple() {
    // RLE run: 5 values of 3
    // Header: (5 << 1) | 0 = 10
    // Value: 3 (bit width 2)
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 10);  // RLE header: 5 values
    buffer.put((byte) 3);   // Value: 3
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 2, 5);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {3, 3, 3, 3, 3}, result);
  }

  @Test
  void testRleRunZeroValue() {
    // RLE run: 4 values of 0
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 8);   // RLE header: 4 values
    buffer.put((byte) 0);   // Value: 0
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 1, 4);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {0, 0, 0, 0}, result);
  }

  @Test
  void testBitPackedRunSimple() {
    // Bit-packed run: 8 values with bit width 2
    // Header: (1 << 1) | 1 = 3 (1 group of 8 values)
    // Values: [0, 1, 2, 3, 0, 1, 2, 3]
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 3);   // Bit-packed header: 1 group
    // Pack 8 values with 2 bits each = 16 bits = 2 bytes
    // Values: 00 01 10 11 00 01 10 11
    // Byte 1: 11100100 = 0xE4
    // Byte 2: 11100100 = 0xE4
    buffer.put((byte) 0b11100100);
    buffer.put((byte) 0b11100100);
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 2, 8);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {0, 1, 2, 3, 0, 1, 2, 3}, result);
  }

  @Test
  void testBitPackedRunPartial() {
    // Bit-packed run with only 4 values needed from a group of 8
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 3);   // Bit-packed header: 1 group
    buffer.put((byte) 0b11100100);  // First 4 values: 0, 1, 2, 3
    buffer.put((byte) 0b11100100);
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 2, 4);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {0, 1, 2, 3}, result);
  }

  @Test
  void testMixedRleAndBitPacked() {
    // First: RLE run of 3 values of 1
    // Then: Bit-packed run with 8 values
    ByteBuffer buffer = ByteBuffer.allocate(20);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // RLE run: 3 values of 1
    buffer.put((byte) 6);   // (3 << 1) | 0 = 6
    buffer.put((byte) 1);   // Value: 1

    // Bit-packed run: 8 values [0, 1, 0, 1, 0, 1, 0, 1]
    // With bit width 1, we pack these as: LSB first per value
    // Bits: 0 1 0 1 0 1 0 1 = 10101010 = 0xAA
    buffer.put((byte) 3);   // (1 << 1) | 1 = 3
    buffer.put((byte) 0xAA);

    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 1, 11);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1}, result);
  }

  @Test
  void testBitWidthZero() {
    // Bit width 0 means all values are 0
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 10);  // RLE header: 5 values
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 0, 5);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {0, 0, 0, 0, 0}, result);
  }

  @Test
  void testLargerBitWidth() {
    // Test with bit width 4 (values 0-15)
    // RLE run: 4 values of 15
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 8);   // (4 << 1) | 0 = 8
    buffer.put((byte) 15);  // Value: 15
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 4, 4);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {15, 15, 15, 15}, result);
  }

  @Test
  void testReadNextIncremental() {
    // Test reading values one at a time
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 6);   // RLE: 3 values of 2
    buffer.put((byte) 2);
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 2, 3);

    assertEquals(2, decoder.readNext());
    assertEquals(2, decoder.readNext());
    assertEquals(2, decoder.readNext());
    assertEquals(-1, decoder.readNext());  // No more values
  }

  @Test
  void testUnderflow() {
    // Request more values than available
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 4);   // RLE: 2 values
    buffer.put((byte) 1);
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 1, 5);

    assertThrows(ParquetException.class, () -> decoder.readAll());
  }

  @Test
  void testEmptyBuffer() {
    ByteBuffer buffer = ByteBuffer.allocate(0);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 1, 5);

    assertThrows(ParquetException.class, () -> decoder.readAll());
  }

  @Test
  void testVarIntDecoding() {
    // Test with large run length requiring multi-byte varint
    // Run length 200 = 0xC8
    // VarInt: 0xC8 0x01 (binary: 11001000 00000001)
    ByteBuffer buffer = ByteBuffer.allocate(300);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    writeUnsignedVarInt(buffer, (200 << 1));  // RLE header
    buffer.put((byte) 5);  // Value: 5
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 3, 200);
    int[] result = decoder.readAll();

    assertEquals(200, result.length);
    for (int value : result) {
      assertEquals(5, value);
    }
  }

  @Test
  void testBitPackedWithWidth1() {
    // Test bit-packed with 1-bit width (binary values)
    // 8 values: [1, 0, 1, 1, 0, 0, 1, 0]
    // Bits: 01001101 = 0x4D
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 3);    // (1 << 1) | 1 = 3
    buffer.put((byte) 0b01001101);
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 1, 8);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {1, 0, 1, 1, 0, 0, 1, 0}, result);
  }

  @Test
  void testBitPackedWithWidth3() {
    // Test bit-packed with 3-bit width (values 0-7)
    // 8 values: [0, 1, 2, 3, 4, 5, 6, 7]
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 3);    // (1 << 1) | 1 = 3
    // Pack 8 values with 3 bits each = 24 bits = 3 bytes
    // Bit packing fills LSB first in each byte
    buffer.put((byte) 0x88);
    buffer.put((byte) 0xC6);
    buffer.put((byte) 0xFA);
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 3, 8);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5, 6, 7}, result);
  }

  @Test
  void testMultipleRleRuns() {
    // Multiple RLE runs back-to-back
    ByteBuffer buffer = ByteBuffer.allocate(20);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // First run: 3 values of 1
    buffer.put((byte) 6);
    buffer.put((byte) 1);

    // Second run: 2 values of 3
    buffer.put((byte) 4);
    buffer.put((byte) 3);

    // Third run: 4 values of 0
    buffer.put((byte) 8);
    buffer.put((byte) 0);

    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 2, 9);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {1, 1, 1, 3, 3, 0, 0, 0, 0}, result);
  }

  @Test
  void testDefinitionLevelsExample() {
    // Realistic example: definition levels for optional column
    // Pattern: 3 non-null (level 1), 2 null (level 0), 3 non-null (level 1)
    ByteBuffer buffer = ByteBuffer.allocate(20);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // RLE: 3 values of 1
    buffer.put((byte) 6);
    buffer.put((byte) 1);

    // RLE: 2 values of 0
    buffer.put((byte) 4);
    buffer.put((byte) 0);

    // RLE: 3 values of 1
    buffer.put((byte) 6);
    buffer.put((byte) 1);

    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 1, 8);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {1, 1, 1, 0, 0, 1, 1, 1}, result);
  }

  @Test
  void testVarIntTooLarge() {
    // Test varint overflow protection
    ByteBuffer buffer = ByteBuffer.allocate(20);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // Create a varint that's too large (more than 5 bytes with continuation bits)
    for (int i = 0; i < 6; i++) {
      buffer.put((byte) 0xFF);  // All continuation bits set
    }
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 1, 10);

    assertThrows(ParquetException.class, () -> decoder.readAll());
  }

  @Test
  void testSingleValue() {
    // Edge case: single value
    ByteBuffer buffer = ByteBuffer.allocate(10);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 2);   // RLE: 1 value
    buffer.put((byte) 7);
    buffer.flip();

    RleDecoder decoder = new RleDecoder(buffer, 3, 1);
    int[] result = decoder.readAll();

    assertArrayEquals(new int[] {7}, result);
  }


  @Test
  void testBitPackedPartialGroup() {
    // Test that we correctly handle reading only part of a group
    // Group size is 8, but we only need 3 values
    // According to spec, we should still read full 8 values worth of bytes
    ByteBuffer buffer = ByteBuffer.allocate(20);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    buffer.put((byte) 3);    // (1 << 1) | 1 = 3 (1 group)
    // Pack 8 values [0, 1, 2, 3, 4, 5, 6, 7] with bit-width 3
    // Even though we only need 3 values, the encoder writes all 8
    buffer.put((byte) 0x88);
    buffer.put((byte) 0xC6);
    buffer.put((byte) 0xFA);
    buffer.flip();

    // Request only 3 values
    RleDecoder decoder = new RleDecoder(buffer, 3, 3);
    int[] result = decoder.readAll();

    // Should get first 3 values from the group
    assertArrayEquals(new int[] {0, 1, 2}, result);
  }

  /**
   * Helper method to write unsigned varint
   */
  private void writeUnsignedVarInt(ByteBuffer buffer, int value) {
    while (value > 0x7F) {
      buffer.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    buffer.put((byte) value);
  }
}
