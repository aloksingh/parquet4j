package io.github.aloksingh.parquet.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteUtils {

  public static byte[] intToBytes(int value) {
    ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    bb.putInt(value);
    byte[] encodedArray = bb.array();
    return encodedArray;
  }

  public static byte[] longToBytes(long value) {
    ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    bb.putLong(value);
    byte[] encodedArray = bb.array();
    return encodedArray;
  }

  public static byte[] floatToBytes(float value) {
    ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    bb.putFloat(value);
    byte[] encodedArray = bb.array();
    return encodedArray;
  }

  public static byte[] doubleToBytes(double value) {
    ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    bb.putDouble(value);
    byte[] encodedArray = bb.array();
    return encodedArray;
  }

  public static byte[] booleanToBytes(boolean value) {
    return new byte[] {(byte) (value ? 1 : 0)};
  }
}
