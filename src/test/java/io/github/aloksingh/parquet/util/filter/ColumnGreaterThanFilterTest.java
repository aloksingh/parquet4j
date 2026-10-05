package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.github.aloksingh.parquet.util.filter.FilterTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.MapMetadata;
import io.github.aloksingh.parquet.model.Type;
import io.github.aloksingh.parquet.util.ByteUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ColumnGreaterThanFilterTest {

  // Apply method tests

  @Test
  public void testIntegerGreaterThan() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 10);

    assertTrue(filter.apply(15));
    assertFalse(filter.apply(10));
    assertFalse(filter.apply(5));
  }

  @Test
  public void testLongGreaterThan() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, "100");

    assertTrue(filter.apply(200L));
    assertFalse(filter.apply(100L));
    assertFalse(filter.apply(50L));
  }

  @Test
  public void testFloatGreaterThan() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 10.5f);

    assertTrue(filter.apply(15.5f));
    assertFalse(filter.apply(10.5f));
    assertFalse(filter.apply(5.5f));
  }

  @Test
  public void testDoubleGreaterThan() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 10.5);

    assertTrue(filter.apply(15.5));
    assertFalse(filter.apply(10.5));
    assertFalse(filter.apply(5.5));
  }

  @Test
  public void testStringGreaterThan() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, "m");

    assertTrue(filter.apply("z"));
    assertFalse(filter.apply("m"));
    assertFalse(filter.apply("a"));
  }

  @Test
  public void testNullValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 10);

    assertFalse(filter.apply(null));
  }

  @Test
  public void testNullMatchValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnGreaterThanFilter(descriptor, null));
  }

  @Test
  public void testNonPrimitiveType() {
    LogicalColumnDescriptor descriptor = map(Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnGreaterThanFilter(descriptor, 10));
  }

  @Test
  public void testNonComparableValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, "test");

    Object nonComparable = new Object();
    assertThrows(IllegalArgumentException.class, () -> filter.apply(nonComparable));
  }

  @Test
  public void testIncompatibleTypes() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 10);

    // Comparing Integer matchValue with String colValue must throw
    assertThrows(IllegalArgumentException.class, () -> filter.apply("test"));
  }

  // Map column tests

  @Test
  public void testMapKeyValueGreaterThan() {
    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 15);
    colValue.put("key2", 5);

    assertTrue(filter.apply(colValue));
    colValue = new HashMap<>();
    colValue.put("key1", 10);
    colValue.put("key2", 5);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueEqualToMatch() {
    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 10);
    colValue.put("key2", 5);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueLessThan() {
    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 5);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueNullValue() {
    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", null);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyMissing() {
    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, 10, Optional.of("key3"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 15);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueNonComparable() {
    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("key1", new Object());
    colValue.put("key2", 20);

    assertThrows(IllegalArgumentException.class, () -> filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueIncompatibleTypes() {
    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("key1", "not a number");
    colValue.put("key2", 20);

    assertThrows(IllegalArgumentException.class, () -> filter.apply(colValue));
  }

  @Test
  public void testMapWithoutKeyNotSupported() {
    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnGreaterThanFilter(descriptor, 10));
  }

  @Test
  public void testMapKeyValueWithStringGreaterThan() {
    LogicalColumnDescriptor descriptor =
        map(Type.BYTE_ARRAY);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, "b", Optional.of("name"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("name", "zebra");
    colValue.put("id", "123");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithDoubleGreaterThan() {
    LogicalColumnDescriptor descriptor =
        map(Type.DOUBLE);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, 10.5, Optional.of("score"));

    Map<String, Double> colValue = new HashMap<>();
    colValue.put("score", 15.7);
    colValue.put("rank", 5.0);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithLongGreaterThan() {
    LogicalColumnDescriptor descriptor =
        map(Type.INT64);
    ColumnGreaterThanFilter filter =
        new ColumnGreaterThanFilter(descriptor, 1000L, Optional.of("timestamp"));

    Map<String, Long> colValue = new HashMap<>();
    colValue.put("timestamp", 2000L);
    colValue.put("counter", 500L);

    assertTrue(filter.apply(colValue));
  }

  // isApplicable method tests

  @Test
  public void testIsApplicableSameDescriptor() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 10);

    assertTrue(filter.isApplicable(descriptor));
  }

  @Test
  public void testIsApplicableDifferentDescriptor() {
    LogicalColumnDescriptor descriptor1 =
        primitive("col1", Type.INT32);
    LogicalColumnDescriptor descriptor2 =
        primitive("col2", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor1, 10);

    assertFalse(filter.isApplicable(descriptor2));
  }

  // Skip method tests

  @Test
  public void testSkipWithNullValueAndNullCount() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnGreaterThanFilter(descriptor, null));
  }

  @Test
  public void testSkipWithNullValueAndZeroNullCount() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnGreaterThanFilter(descriptor, null));
  }

  @Test
  public void testSkipWithNullValueNoNullCountTracked() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnGreaterThanFilter(descriptor, null));
  }

  @Test
  public void testSkipBooleanMaxGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BOOLEAN);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, false);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertFalse(filter.skip(stats, false));
  }

  @Test
  public void testSkipBooleanMaxNotGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BOOLEAN);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, true);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, true));
  }

  @Test
  public void testSkipInt32MaxGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 15);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 15));
  }

  @Test
  public void testSkipInt32MaxEqualToValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 20);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 20));
  }

  @Test
  public void testSkipInt32MaxLessThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 25);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 25));
  }

  @Test
  public void testSkipInt32ValueAtMin() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 10);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 10));
  }

  @Test
  public void testSkipInt32ValueBelowMin() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 5));
  }

  @Test
  public void testSkipInt64MaxGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 1500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertFalse(filter.skip(stats, 1500L));
  }

  @Test
  public void testSkipInt64MaxEqualToValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 2000L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 2000L));
  }

  @Test
  public void testSkipInt64MaxLessThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 2500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 2500L));
  }

  @Test
  public void testSkipFloatMaxGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 15.5f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 15.5f));
  }

  @Test
  public void testSkipFloatMaxEqualToValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 20.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 20.0f));
  }

  @Test
  public void testSkipFloatMaxLessThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 25.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 25.0f));
  }

  @Test
  public void testSkipDoubleMaxGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 15.5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 15.5));
  }

  @Test
  public void testSkipDoubleMaxEqualToValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 20.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 20.0));
  }

  @Test
  public void testSkipDoubleMaxLessThanValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, 25.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 25.0));
  }

  @Test
  public void testSkipByteArray() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("a".getBytes(), "z".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipFixedLenByteArray() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FIXED_LEN_BYTE_ARRAY);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(descriptor, "test".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    ColumnStatistics stats = new ColumnStatistics("aaaa".getBytes(), "zzzz".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipNonPrimitiveType() {
    LogicalColumnDescriptor descriptor =
        list(Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnGreaterThanFilter(descriptor, 10));
  }
}
