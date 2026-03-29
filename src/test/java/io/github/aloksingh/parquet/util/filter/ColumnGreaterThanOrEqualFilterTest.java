package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
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

public class ColumnGreaterThanOrEqualFilterTest {

  // Apply method tests

  @Test
  public void testIntegerGreaterThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10);

    assertTrue(filter.apply(15));
    assertTrue(filter.apply(10));
    assertFalse(filter.apply(5));
  }

  @Test
  public void testLongGreaterThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 100L);

    assertTrue(filter.apply(200L));
    assertTrue(filter.apply(100L));
    assertFalse(filter.apply(50L));
  }

  @Test
  public void testFloatGreaterThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10.5f);

    assertTrue(filter.apply(15.5f));
    assertTrue(filter.apply(10.5f));
    assertFalse(filter.apply(5.5f));
  }

  @Test
  public void testDoubleGreaterThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10.5);

    assertTrue(filter.apply(15.5));
    assertTrue(filter.apply(10.5));
    assertFalse(filter.apply(5.5));
  }

  @Test
  public void testStringGreaterThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, "m");

    assertTrue(filter.apply("z"));
    assertTrue(filter.apply("m"));
    assertFalse(filter.apply("a"));
  }

  @Test
  public void testNullValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10);

    assertFalse(filter.apply(null));
  }

  @Test
  public void testNullMatchValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, null);

    assertFalse(filter.apply(10));
  }

  @Test
  public void testNonPrimitiveType() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP,
        (io.github.aloksingh.parquet.model.MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10);

    assertFalse(filter.apply(15));
  }

  @Test
  public void testNonComparableValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, "test");

    Object nonComparable = new Object();
    assertFalse(filter.apply(nonComparable));
  }

  @Test
  public void testIncompatibleTypes() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10);

    // Comparing Integer matchValue with String colValue should return false
    assertFalse(filter.apply("test"));
  }

  // Map column tests

  @Test
  public void testMapKeyValueGreaterThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 15);
    colValue.put("key2", 5);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueEqualToMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 10);
    colValue.put("key2", 5);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueLessThan() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 5);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueNullValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", null);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyMissing() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 10, Optional.of("key3"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 15);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueNonComparable() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("key1", new Object());
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueIncompatibleTypes() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("key1", "not a number");
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapWithoutKeyNotSupported() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10);

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 15);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithStringGreaterThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, "b", Optional.of("name"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("name", "zebra");
    colValue.put("id", "123");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithStringEqualToMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, "b", Optional.of("name"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("name", "b");
    colValue.put("id", "123");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithDoubleGreaterThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 10.5, Optional.of("score"));

    Map<String, Double> colValue = new HashMap<>();
    colValue.put("score", 15.7);
    colValue.put("rank", 5.0);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithDoubleEqualToMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 10.5, Optional.of("score"));

    Map<String, Double> colValue = new HashMap<>();
    colValue.put("score", 10.5);
    colValue.put("rank", 5.0);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithLongGreaterThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 1000L, Optional.of("timestamp"));

    Map<String, Long> colValue = new HashMap<>();
    colValue.put("timestamp", 2000L);
    colValue.put("counter", 500L);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithLongEqualToMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(descriptor, 1000L, Optional.of("timestamp"));

    Map<String, Long> colValue = new HashMap<>();
    colValue.put("timestamp", 1000L);
    colValue.put("counter", 500L);

    assertTrue(filter.apply(colValue));
  }

  // isApplicable method tests

  @Test
  public void testIsApplicableSameDescriptor() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10);

    assertTrue(filter.isApplicable(descriptor));
  }

  @Test
  public void testIsApplicableDifferentDescriptor() {
    LogicalColumnDescriptor descriptor1 =
        new LogicalColumnDescriptor("col1", LogicalType.PRIMITIVE, null, null);
    LogicalColumnDescriptor descriptor2 =
        new LogicalColumnDescriptor("col2", LogicalType.PRIMITIVE, null, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor1, 10);

    assertFalse(filter.isApplicable(descriptor2));
  }

  // Skip method tests

  @Test
  public void testSkipWithNullValueAndNullCount() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 5L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueAndZeroNullCount() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueNoNullCountTracked() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), null, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipBooleanMaxGreaterThanOrEqualValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, false);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, false));
  }

  @Test
  public void testSkipBooleanMaxEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, true);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, true));
  }

  @Test
  public void testSkipBooleanMaxLessThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, true);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(false), 0L,
            null);
    assertFalse(filter.skip(stats, true));
  }

  @Test
  public void testSkipInt32MaxGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 15);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 15));
  }

  @Test
  public void testSkipInt32MaxEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 20);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 20));
  }

  @Test
  public void testSkipInt32MaxLessThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 25);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 25));
  }

  @Test
  public void testSkipInt32ValueAtMin() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 10));
  }

  @Test
  public void testSkipInt32ValueBelowMin() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 5));
  }

  @Test
  public void testSkipInt64MaxGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 1500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 1500L));
  }

  @Test
  public void testSkipInt64MaxEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 2000L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 2000L));
  }

  @Test
  public void testSkipInt64MaxLessThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 2500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertFalse(filter.skip(stats, 2500L));
  }

  @Test
  public void testSkipFloatMaxGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 15.5f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertTrue(filter.skip(stats, 15.5f));
  }

  @Test
  public void testSkipFloatMaxEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 20.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertTrue(filter.skip(stats, 20.0f));
  }

  @Test
  public void testSkipFloatMaxLessThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 25.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 25.0f));
  }

  @Test
  public void testSkipDoubleMaxGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 15.5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertTrue(filter.skip(stats, 15.5));
  }

  @Test
  public void testSkipDoubleMaxEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 20.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertTrue(filter.skip(stats, 20.0));
  }

  @Test
  public void testSkipDoubleMaxLessThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 25.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 25.0));
  }

  @Test
  public void testSkipByteArray() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BYTE_ARRAY, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("a".getBytes(), "z".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipFixedLenByteArray() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FIXED_LEN_BYTE_ARRAY, null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("aaaa".getBytes(), "zzzz".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipNonPrimitiveType() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST,
            (io.github.aloksingh.parquet.model.ListMetadata) null);
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(descriptor, 10);

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertFalse(filter.skip(stats, 10));
  }
}
