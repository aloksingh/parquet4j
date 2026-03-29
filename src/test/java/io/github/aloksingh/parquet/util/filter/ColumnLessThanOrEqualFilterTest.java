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

public class ColumnLessThanOrEqualFilterTest {

  // Apply method tests

  @Test
  public void testIntegerLessThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10);

    assertTrue(filter.apply(5));
    assertTrue(filter.apply(10));
    assertFalse(filter.apply(15));
  }

  @Test
  public void testLongLessThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 100L);

    assertTrue(filter.apply(50L));
    assertTrue(filter.apply(100L));
    assertFalse(filter.apply(200L));
  }

  @Test
  public void testFloatLessThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10.5f);

    assertTrue(filter.apply(5.5f));
    assertTrue(filter.apply(10.5f));
    assertFalse(filter.apply(15.5f));
  }

  @Test
  public void testDoubleLessThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10.5);

    assertTrue(filter.apply(5.5));
    assertTrue(filter.apply(10.5));
    assertFalse(filter.apply(15.5));
  }

  @Test
  public void testStringLessThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, "m");

    assertTrue(filter.apply("a"));
    assertTrue(filter.apply("m"));
    assertFalse(filter.apply("z"));
  }

  @Test
  public void testNullValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10);

    assertFalse(filter.apply(null));
  }

  @Test
  public void testNullMatchValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, null);

    assertFalse(filter.apply(10));
  }

  @Test
  public void testNonPrimitiveType() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP,
        (io.github.aloksingh.parquet.model.MapMetadata) null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10);

    assertFalse(filter.apply(5));
  }

  @Test
  public void testNonComparableValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, "test");

    Object nonComparable = new Object();
    assertFalse(filter.apply(nonComparable));
  }

  @Test
  public void testIncompatibleTypes() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10);

    // Comparing Integer matchValue with String colValue should return false
    assertFalse(filter.apply("test"));
  }

  // Map column tests

  @Test
  public void testMapKeyValueLessThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 5);
    colValue.put("key2", 20);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueEqualToMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 10);
    colValue.put("key2", 5);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueGreaterThan() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 15);
    colValue.put("key2", 5);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueNullValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", null);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyMissing() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 10, Optional.of("key3"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 15);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueNonComparable() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("key1", new Object());
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueIncompatibleTypes() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 10, Optional.of("key1"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("key1", "not a number");
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapWithoutKeyNotSupported() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10);

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 5);
    colValue.put("key2", 10);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithStringLessThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, "m", Optional.of("name"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("name", "apple");
    colValue.put("id", "123");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithStringEqualToMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, "m", Optional.of("name"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("name", "m");
    colValue.put("id", "123");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithDoubleLessThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 10.5, Optional.of("score"));

    Map<String, Double> colValue = new HashMap<>();
    colValue.put("score", 8.3);
    colValue.put("rank", 15.0);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithDoubleEqualToMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 10.5, Optional.of("score"));

    Map<String, Double> colValue = new HashMap<>();
    colValue.put("score", 10.5);
    colValue.put("rank", 5.0);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithLongLessThanOrEqual() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 1000L, Optional.of("timestamp"));

    Map<String, Long> colValue = new HashMap<>();
    colValue.put("timestamp", 500L);
    colValue.put("counter", 2000L);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithLongEqualToMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(descriptor, 1000L, Optional.of("timestamp"));

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
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10);

    assertTrue(filter.isApplicable(descriptor));
  }

  @Test
  public void testIsApplicableDifferentDescriptor() {
    LogicalColumnDescriptor descriptor1 =
        new LogicalColumnDescriptor("col1", LogicalType.PRIMITIVE, null, null);
    LogicalColumnDescriptor descriptor2 =
        new LogicalColumnDescriptor("col2", LogicalType.PRIMITIVE, null, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor1, 10);

    assertFalse(filter.isApplicable(descriptor2));
  }

  // Skip method tests

  @Test
  public void testSkipWithNullValueAndNullCount() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 5L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueAndZeroNullCount() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueNoNullCountTracked() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), null, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipBooleanMinLessThanOrEqualValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, true);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, true));
  }

  @Test
  public void testSkipBooleanMinEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, false);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, false));
  }

  @Test
  public void testSkipBooleanMinGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, false);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(true), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertFalse(filter.skip(stats, false));
  }

  @Test
  public void testSkipInt32MinLessThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 15);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 15));
  }

  @Test
  public void testSkipInt32MinEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 10));
  }

  @Test
  public void testSkipInt32MinGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 5));
  }

  @Test
  public void testSkipInt32ValueAtMax() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 20);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 20));
  }

  @Test
  public void testSkipInt32ValueAboveMax() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 25);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 25));
  }

  @Test
  public void testSkipInt64MinLessThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 1500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 1500L));
  }

  @Test
  public void testSkipInt64MinEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 1000L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 1000L));
  }

  @Test
  public void testSkipInt64MinGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertFalse(filter.skip(stats, 500L));
  }

  @Test
  public void testSkipFloatMinLessThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 15.5f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertTrue(filter.skip(stats, 15.5f));
  }

  @Test
  public void testSkipFloatMinEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertTrue(filter.skip(stats, 10.0f));
  }

  @Test
  public void testSkipFloatMinGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 5.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 5.0f));
  }

  @Test
  public void testSkipDoubleMinLessThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 15.5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertTrue(filter.skip(stats, 15.5));
  }

  @Test
  public void testSkipDoubleMinEqualToValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertTrue(filter.skip(stats, 10.0));
  }

  @Test
  public void testSkipDoubleMinGreaterThanValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 5.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 5.0));
  }

  @Test
  public void testSkipByteArray() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BYTE_ARRAY, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("a".getBytes(), "z".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipFixedLenByteArray() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FIXED_LEN_BYTE_ARRAY, null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("aaaa".getBytes(), "zzzz".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipNonPrimitiveType() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST,
            (io.github.aloksingh.parquet.model.ListMetadata) null);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(descriptor, 10);

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertFalse(filter.skip(stats, 10));
  }
}
