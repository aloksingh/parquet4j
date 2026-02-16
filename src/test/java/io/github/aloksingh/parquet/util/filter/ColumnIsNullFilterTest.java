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

public class ColumnIsNullFilterTest {

  // Apply method tests

  @Test
  public void testApplyNullValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    assertTrue(filter.apply(null));
  }

  @Test
  public void testApplyNonNullInteger() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    assertFalse(filter.apply(42));
  }

  @Test
  public void testApplyNonNullString() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    assertFalse(filter.apply("test"));
  }

  @Test
  public void testApplyNonNullDouble() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    assertFalse(filter.apply(3.14));
  }

  @Test
  public void testApplyNonNullBoolean() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    assertFalse(filter.apply(true));
    assertFalse(filter.apply(false));
  }

  @Test
  public void testApplyNonNullObject() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    assertFalse(filter.apply(new Object()));
  }

  // Map column tests

  @Test
  public void testMapKeyValueIsNull() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", null);
    colValue.put("key2", 20);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueIsNotNull() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("key1"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 10);
    colValue.put("key2", 20);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyMissingTreatedAsNull() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("key3"));

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 10);
    colValue.put("key2", 20);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapItselfNullWithKeyReturnsFlase() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("key1"));

    assertFalse(filter.apply(null));
  }

  @Test
  public void testMapWithoutKeyCheckMapItself() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 10);
    colValue.put("key2", null);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapWithoutKeyNullMap() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    assertTrue(filter.apply(null));
  }

  @Test
  public void testMapKeyValueWithStringNull() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("name"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("name", null);
    colValue.put("id", "123");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithStringNotNull() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("name"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("name", "John");
    colValue.put("id", "123");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithDoubleNull() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("score"));

    Map<String, Double> colValue = new HashMap<>();
    colValue.put("score", null);
    colValue.put("rank", 5.0);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithDoubleNotNull() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("score"));

    Map<String, Double> colValue = new HashMap<>();
    colValue.put("score", 10.5);
    colValue.put("rank", 5.0);

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithLongNull() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("timestamp"));

    Map<String, Long> colValue = new HashMap<>();
    colValue.put("timestamp", null);
    colValue.put("counter", 500L);

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapKeyValueWithLongNotNull() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor, Optional.of("timestamp"));

    Map<String, Long> colValue = new HashMap<>();
    colValue.put("timestamp", 1000L);
    colValue.put("counter", 500L);

    assertFalse(filter.apply(colValue));
  }

  // isApplicable method tests

  @Test
  public void testIsApplicableSameDescriptor() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    assertTrue(filter.isApplicable(descriptor));
  }

  @Test
  public void testIsApplicableDifferentDescriptor() {
    LogicalColumnDescriptor descriptor1 =
        new LogicalColumnDescriptor("col1", LogicalType.PRIMITIVE, null, null);
    LogicalColumnDescriptor descriptor2 =
        new LogicalColumnDescriptor("col2", LogicalType.PRIMITIVE, null, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor1);

    assertFalse(filter.isApplicable(descriptor2));
  }

  // Skip method tests

  @Test
  public void testSkipWithNullCountZero() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullCountGreaterThanZero() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 5L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullCountOne() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 1L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNoNullCountTracked() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), null, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithInt64NullCountZero() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithInt64NullCountPresent() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 10L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithFloatNullCountZero() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithDoubleNullCountZero() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithBooleanNullCountZero() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithByteArrayNullCountZero() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BYTE_ARRAY, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats = new ColumnStatistics("a".getBytes(), "z".getBytes(), 0L, null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithByteArrayNullCountPresent() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BYTE_ARRAY, null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats = new ColumnStatistics("a".getBytes(), "z".getBytes(), 3L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithListTypeNullCountZero() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST,
            (io.github.aloksingh.parquet.model.ListMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithListTypeNullCountPresent() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST,
            (io.github.aloksingh.parquet.model.ListMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats = new ColumnStatistics(null, null, 7L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithMapTypeNullCountZero() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP,
            (io.github.aloksingh.parquet.model.MapMetadata) null);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(descriptor);

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertTrue(filter.skip(stats, null));
  }
}
