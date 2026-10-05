package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.github.aloksingh.parquet.util.filter.FilterTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.ListMetadata;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.MapMetadata;
import io.github.aloksingh.parquet.model.Type;
import io.github.aloksingh.parquet.util.ByteUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ColumnEqualFilterTest {

  @Test
  public void testPrimitiveStringEqual() {
    LogicalColumnDescriptor descriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, "test");

    assertTrue(filter.apply("test"));
    assertFalse(filter.apply("other"));
  }

  @Test
  public void testPrimitiveIntegerEqual() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 42);

    assertTrue(filter.apply(42));
    assertFalse(filter.apply(43));
  }

  @Test
  public void testPrimitiveIntegerColumnWithStringMatchedValueEqual() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, "42");

    assertTrue(filter.apply(42));
    assertFalse(filter.apply(43));
  }

  @Test
  public void testPrimitiveDoubleEqual() {
    LogicalColumnDescriptor descriptor = primitive("col", Type.DOUBLE);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 3.14);

    assertTrue(filter.apply(3.14));
    assertFalse(filter.apply(3.15));
  }

  @Test
  public void testPrimitiveNullValue() {
    LogicalColumnDescriptor descriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testListEqual() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    LogicalColumnDescriptor descriptor = list(Type.BYTE_ARRAY);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchList);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertTrue(filter.apply(valueList));
  }

  @Test
  public void testListNotEqual() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    LogicalColumnDescriptor descriptor = list(Type.BYTE_ARRAY);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchList);

    List<String> valueList = Arrays.asList("a", "b", "d");
    assertFalse(filter.apply(valueList));
  }

  @Test
  public void testListDifferentSize() {
    List<String> matchList = Arrays.asList("a", "b");
    LogicalColumnDescriptor descriptor = list(Type.BYTE_ARRAY);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchList);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertFalse(filter.apply(valueList));
  }

  @Test
  public void testMapEqual() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    LogicalColumnDescriptor descriptor = map(Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchMap);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 2);

    assertTrue(filter.apply(valueMap));
  }

  @Test
  public void testMapKeyValueEqual() {

    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 1, Optional.of("key1"));
    Map<String, Integer> colValue = new HashMap<>();
    colValue.put("key1", 1);
    colValue.put("key2", 2);
    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testMapNotEqual() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    LogicalColumnDescriptor descriptor = map(Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchMap);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 3);

    assertFalse(filter.apply(valueMap));
  }

  @Test
  public void testMapDifferentSize() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);

    LogicalColumnDescriptor descriptor = map(Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchMap);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 2);

    assertFalse(filter.apply(valueMap));
  }

  @Test
  public void testMapMissingKey() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    LogicalColumnDescriptor descriptor = map(Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchMap);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key3", 2);

    assertFalse(filter.apply(valueMap));
  }

  // Skip method tests

  @Test
  public void testSkipWithNullValueAndNullCount() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 5L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueAndZeroNullCount() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueNoNullCountTracked() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), null, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipBooleanMatchesMin() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BOOLEAN);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, false);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertFalse(filter.skip(stats, false));
  }

  @Test
  public void testSkipBooleanMatchesMax() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BOOLEAN);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, true);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertFalse(filter.skip(stats, true));
  }

  @Test
  public void testSkipInt32WithinRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 15);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 15));
  }

  @Test
  public void testSkipInt32BelowRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 5));
  }

  @Test
  public void testSkipInt32AboveRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 25);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 25));
  }

  @Test
  public void testSkipInt32AtMin() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 10);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 10));
  }

  @Test
  public void testSkipInt32AtMax() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 20);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 20));
  }

  @Test
  public void testSkipInt64WithinRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 1500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertFalse(filter.skip(stats, 1500L));
  }

  @Test
  public void testSkipInt64BelowRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 500L));
  }

  @Test
  public void testSkipInt64AboveRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 2500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 2500L));
  }

  @Test
  public void testSkipFloatWithinRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 15.5f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 15.5f));
  }

  @Test
  public void testSkipFloatBelowRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 5.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertTrue(filter.skip(stats, 5.0f));
  }

  @Test
  public void testSkipFloatAboveRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 25.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertTrue(filter.skip(stats, 25.0f));
  }

  @Test
  public void testSkipDoubleWithinRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 15.5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 15.5));
  }

  @Test
  public void testSkipDoubleBelowRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 5.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertTrue(filter.skip(stats, 5.0));
  }

  @Test
  public void testSkipDoubleAboveRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 25.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertTrue(filter.skip(stats, 25.0));
  }

  @Test
  public void testSkipByteArray() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("a".getBytes(), "z".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipFixedLenByteArray() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FIXED_LEN_BYTE_ARRAY);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, "test".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    ColumnStatistics stats = new ColumnStatistics("aaaa".getBytes(), "zzzz".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipListType() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    LogicalColumnDescriptor descriptor =
        list(Type.BYTE_ARRAY);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchList);

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertFalse(filter.skip(stats, matchList));
  }

  @Test
  public void testSkipMapType() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    LogicalColumnDescriptor descriptor =
        map(Type.INT32);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchMap);

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertFalse(filter.skip(stats, matchMap));
  }

}

