package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
import org.junit.jupiter.api.Test;

public class ColumnEqualFilterTest {

  @Test
  public void testPrimitiveStringEqual() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, "test");

    assertTrue(filter.apply("test"));
    assertFalse(filter.apply("other"));
  }

  @Test
  public void testPrimitiveIntegerEqual() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 42);

    assertTrue(filter.apply(42));
    assertFalse(filter.apply(43));
  }

  @Test
  public void testPrimitiveDoubleEqual() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 3.14);

    assertTrue(filter.apply(3.14));
    assertFalse(filter.apply(3.15));
  }

  @Test
  public void testPrimitiveNullValue() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testListEqual() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchList);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertTrue(filter.apply(valueList));
  }

  @Test
  public void testListNotEqual() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchList);

    List<String> valueList = Arrays.asList("a", "b", "d");
    assertFalse(filter.apply(valueList));
  }

  @Test
  public void testListDifferentSize() {
    List<String> matchList = Arrays.asList("a", "b");
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchList);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertFalse(filter.apply(valueList));
  }

  @Test
  public void testMapEqual() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchMap);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 2);

    assertTrue(filter.apply(valueMap));
  }

  @Test
  public void testMapNotEqual() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
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

    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
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

    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
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
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 5L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueAndZeroNullCount() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueNoNullCountTracked() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), null, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipBooleanMatchesMin() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, false);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, false));
  }

  @Test
  public void testSkipBooleanMatchesMax() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, true);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, true));
  }

  @Test
  public void testSkipInt32WithinRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 15);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 15));
  }

  @Test
  public void testSkipInt32BelowRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 5));
  }

  @Test
  public void testSkipInt32AboveRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 25);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 25));
  }

  @Test
  public void testSkipInt32AtMin() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 10);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 10));
  }

  @Test
  public void testSkipInt32AtMax() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 20);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 20));
  }

  @Test
  public void testSkipInt64WithinRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 1500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 1500L));
  }

  @Test
  public void testSkipInt64BelowRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertFalse(filter.skip(stats, 500L));
  }

  @Test
  public void testSkipInt64AboveRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 2500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertFalse(filter.skip(stats, 2500L));
  }

  @Test
  public void testSkipFloatWithinRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 15.5f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertTrue(filter.skip(stats, 15.5f));
  }

  @Test
  public void testSkipFloatBelowRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 5.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 5.0f));
  }

  @Test
  public void testSkipFloatAboveRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 25.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 25.0f));
  }

  @Test
  public void testSkipDoubleWithinRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 15.5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertTrue(filter.skip(stats, 15.5));
  }

  @Test
  public void testSkipDoubleBelowRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 5.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 5.0));
  }

  @Test
  public void testSkipDoubleAboveRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, 25.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 25.0));
  }

  @Test
  public void testSkipByteArray() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BYTE_ARRAY, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("a".getBytes(), "z".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipFixedLenByteArray() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FIXED_LEN_BYTE_ARRAY, null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("aaaa".getBytes(), "zzzz".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipListType() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchList);

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertFalse(filter.skip(stats, matchList));
  }

  @Test
  public void testSkipMapType() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnEqualFilter filter = new ColumnEqualFilter(descriptor, matchMap);

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertFalse(filter.skip(stats, matchMap));
  }

}
