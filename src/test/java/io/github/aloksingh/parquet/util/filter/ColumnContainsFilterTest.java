package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.ListMetadata;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.Type;
import io.github.aloksingh.parquet.util.ByteUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ColumnContainsFilterTest {

  // Apply method tests for primitive strings

  @Test
  public void testPrimitiveStringContains() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertTrue(filter.apply("this is a test string"));
  }

  @Test
  public void testPrimitiveStringDoesNotContain() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertFalse(filter.apply("this is a sample"));
  }

  @Test
  public void testPrimitiveStringExactMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertTrue(filter.apply("test"));
  }

  @Test
  public void testMapColumnContainsValueMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertTrue(filter.apply(Map.of("foo", "test")));
    assertTrue(filter.apply(Map.of("bar", "test")));
    assertFalse(filter.apply(Map.of("bar", "test1")));
    assertTrue(filter.apply(Map.of("bar", "test1", "foo", "test")));
  }

  @Test
  public void testMapColumnKeyContainsValueMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.MAP, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test", Optional.of("key1"));

    assertTrue(filter.apply(Map.of("key1", "test")));
    assertFalse(filter.apply(Map.of("bar", "test")));
    assertFalse(filter.apply(Map.of("bar", "test1")));
    assertTrue(filter.apply(Map.of("bar", "test1", "key1", "test")));
  }

  @Test
  public void testPrimitiveStringEmptyMatch() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "");

    assertTrue(filter.apply("test"));
  }

  @Test
  public void testPrimitiveStringNullValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testPrimitiveNonStringValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertFalse(filter.apply(42));
  }

  @Test
  public void testPrimitiveIntegerValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 5);

    assertFalse(filter.apply(5));
  }

  // Apply method tests for lists

  @Test
  public void testListContainsElement() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "b");

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertTrue(filter.apply(valueList));
  }

  @Test
  public void testListDoesNotContainElement() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "d");

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertFalse(filter.apply(valueList));
  }

  @Test
  public void testListContainsIntegerElement() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 42);

    List<Integer> valueList = Arrays.asList(10, 20, 42, 50);
    assertTrue(filter.apply(valueList));
  }

  @Test
  public void testListDoesNotContainIntegerElement() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 100);

    List<Integer> valueList = Arrays.asList(10, 20, 42, 50);
    assertFalse(filter.apply(valueList));
  }

  @Test
  public void testListNullValue() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testListEmptyList() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    List<String> valueList = Arrays.asList();
    assertFalse(filter.apply(valueList));
  }

  // isApplicable method tests

  @Test
  public void testIsApplicableSameDescriptor() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertTrue(filter.isApplicable(descriptor));
  }

  @Test
  public void testIsApplicableDifferentDescriptor() {
    LogicalColumnDescriptor descriptor1 =
        new LogicalColumnDescriptor("col1", LogicalType.PRIMITIVE, null, null);
    LogicalColumnDescriptor descriptor2 =
        new LogicalColumnDescriptor("col2", LogicalType.PRIMITIVE, null, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor1, "test");

    assertFalse(filter.isApplicable(descriptor2));
  }

  // Skip method tests

  @Test
  public void testSkipWithNullValueAndNullCount() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 5L, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueAndZeroNullCount() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, null));
  }

  @Test
  public void testSkipWithNullValueNoNullCountTracked() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, null);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), null, null);
    assertFalse(filter.skip(stats, null));
  }

  @Test
  public void testSkipBooleanMatchesMin() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, false);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, false));
  }

  @Test
  public void testSkipBooleanMatchesMax() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BOOLEAN, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, true);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.booleanToBytes(false), ByteUtils.booleanToBytes(true), 0L,
            null);
    assertTrue(filter.skip(stats, true));
  }

  @Test
  public void testSkipInt32WithinRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 15);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 15));
  }

  @Test
  public void testSkipInt32BelowRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 5));
  }

  @Test
  public void testSkipInt32AboveRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 25);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertFalse(filter.skip(stats, 25));
  }

  @Test
  public void testSkipInt32AtMin() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 10);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 10));
  }

  @Test
  public void testSkipInt32AtMax() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT32, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 20);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.intToBytes(10), ByteUtils.intToBytes(20), 0L, null);
    assertTrue(filter.skip(stats, 20));
  }

  @Test
  public void testSkipInt64WithinRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 1500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertTrue(filter.skip(stats, 1500L));
  }

  @Test
  public void testSkipInt64BelowRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertFalse(filter.skip(stats, 500L));
  }

  @Test
  public void testSkipInt64AboveRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.INT64, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 2500L);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.longToBytes(1000L), ByteUtils.longToBytes(2000L), 0L, null);
    assertFalse(filter.skip(stats, 2500L));
  }

  @Test
  public void testSkipFloatWithinRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 15.5f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertTrue(filter.skip(stats, 15.5f));
  }

  @Test
  public void testSkipFloatBelowRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 5.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 5.0f));
  }

  @Test
  public void testSkipFloatAboveRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FLOAT, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 25.0f);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.floatToBytes(10.0f), ByteUtils.floatToBytes(20.0f), 0L,
            null);
    assertFalse(filter.skip(stats, 25.0f));
  }

  @Test
  public void testSkipDoubleWithinRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 15.5);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertTrue(filter.skip(stats, 15.5));
  }

  @Test
  public void testSkipDoubleBelowRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 5.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 5.0));
  }

  @Test
  public void testSkipDoubleAboveRange() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.DOUBLE, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 25.0);

    ColumnStatistics stats =
        new ColumnStatistics(ByteUtils.doubleToBytes(10.0), ByteUtils.doubleToBytes(20.0), 0L,
            null);
    assertFalse(filter.skip(stats, 25.0));
  }

  @Test
  public void testSkipByteArray() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.BYTE_ARRAY, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("a".getBytes(), "z".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipFixedLenByteArray() {
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, Type.FIXED_LEN_BYTE_ARRAY, null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("aaaa".getBytes(), "zzzz".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipListType() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    LogicalColumnDescriptor descriptor =
        new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "b");

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertFalse(filter.skip(stats, matchList));
  }
}
