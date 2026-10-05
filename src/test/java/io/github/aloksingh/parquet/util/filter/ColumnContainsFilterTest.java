package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.github.aloksingh.parquet.util.filter.FilterTestSupport.*;
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
        primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertTrue(filter.apply("this is a test string"));
  }

  @Test
  public void testPrimitiveStringDoesNotContain() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertFalse(filter.apply("this is a sample"));
  }

  @Test
  public void testPrimitiveStringExactMatch() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertTrue(filter.apply("test"));
  }

  @Test
  public void testMapColumnContainsValueMatch() {
    LogicalColumnDescriptor descriptor =
        map(Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertTrue(filter.apply(Map.of("foo", "test")));
    assertTrue(filter.apply(Map.of("bar", "test")));
    assertFalse(filter.apply(Map.of("bar", "test1")));
    assertTrue(filter.apply(Map.of("bar", "test1", "foo", "test")));
  }

  @Test
  public void testMapColumnKeyContainsValueMatch() {
    LogicalColumnDescriptor descriptor =
        map(Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test", Optional.of("key1"));

    assertTrue(filter.apply(Map.of("key1", "test")));
    assertFalse(filter.apply(Map.of("bar", "test")));
    assertFalse(filter.apply(Map.of("bar", "test1")));
    assertTrue(filter.apply(Map.of("bar", "test1", "key1", "test")));
  }

  @Test
  public void testPrimitiveStringEmptyMatch() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "");

    assertTrue(filter.apply("test"));
  }

  @Test
  public void testPrimitiveStringNullValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testPrimitiveNonStringValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertThrows(IllegalArgumentException.class, () -> filter.apply(42));
  }

  @Test
  public void testPrimitiveIntegerValue() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 5));
  }

  // Apply method tests for lists

  @Test
  public void testListContainsElement() {
    LogicalColumnDescriptor descriptor =
        list(Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "b");

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertTrue(filter.apply(valueList));
  }

  @Test
  public void testListDoesNotContainElement() {
    LogicalColumnDescriptor descriptor =
        list(Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "d");

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertFalse(filter.apply(valueList));
  }

  @Test
  public void testListContainsIntegerElement() {
    LogicalColumnDescriptor descriptor =
        list(Type.INT32);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 42);

    List<Integer> valueList = Arrays.asList(10, 20, 42, 50);
    assertTrue(filter.apply(valueList));
  }

  @Test
  public void testListDoesNotContainIntegerElement() {
    LogicalColumnDescriptor descriptor =
        list(Type.INT32);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, 100);

    List<Integer> valueList = Arrays.asList(10, 20, 42, 50);
    assertFalse(filter.apply(valueList));
  }

  @Test
  public void testListNullValue() {
    LogicalColumnDescriptor descriptor =
        list(Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testListEmptyList() {
    LogicalColumnDescriptor descriptor =
        list(Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    List<String> valueList = Arrays.asList();
    assertFalse(filter.apply(valueList));
  }

  // isApplicable method tests

  @Test
  public void testIsApplicableSameDescriptor() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    assertTrue(filter.isApplicable(descriptor));
  }

  @Test
  public void testIsApplicableDifferentDescriptor() {
    LogicalColumnDescriptor descriptor1 =
        primitive("col1", Type.BYTE_ARRAY);
    LogicalColumnDescriptor descriptor2 =
        primitive("col2", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor1, "test");

    assertFalse(filter.isApplicable(descriptor2));
  }

  // Skip method tests

  @Test
  public void testSkipWithNullValueAndNullCount() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, null));
  }

  @Test
  public void testSkipWithNullValueAndZeroNullCount() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, null));
  }

  @Test
  public void testSkipWithNullValueNoNullCountTracked() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, null));
  }

  @Test
  public void testSkipBooleanMatchesMin() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BOOLEAN);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, false));
  }

  @Test
  public void testSkipBooleanMatchesMax() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BOOLEAN);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, true));
  }

  @Test
  public void testSkipInt32WithinRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 15));
  }

  @Test
  public void testSkipInt32BelowRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 5));
  }

  @Test
  public void testSkipInt32AboveRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 25));
  }

  @Test
  public void testSkipInt32AtMin() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 10));
  }

  @Test
  public void testSkipInt32AtMax() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 20));
  }

  @Test
  public void testSkipInt64WithinRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 1500L));
  }

  @Test
  public void testSkipInt64BelowRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 500L));
  }

  @Test
  public void testSkipInt64AboveRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.INT64);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 2500L));
  }

  @Test
  public void testSkipFloatWithinRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 15.5f));
  }

  @Test
  public void testSkipFloatBelowRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 5.0f));
  }

  @Test
  public void testSkipFloatAboveRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FLOAT);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 25.0f));
  }

  @Test
  public void testSkipDoubleWithinRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 15.5));
  }

  @Test
  public void testSkipDoubleBelowRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 5.0));
  }

  @Test
  public void testSkipDoubleAboveRange() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.DOUBLE);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, 25.0));
  }

  @Test
  public void testSkipByteArray() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "test");

    ColumnStatistics stats = new ColumnStatistics("a".getBytes(), "z".getBytes(), 0L, null);
    assertFalse(filter.skip(stats, "test"));
  }

  @Test
  public void testSkipFixedLenByteArray() {
    LogicalColumnDescriptor descriptor =
        primitive("col", Type.FIXED_LEN_BYTE_ARRAY);
    assertThrows(IllegalArgumentException.class, () -> new ColumnContainsFilter(descriptor, "test"));
  }

  @Test
  public void testSkipListType() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    LogicalColumnDescriptor descriptor =
        list(Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(descriptor, "b");

    ColumnStatistics stats = new ColumnStatistics(null, null, 0L, null);
    assertFalse(filter.skip(stats, matchList));
  }
}

