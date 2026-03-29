package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ListMetadata;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.MapMetadata;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ColumnStringFiltersTest {

  private final LogicalColumnDescriptor primitiveDescriptor =
      new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
  private final LogicalColumnDescriptor listDescriptor =
      new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
  private final LogicalColumnDescriptor mapDescriptor =
      new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);

  // Contains Tests for Strings
  @Test
  public void testContainsString() {
    ColumnContainsFilter filter = new ColumnContainsFilter(primitiveDescriptor, "world");

    assertTrue(filter.apply("hello world"));
    assertTrue(filter.apply("world"));
    assertFalse(filter.apply("hello"));
  }

  @Test
  public void testContainsStringCaseSensitive() {
    ColumnContainsFilter filter = new ColumnContainsFilter(primitiveDescriptor, "World");

    assertTrue(filter.apply("Hello World"));
    assertFalse(filter.apply("hello world"));
  }

  @Test
  public void testContainsWithNullValue() {
    ColumnContainsFilter filter = new ColumnContainsFilter(primitiveDescriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testContainsWithNonString() {
    ColumnContainsFilter filter = new ColumnContainsFilter(primitiveDescriptor, "test");

    assertFalse(filter.apply(123));
  }

  // Contains Tests for Lists
  @Test
  public void testContainsInList() {
    ColumnContainsFilter filter = new ColumnContainsFilter(listDescriptor, "apple");

    List<String> list = Arrays.asList("apple", "banana", "cherry");
    assertTrue(filter.apply(list));

    List<String> noMatch = Arrays.asList("banana", "cherry");
    assertFalse(filter.apply(noMatch));
  }

  @Test
  public void testContainsInListWithIntegers() {
    ColumnContainsFilter filter = new ColumnContainsFilter(listDescriptor, 42);

    List<Integer> list = Arrays.asList(10, 20, 42, 50);
    assertTrue(filter.apply(list));

    List<Integer> noMatch = Arrays.asList(10, 20, 50);
    assertFalse(filter.apply(noMatch));
  }

  @Test
  public void testContainsOnMap() {
    ColumnContainsFilter filter = new ColumnContainsFilter(mapDescriptor, "test");

    // Contains is not applicable to maps
    assertFalse(filter.apply(new java.util.HashMap<>()));
  }

  // Prefix Tests
  @Test
  public void testPrefix() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "hello");

    assertTrue(filter.apply("hello world"));
    assertTrue(filter.apply("hello"));
    assertFalse(filter.apply("world hello"));
  }

  @Test
  public void testPrefixCaseSensitive() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "Hello");

    assertTrue(filter.apply("Hello World"));
    assertFalse(filter.apply("hello world"));
  }

  @Test
  public void testPrefixWithNullValue() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testPrefixWithNullMatch() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, null);

    assertFalse(filter.apply("test"));
  }

  @Test
  public void testPrefixWithNonString() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "test");

    assertFalse(filter.apply(123));
  }

  @Test
  public void testPrefixWithComplexType() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(listDescriptor, "test");

    assertFalse(filter.apply(Arrays.asList("test")));
  }

  @Test
  public void testPrefixEmptyString() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "");

    assertTrue(filter.apply("any string"));
    assertTrue(filter.apply(""));
  }

  // Suffix Tests
  @Test
  public void testSuffix() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "world");

    assertTrue(filter.apply("hello world"));
    assertTrue(filter.apply("world"));
    assertFalse(filter.apply("world hello"));
  }

  @Test
  public void testSuffixCaseSensitive() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "World");

    assertTrue(filter.apply("Hello World"));
    assertFalse(filter.apply("hello world"));
  }

  @Test
  public void testSuffixWithNullValue() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testSuffixWithNullMatch() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, null);

    assertFalse(filter.apply("test"));
  }

  @Test
  public void testSuffixWithNonString() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "test");

    assertFalse(filter.apply(123));
  }

  @Test
  public void testSuffixWithComplexType() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter(mapDescriptor, "test");

    assertFalse(filter.apply(new java.util.HashMap<>()));
  }

  @Test
  public void testSuffixEmptyString() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "");

    assertTrue(filter.apply("any string"));
    assertTrue(filter.apply(""));
  }

  // Prefix Tests with Map columns
  @Test
  public void testPrefixMapKeyValue() {
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "hello world");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyValueNoMatch() {
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "world hello");
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyValueExactMatch() {
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "hello");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyValueNull() {
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", null);
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyMissing() {
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyValueNonString() {
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "123", Optional.of("code"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("code", 12345);
    colValue.put("name", "test");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapWithoutKey() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(mapDescriptor, "hello");

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "hello world");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapEmptyPrefix() {
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "any string");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  // Suffix Tests with Map columns
  @Test
  public void testSuffixMapKeyValue() {
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "hello world");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyValueNoMatch() {
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "world hello");
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyValueExactMatch() {
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "world");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyValueNull() {
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", null);
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyMissing() {
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyValueNonString() {
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "45", Optional.of("code"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("code", 12345);
    colValue.put("name", "test");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapWithoutKey() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter(mapDescriptor, "world");

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "hello world");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapEmptySuffix() {
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "any string");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  // Edge cases for all string filters
  @Test
  public void testEmptyStringOperations() {
    ColumnContainsFilter containsFilter = new ColumnContainsFilter(primitiveDescriptor, "");
    assertTrue(containsFilter.apply("test"));
    assertTrue(containsFilter.apply(""));

    ColumnPrefixFilter prefixFilter = new ColumnPrefixFilter(primitiveDescriptor, "");
    assertTrue(prefixFilter.apply("test"));

    ColumnSuffixFilter suffixFilter = new ColumnSuffixFilter(primitiveDescriptor, "");
    assertTrue(suffixFilter.apply("test"));
  }
}
