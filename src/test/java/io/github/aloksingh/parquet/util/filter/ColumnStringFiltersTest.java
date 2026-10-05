package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.github.aloksingh.parquet.util.filter.FilterTestSupport.*;
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
import io.github.aloksingh.parquet.model.Type;
import org.junit.jupiter.api.Test;

public class ColumnStringFiltersTest {


  // Contains Tests for Strings
  @Test
  public void testContainsString() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(primitiveDescriptor, "world");

    assertTrue(filter.apply("hello world"));
    assertTrue(filter.apply("world"));
    assertFalse(filter.apply("hello"));
  }

  @Test
  public void testContainsStringCaseSensitive() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(primitiveDescriptor, "World");

    assertTrue(filter.apply("Hello World"));
    assertFalse(filter.apply("hello world"));
  }

  @Test
  public void testContainsWithNullValue() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(primitiveDescriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testContainsWithNonString() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(primitiveDescriptor, "test");

    assertThrows(IllegalArgumentException.class, () -> filter.apply(123));
  }

  // Contains Tests for Lists
  @Test
  public void testContainsInList() {
    var listDescriptor = list(Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(listDescriptor, "apple");

    List<String> list = Arrays.asList("apple", "banana", "cherry");
    assertTrue(filter.apply(list));

    List<String> noMatch = Arrays.asList("banana", "cherry");
    assertFalse(filter.apply(noMatch));
  }

  @Test
  public void testContainsInListWithIntegers() {
    var listDescriptor = list(Type.INT32);
    ColumnContainsFilter filter = new ColumnContainsFilter(listDescriptor, 42);

    List<Integer> list = Arrays.asList(10, 20, 42, 50);
    assertTrue(filter.apply(list));

    List<Integer> noMatch = Arrays.asList(10, 20, 50);
    assertFalse(filter.apply(noMatch));
  }

  @Test
  public void testContainsOnMap() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnContainsFilter filter = new ColumnContainsFilter(mapDescriptor, "test");

    // Contains is not applicable to maps
    assertFalse(filter.apply(new java.util.HashMap<>()));
  }

  // Prefix Tests
  @Test
  public void testPrefix() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "hello");

    assertTrue(filter.apply("hello world"));
    assertTrue(filter.apply("hello"));
    assertFalse(filter.apply("world hello"));
  }

  @Test
  public void testPrefixCaseSensitive() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "Hello");

    assertTrue(filter.apply("Hello World"));
    assertFalse(filter.apply("hello world"));
  }

  @Test
  public void testPrefixWithNullValue() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testPrefixWithNullMatch() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnPrefixFilter(primitiveDescriptor, null));
  }

  @Test
  public void testPrefixWithNonString() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "test");

    assertThrows(IllegalArgumentException.class, () -> filter.apply(123));
  }

  @Test
  public void testPrefixWithComplexType() {
    var listDescriptor = list(Type.BYTE_ARRAY);
    assertThrows(IllegalArgumentException.class, () -> new ColumnPrefixFilter(listDescriptor, "test"));
  }

  @Test
  public void testPrefixEmptyString() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnPrefixFilter filter = new ColumnPrefixFilter(primitiveDescriptor, "");

    assertTrue(filter.apply("any string"));
    assertTrue(filter.apply(""));
  }

  // Suffix Tests
  @Test
  public void testSuffix() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "world");

    assertTrue(filter.apply("hello world"));
    assertTrue(filter.apply("world"));
    assertFalse(filter.apply("world hello"));
  }

  @Test
  public void testSuffixCaseSensitive() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "World");

    assertTrue(filter.apply("Hello World"));
    assertFalse(filter.apply("hello world"));
  }

  @Test
  public void testSuffixWithNullValue() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "test");

    assertFalse(filter.apply(null));
  }

  @Test
  public void testSuffixWithNullMatch() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnSuffixFilter(primitiveDescriptor, null));
  }

  @Test
  public void testSuffixWithNonString() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "test");

    assertThrows(IllegalArgumentException.class, () -> filter.apply(123));
  }

  @Test
  public void testSuffixWithComplexType() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    assertThrows(IllegalArgumentException.class, () -> new ColumnSuffixFilter(mapDescriptor, "test"));
  }

  @Test
  public void testSuffixEmptyString() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnSuffixFilter filter = new ColumnSuffixFilter(primitiveDescriptor, "");

    assertTrue(filter.apply("any string"));
    assertTrue(filter.apply(""));
  }

  // Prefix Tests with Map columns
  @Test
  public void testPrefixMapKeyValue() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "hello world");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyValueNoMatch() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "world hello");
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyValueExactMatch() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "hello");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyValueNull() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", null);
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyMissing() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "hello", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testPrefixMapKeyValueNonString() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnPrefixFilter filter =
        new ColumnPrefixFilter(mapDescriptor, "123", Optional.of("code"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("code", 12345);
    colValue.put("name", "test");

    assertThrows(IllegalArgumentException.class, () -> filter.apply(colValue));
  }

  @Test
  public void testPrefixMapWithoutKey() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    assertThrows(IllegalArgumentException.class, () -> new ColumnPrefixFilter(mapDescriptor, "hello"));
  }

  @Test
  public void testPrefixMapEmptyPrefix() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
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
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "hello world");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyValueNoMatch() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "world hello");
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyValueExactMatch() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", "world");
    colValue.put("sender", "alice");

    assertTrue(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyValueNull() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("message", null);
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyMissing() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "world", Optional.of("message"));

    Map<String, String> colValue = new HashMap<>();
    colValue.put("sender", "alice");

    assertFalse(filter.apply(colValue));
  }

  @Test
  public void testSuffixMapKeyValueNonString() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    ColumnSuffixFilter filter =
        new ColumnSuffixFilter(mapDescriptor, "45", Optional.of("code"));

    Map<String, Object> colValue = new HashMap<>();
    colValue.put("code", 12345);
    colValue.put("name", "test");

    assertThrows(IllegalArgumentException.class, () -> filter.apply(colValue));
  }

  @Test
  public void testSuffixMapWithoutKey() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
    assertThrows(IllegalArgumentException.class, () -> new ColumnSuffixFilter(mapDescriptor, "world"));
  }

  @Test
  public void testSuffixMapEmptySuffix() {
    var mapDescriptor = map(Type.BYTE_ARRAY);
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
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnContainsFilter containsFilter = new ColumnContainsFilter(primitiveDescriptor, "");
    assertTrue(containsFilter.apply("test"));
    assertTrue(containsFilter.apply(""));

    ColumnPrefixFilter prefixFilter = new ColumnPrefixFilter(primitiveDescriptor, "");
    assertTrue(prefixFilter.apply("test"));

    ColumnSuffixFilter suffixFilter = new ColumnSuffixFilter(primitiveDescriptor, "");
    assertTrue(suffixFilter.apply("test"));
  }
}

