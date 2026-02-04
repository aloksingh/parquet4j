package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ListMetadata;
import io.github.aloksingh.parquet.model.MapMetadata;
import java.util.Arrays;
import java.util.List;
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
    ColumnContainsFilter filter = new ColumnContainsFilter("world");

    assertTrue(filter.apply(primitiveDescriptor, "hello world"));
    assertTrue(filter.apply(primitiveDescriptor, "world"));
    assertFalse(filter.apply(primitiveDescriptor, "hello"));
  }

  @Test
  public void testContainsStringCaseSensitive() {
    ColumnContainsFilter filter = new ColumnContainsFilter("World");

    assertTrue(filter.apply(primitiveDescriptor, "Hello World"));
    assertFalse(filter.apply(primitiveDescriptor, "hello world"));
  }

  @Test
  public void testContainsWithNullValue() {
    ColumnContainsFilter filter = new ColumnContainsFilter("test");

    assertFalse(filter.apply(primitiveDescriptor, null));
  }

  @Test
  public void testContainsWithNonString() {
    ColumnContainsFilter filter = new ColumnContainsFilter("test");

    assertFalse(filter.apply(primitiveDescriptor, 123));
  }

  // Contains Tests for Lists
  @Test
  public void testContainsInList() {
    ColumnContainsFilter filter = new ColumnContainsFilter("apple");

    List<String> list = Arrays.asList("apple", "banana", "cherry");
    assertTrue(filter.apply(listDescriptor, list));

    List<String> noMatch = Arrays.asList("banana", "cherry");
    assertFalse(filter.apply(listDescriptor, noMatch));
  }

  @Test
  public void testContainsInListWithIntegers() {
    ColumnContainsFilter filter = new ColumnContainsFilter(42);

    List<Integer> list = Arrays.asList(10, 20, 42, 50);
    assertTrue(filter.apply(listDescriptor, list));

    List<Integer> noMatch = Arrays.asList(10, 20, 50);
    assertFalse(filter.apply(listDescriptor, noMatch));
  }

  @Test
  public void testContainsOnMap() {
    ColumnContainsFilter filter = new ColumnContainsFilter("test");

    // Contains is not applicable to maps
    assertFalse(filter.apply(mapDescriptor, new java.util.HashMap<>()));
  }

  // Prefix Tests
  @Test
  public void testPrefix() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter("hello");

    assertTrue(filter.apply(primitiveDescriptor, "hello world"));
    assertTrue(filter.apply(primitiveDescriptor, "hello"));
    assertFalse(filter.apply(primitiveDescriptor, "world hello"));
  }

  @Test
  public void testPrefixCaseSensitive() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter("Hello");

    assertTrue(filter.apply(primitiveDescriptor, "Hello World"));
    assertFalse(filter.apply(primitiveDescriptor, "hello world"));
  }

  @Test
  public void testPrefixWithNullValue() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter("test");

    assertFalse(filter.apply(primitiveDescriptor, null));
  }

  @Test
  public void testPrefixWithNullMatch() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter(null);

    assertFalse(filter.apply(primitiveDescriptor, "test"));
  }

  @Test
  public void testPrefixWithNonString() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter("test");

    assertFalse(filter.apply(primitiveDescriptor, 123));
  }

  @Test
  public void testPrefixWithComplexType() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter("test");

    assertFalse(filter.apply(listDescriptor, Arrays.asList("test")));
  }

  @Test
  public void testPrefixEmptyString() {
    ColumnPrefixFilter filter = new ColumnPrefixFilter("");

    assertTrue(filter.apply(primitiveDescriptor, "any string"));
    assertTrue(filter.apply(primitiveDescriptor, ""));
  }

  // Suffix Tests
  @Test
  public void testSuffix() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter("world");

    assertTrue(filter.apply(primitiveDescriptor, "hello world"));
    assertTrue(filter.apply(primitiveDescriptor, "world"));
    assertFalse(filter.apply(primitiveDescriptor, "world hello"));
  }

  @Test
  public void testSuffixCaseSensitive() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter("World");

    assertTrue(filter.apply(primitiveDescriptor, "Hello World"));
    assertFalse(filter.apply(primitiveDescriptor, "hello world"));
  }

  @Test
  public void testSuffixWithNullValue() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter("test");

    assertFalse(filter.apply(primitiveDescriptor, null));
  }

  @Test
  public void testSuffixWithNullMatch() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter(null);

    assertFalse(filter.apply(primitiveDescriptor, "test"));
  }

  @Test
  public void testSuffixWithNonString() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter("test");

    assertFalse(filter.apply(primitiveDescriptor, 123));
  }

  @Test
  public void testSuffixWithComplexType() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter("test");

    assertFalse(filter.apply(mapDescriptor, new java.util.HashMap<>()));
  }

  @Test
  public void testSuffixEmptyString() {
    ColumnSuffixFilter filter = new ColumnSuffixFilter("");

    assertTrue(filter.apply(primitiveDescriptor, "any string"));
    assertTrue(filter.apply(primitiveDescriptor, ""));
  }

  // Edge cases for all string filters
  @Test
  public void testEmptyStringOperations() {
    ColumnContainsFilter containsFilter = new ColumnContainsFilter("");
    assertTrue(containsFilter.apply(primitiveDescriptor, "test"));
    assertTrue(containsFilter.apply(primitiveDescriptor, ""));

    ColumnPrefixFilter prefixFilter = new ColumnPrefixFilter("");
    assertTrue(prefixFilter.apply(primitiveDescriptor, "test"));

    ColumnSuffixFilter suffixFilter = new ColumnSuffixFilter("");
    assertTrue(suffixFilter.apply(primitiveDescriptor, "test"));
  }
}
