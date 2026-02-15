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
}
