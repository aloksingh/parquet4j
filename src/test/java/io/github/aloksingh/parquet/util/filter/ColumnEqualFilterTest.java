package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ListMetadata;
import io.github.aloksingh.parquet.model.MapMetadata;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ColumnEqualFilterTest {

  @Test
  public void testPrimitiveStringEqual() {
    ColumnEqualFilter filter = new ColumnEqualFilter("test");
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);

    assertTrue(filter.apply(descriptor, "test"));
    assertFalse(filter.apply(descriptor, "other"));
  }

  @Test
  public void testPrimitiveIntegerEqual() {
    ColumnEqualFilter filter = new ColumnEqualFilter(42);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);

    assertTrue(filter.apply(descriptor, 42));
    assertFalse(filter.apply(descriptor, 43));
  }

  @Test
  public void testPrimitiveDoubleEqual() {
    ColumnEqualFilter filter = new ColumnEqualFilter(3.14);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);

    assertTrue(filter.apply(descriptor, 3.14));
    assertFalse(filter.apply(descriptor, 3.15));
  }

  @Test
  public void testPrimitiveNullValue() {
    ColumnEqualFilter filter = new ColumnEqualFilter("test");
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);

    assertFalse(filter.apply(descriptor, null));
  }

  @Test
  public void testListEqual() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    ColumnEqualFilter filter = new ColumnEqualFilter(matchList);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertTrue(filter.apply(descriptor, valueList));
  }

  @Test
  public void testListNotEqual() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    ColumnEqualFilter filter = new ColumnEqualFilter(matchList);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);

    List<String> valueList = Arrays.asList("a", "b", "d");
    assertFalse(filter.apply(descriptor, valueList));
  }

  @Test
  public void testListDifferentSize() {
    List<String> matchList = Arrays.asList("a", "b");
    ColumnEqualFilter filter = new ColumnEqualFilter(matchList);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertFalse(filter.apply(descriptor, valueList));
  }

  @Test
  public void testMapEqual() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    ColumnEqualFilter filter = new ColumnEqualFilter(matchMap);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 2);

    assertTrue(filter.apply(descriptor, valueMap));
  }

  @Test
  public void testMapNotEqual() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    ColumnEqualFilter filter = new ColumnEqualFilter(matchMap);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 3);

    assertFalse(filter.apply(descriptor, valueMap));
  }

  @Test
  public void testMapDifferentSize() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);

    ColumnEqualFilter filter = new ColumnEqualFilter(matchMap);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 2);

    assertFalse(filter.apply(descriptor, valueMap));
  }

  @Test
  public void testMapMissingKey() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    ColumnEqualFilter filter = new ColumnEqualFilter(matchMap);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key3", 2);

    assertFalse(filter.apply(descriptor, valueMap));
  }
}
