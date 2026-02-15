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

public class ColumnNotEqualFilterTest {

  @Test
  public void testPrimitiveStringNotEqual() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(descriptor, "test");

    assertFalse(filter.apply("test"));
    assertTrue(filter.apply("other"));
  }

  @Test
  public void testPrimitiveIntegerNotEqual() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(descriptor, 42);

    assertFalse(filter.apply(42));
    assertTrue(filter.apply(43));
  }

  @Test
  public void testPrimitiveNullValueWithNonNullMatch() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(descriptor, "test");

    assertTrue(filter.apply(null));
  }

  @Test
  public void testPrimitiveNullValueWithNullMatch() {
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(descriptor, null);

    assertFalse(filter.apply(null));
  }

  @Test
  public void testListNotEqual() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(descriptor, matchList);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertFalse(filter.apply(valueList));

    List<String> differentList = Arrays.asList("a", "b", "d");
    assertTrue(filter.apply(differentList));
  }

  @Test
  public void testListDifferentSize() {
    List<String> matchList = Arrays.asList("a", "b");
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(descriptor, matchList);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertTrue(filter.apply(valueList));
  }

  @Test
  public void testMapNotEqual() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(descriptor, matchMap);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 2);

    assertFalse(filter.apply(valueMap));

    Map<String, Integer> differentMap = new HashMap<>();
    differentMap.put("key1", 1);
    differentMap.put("key2", 3);

    assertTrue(filter.apply(differentMap));
  }

  @Test
  public void testMapDifferentSize() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);

    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(descriptor, matchMap);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 2);

    assertTrue(filter.apply(valueMap));
  }
}
