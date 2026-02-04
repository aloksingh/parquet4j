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

public class ColumnNotEqualFilterTest {

  @Test
  public void testPrimitiveStringNotEqual() {
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter("test");
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);

    assertFalse(filter.apply(descriptor, "test"));
    assertTrue(filter.apply(descriptor, "other"));
  }

  @Test
  public void testPrimitiveIntegerNotEqual() {
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(42);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);

    assertFalse(filter.apply(descriptor, 42));
    assertTrue(filter.apply(descriptor, 43));
  }

  @Test
  public void testPrimitiveNullValueWithNonNullMatch() {
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter("test");
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);

    assertTrue(filter.apply(descriptor, null));
  }

  @Test
  public void testPrimitiveNullValueWithNullMatch() {
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(null);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);

    assertFalse(filter.apply(descriptor, null));
  }

  @Test
  public void testListNotEqual() {
    List<String> matchList = Arrays.asList("a", "b", "c");
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(matchList);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertFalse(filter.apply(descriptor, valueList));

    List<String> differentList = Arrays.asList("a", "b", "d");
    assertTrue(filter.apply(descriptor, differentList));
  }

  @Test
  public void testListDifferentSize() {
    List<String> matchList = Arrays.asList("a", "b");
    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(matchList);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);

    List<String> valueList = Arrays.asList("a", "b", "c");
    assertTrue(filter.apply(descriptor, valueList));
  }

  @Test
  public void testMapNotEqual() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);
    matchMap.put("key2", 2);

    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(matchMap);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 2);

    assertFalse(filter.apply(descriptor, valueMap));

    Map<String, Integer> differentMap = new HashMap<>();
    differentMap.put("key1", 1);
    differentMap.put("key2", 3);

    assertTrue(filter.apply(descriptor, differentMap));
  }

  @Test
  public void testMapDifferentSize() {
    Map<String, Integer> matchMap = new HashMap<>();
    matchMap.put("key1", 1);

    ColumnNotEqualFilter filter = new ColumnNotEqualFilter(matchMap);
    LogicalColumnDescriptor descriptor = new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);

    Map<String, Integer> valueMap = new HashMap<>();
    valueMap.put("key1", 1);
    valueMap.put("key2", 2);

    assertTrue(filter.apply(descriptor, valueMap));
  }
}
