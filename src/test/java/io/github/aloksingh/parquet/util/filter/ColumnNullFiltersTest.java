package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ListMetadata;
import io.github.aloksingh.parquet.model.MapMetadata;
import java.util.Arrays;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class ColumnNullFiltersTest {

  private final LogicalColumnDescriptor primitiveDescriptor =
      new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
  private final LogicalColumnDescriptor listDescriptor =
      new LogicalColumnDescriptor("col", LogicalType.LIST, (ListMetadata) null);
  private final LogicalColumnDescriptor mapDescriptor =
      new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);

  // IsNull Tests
  @Test
  public void testIsNullWithNullValue() {
    ColumnIsNullFilter filter = new ColumnIsNullFilter();

    assertTrue(filter.apply(primitiveDescriptor, null));
  }

  @Test
  public void testIsNullWithNonNullPrimitive() {
    ColumnIsNullFilter filter = new ColumnIsNullFilter();

    assertFalse(filter.apply(primitiveDescriptor, "test"));
    assertFalse(filter.apply(primitiveDescriptor, 42));
    assertFalse(filter.apply(primitiveDescriptor, 0));
    assertFalse(filter.apply(primitiveDescriptor, ""));
  }

  @Test
  public void testIsNullWithList() {
    ColumnIsNullFilter filter = new ColumnIsNullFilter();

    assertFalse(filter.apply(listDescriptor, Arrays.asList("a", "b")));
    assertFalse(filter.apply(listDescriptor, Arrays.asList()));
    assertTrue(filter.apply(listDescriptor, null));
  }

  @Test
  public void testIsNullWithMap() {
    ColumnIsNullFilter filter = new ColumnIsNullFilter();

    assertFalse(filter.apply(mapDescriptor, new HashMap<>()));
    assertTrue(filter.apply(mapDescriptor, null));
  }

  // IsNotNull Tests
  @Test
  public void testIsNotNullWithNullValue() {
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter();

    assertFalse(filter.apply(primitiveDescriptor, null));
  }

  @Test
  public void testIsNotNullWithNonNullPrimitive() {
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter();

    assertTrue(filter.apply(primitiveDescriptor, "test"));
    assertTrue(filter.apply(primitiveDescriptor, 42));
    assertTrue(filter.apply(primitiveDescriptor, 0));
    assertTrue(filter.apply(primitiveDescriptor, ""));
    assertTrue(filter.apply(primitiveDescriptor, false));
  }

  @Test
  public void testIsNotNullWithList() {
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter();

    assertTrue(filter.apply(listDescriptor, Arrays.asList("a", "b")));
    assertTrue(filter.apply(listDescriptor, Arrays.asList()));
    assertFalse(filter.apply(listDescriptor, null));
  }

  @Test
  public void testIsNotNullWithMap() {
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter();

    assertTrue(filter.apply(mapDescriptor, new HashMap<>()));
    assertFalse(filter.apply(mapDescriptor, null));
  }

  @Test
  public void testIsNotNullWithZeroValues() {
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter();

    // Zero is not null
    assertTrue(filter.apply(primitiveDescriptor, 0));
    assertTrue(filter.apply(primitiveDescriptor, 0L));
    assertTrue(filter.apply(primitiveDescriptor, 0.0));
    assertTrue(filter.apply(primitiveDescriptor, 0.0f));
  }

  @Test
  public void testIsNullWithZeroValues() {
    ColumnIsNullFilter filter = new ColumnIsNullFilter();

    // Zero is not null
    assertFalse(filter.apply(primitiveDescriptor, 0));
    assertFalse(filter.apply(primitiveDescriptor, 0L));
    assertFalse(filter.apply(primitiveDescriptor, 0.0));
    assertFalse(filter.apply(primitiveDescriptor, 0.0f));
  }

  @Test
  public void testNullFiltersIgnoreColumnType() {
    ColumnIsNullFilter isNullFilter = new ColumnIsNullFilter();
    ColumnIsNotNullFilter isNotNullFilter = new ColumnIsNotNullFilter();

    // Null filters should work consistently across all column types
    assertTrue(isNullFilter.apply(primitiveDescriptor, null));
    assertTrue(isNullFilter.apply(listDescriptor, null));
    assertTrue(isNullFilter.apply(mapDescriptor, null));

    assertFalse(isNotNullFilter.apply(primitiveDescriptor, null));
    assertFalse(isNotNullFilter.apply(listDescriptor, null));
    assertFalse(isNotNullFilter.apply(mapDescriptor, null));
  }
}
