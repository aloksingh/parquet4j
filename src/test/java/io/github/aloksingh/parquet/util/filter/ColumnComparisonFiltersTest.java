package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.MapMetadata;
import org.junit.jupiter.api.Test;

public class ColumnComparisonFiltersTest {

  private final LogicalColumnDescriptor primitiveDescriptor =
      new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);
  private final LogicalColumnDescriptor mapDescriptor =
      new LogicalColumnDescriptor("col", LogicalType.MAP, (MapMetadata) null);

  // LessThan Tests
  @Test
  public void testLessThanWithIntegers() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(10);

    assertTrue(filter.apply(primitiveDescriptor, 5));
    assertFalse(filter.apply(primitiveDescriptor, 10));
    assertFalse(filter.apply(primitiveDescriptor, 15));
  }

  @Test
  public void testLessThanWithStrings() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter("middle");

    assertTrue(filter.apply(primitiveDescriptor, "apple"));
    assertFalse(filter.apply(primitiveDescriptor, "middle"));
    assertFalse(filter.apply(primitiveDescriptor, "zebra"));
  }

  @Test
  public void testLessThanWithNullValue() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(10);

    assertFalse(filter.apply(primitiveDescriptor, null));
  }

  @Test
  public void testLessThanWithNonComparable() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(10);

    assertFalse(filter.apply(primitiveDescriptor, new Object()));
  }

  @Test
  public void testLessThanWithComplexType() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(10);

    assertFalse(filter.apply(mapDescriptor, 5));
  }

  // LessThanOrEqual Tests
  @Test
  public void testLessThanOrEqualWithIntegers() {
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(10);

    assertTrue(filter.apply(primitiveDescriptor, 5));
    assertTrue(filter.apply(primitiveDescriptor, 10));
    assertFalse(filter.apply(primitiveDescriptor, 15));
  }

  @Test
  public void testLessThanOrEqualWithStrings() {
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter("middle");

    assertTrue(filter.apply(primitiveDescriptor, "apple"));
    assertTrue(filter.apply(primitiveDescriptor, "middle"));
    assertFalse(filter.apply(primitiveDescriptor, "zebra"));
  }

  @Test
  public void testLessThanOrEqualWithNullValue() {
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(10);

    assertFalse(filter.apply(primitiveDescriptor, null));
  }

  // GreaterThan Tests
  @Test
  public void testGreaterThanWithIntegers() {
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(10);

    assertFalse(filter.apply(primitiveDescriptor, 5));
    assertFalse(filter.apply(primitiveDescriptor, 10));
    assertTrue(filter.apply(primitiveDescriptor, 15));
  }

  @Test
  public void testGreaterThanWithDoubles() {
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(10.5);

    assertFalse(filter.apply(primitiveDescriptor, 10.0));
    assertFalse(filter.apply(primitiveDescriptor, 10.5));
    assertTrue(filter.apply(primitiveDescriptor, 11.0));
  }

  @Test
  public void testGreaterThanWithStrings() {
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter("middle");

    assertFalse(filter.apply(primitiveDescriptor, "apple"));
    assertFalse(filter.apply(primitiveDescriptor, "middle"));
    assertTrue(filter.apply(primitiveDescriptor, "zebra"));
  }

  @Test
  public void testGreaterThanWithNullValue() {
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(10);

    assertFalse(filter.apply(primitiveDescriptor, null));
  }

  // GreaterThanOrEqual Tests
  @Test
  public void testGreaterThanOrEqualWithIntegers() {
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(10);

    assertFalse(filter.apply(primitiveDescriptor, 5));
    assertTrue(filter.apply(primitiveDescriptor, 10));
    assertTrue(filter.apply(primitiveDescriptor, 15));
  }

  @Test
  public void testGreaterThanOrEqualWithStrings() {
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter("middle");

    assertFalse(filter.apply(primitiveDescriptor, "apple"));
    assertTrue(filter.apply(primitiveDescriptor, "middle"));
    assertTrue(filter.apply(primitiveDescriptor, "zebra"));
  }

  @Test
  public void testGreaterThanOrEqualWithNullValue() {
    ColumnGreaterThanOrEqualFilter filter = new ColumnGreaterThanOrEqualFilter(10);

    assertFalse(filter.apply(primitiveDescriptor, null));
  }

  // Mixed type comparison tests
  @Test
  public void testMixedTypesThrowException() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(10);

    // String vs Integer comparison should return false (ClassCastException caught)
    assertFalse(filter.apply(primitiveDescriptor, "not a number"));
  }

  @Test
  public void testComparisonWithLong() {
    ColumnLessThanFilter ltFilter = new ColumnLessThanFilter(10L);
    assertTrue(ltFilter.apply(primitiveDescriptor, 5L));
    assertFalse(ltFilter.apply(primitiveDescriptor, 15L));

    ColumnGreaterThanFilter gtFilter = new ColumnGreaterThanFilter(10L);
    assertFalse(gtFilter.apply(primitiveDescriptor, 5L));
    assertTrue(gtFilter.apply(primitiveDescriptor, 15L));
  }
}
