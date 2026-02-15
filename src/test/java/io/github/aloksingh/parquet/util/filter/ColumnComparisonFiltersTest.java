package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, 10);

    assertTrue(filter.apply(5));
    assertFalse(filter.apply(10));
    assertFalse(filter.apply(15));
  }

  @Test
  public void testLessThanWithStrings() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, "middle");

    assertTrue(filter.apply("apple"));
    assertFalse(filter.apply("middle"));
    assertFalse(filter.apply("zebra"));
  }

  @Test
  public void testLessThanWithNullValue() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(null));
  }

  @Test
  public void testLessThanWithNonComparable() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(new Object()));
  }

  @Test
  public void testLessThanWithComplexType() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(mapDescriptor, 10);

    assertFalse(filter.apply(5));
  }

  // LessThanOrEqual Tests
  @Test
  public void testLessThanOrEqualWithIntegers() {
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(primitiveDescriptor, 10);

    assertTrue(filter.apply(5));
    assertTrue(filter.apply(10));
    assertFalse(filter.apply(15));
  }

  @Test
  public void testLessThanOrEqualWithStrings() {
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(primitiveDescriptor, "middle");

    assertTrue(filter.apply("apple"));
    assertTrue(filter.apply("middle"));
    assertFalse(filter.apply("zebra"));
  }

  @Test
  public void testLessThanOrEqualWithNullValue() {
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(null));
  }

  // GreaterThan Tests
  @Test
  public void testGreaterThanWithIntegers() {
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(5));
    assertFalse(filter.apply(10));
    assertTrue(filter.apply(15));
  }

  @Test
  public void testGreaterThanWithDoubles() {
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(primitiveDescriptor, 10.5);

    assertFalse(filter.apply(10.0));
    assertFalse(filter.apply(10.5));
    assertTrue(filter.apply(11.0));
  }

  @Test
  public void testGreaterThanWithStrings() {
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(primitiveDescriptor, "middle");

    assertFalse(filter.apply("apple"));
    assertFalse(filter.apply("middle"));
    assertTrue(filter.apply("zebra"));
  }

  @Test
  public void testGreaterThanWithNullValue() {
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(null));
  }

  // GreaterThanOrEqual Tests
  @Test
  public void testGreaterThanOrEqualWithIntegers() {
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(5));
    assertTrue(filter.apply(10));
    assertTrue(filter.apply(15));
  }

  @Test
  public void testGreaterThanOrEqualWithStrings() {
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(primitiveDescriptor, "middle");

    assertFalse(filter.apply("apple"));
    assertTrue(filter.apply("middle"));
    assertTrue(filter.apply("zebra"));
  }

  @Test
  public void testGreaterThanOrEqualWithNullValue() {
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(null));
  }

  // Mixed type comparison tests
  @Test
  public void testMixedTypesThrowException() {
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, 10);

    // String vs Integer comparison should return false (ClassCastException caught)
    assertFalse(filter.apply("not a number"));
  }

  @Test
  public void testComparisonWithLong() {
    ColumnLessThanFilter ltFilter = new ColumnLessThanFilter(primitiveDescriptor, 10L);
    assertTrue(ltFilter.apply(5L));
    assertFalse(ltFilter.apply(15L));

    ColumnGreaterThanFilter gtFilter = new ColumnGreaterThanFilter(primitiveDescriptor, 10L);
    assertFalse(gtFilter.apply(5L));
    assertTrue(gtFilter.apply(15L));
  }
}
