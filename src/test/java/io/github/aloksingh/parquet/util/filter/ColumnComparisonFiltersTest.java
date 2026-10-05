package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.github.aloksingh.parquet.util.filter.FilterTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.MapMetadata;
import io.github.aloksingh.parquet.model.Type;
import org.junit.jupiter.api.Test;

public class ColumnComparisonFiltersTest {


  // LessThan Tests
  @Test
  public void testLessThanWithIntegers() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, 10);

    assertTrue(filter.apply(5));
    assertFalse(filter.apply(10));
    assertFalse(filter.apply(15));
  }

  @Test
  public void testLessThanWithStrings() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, "middle");

    assertTrue(filter.apply("apple"));
    assertFalse(filter.apply("middle"));
    assertFalse(filter.apply("zebra"));
  }

  @Test
  public void testLessThanWithNullValue() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(null));
  }

  @Test
  public void testLessThanWithNonComparable() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, 10);

    assertThrows(IllegalArgumentException.class, () -> filter.apply(new Object()));
  }

  @Test
  public void testLessThanWithComplexType() {
    var mapDescriptor = map(Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> new ColumnLessThanFilter(mapDescriptor, 10));
  }

  // LessThanOrEqual Tests
  @Test
  public void testLessThanOrEqualWithIntegers() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(primitiveDescriptor, 10);

    assertTrue(filter.apply(5));
    assertTrue(filter.apply(10));
    assertFalse(filter.apply(15));
  }

  @Test
  public void testLessThanOrEqualWithStrings() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnLessThanOrEqualFilter filter =
        new ColumnLessThanOrEqualFilter(primitiveDescriptor, "middle");

    assertTrue(filter.apply("apple"));
    assertTrue(filter.apply("middle"));
    assertFalse(filter.apply("zebra"));
  }

  @Test
  public void testLessThanOrEqualWithNullValue() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnLessThanOrEqualFilter filter = new ColumnLessThanOrEqualFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(null));
  }

  // GreaterThan Tests
  @Test
  public void testGreaterThanWithIntegers() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(5));
    assertFalse(filter.apply(10));
    assertTrue(filter.apply(15));
  }

  @Test
  public void testGreaterThanWithDoubles() {
    var primitiveDescriptor = primitive("col", Type.DOUBLE);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(primitiveDescriptor, 10.5);

    assertFalse(filter.apply(10.0));
    assertFalse(filter.apply(10.5));
    assertTrue(filter.apply(11.0));
  }

  @Test
  public void testGreaterThanWithStrings() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(primitiveDescriptor, "middle");

    assertFalse(filter.apply("apple"));
    assertFalse(filter.apply("middle"));
    assertTrue(filter.apply("zebra"));
  }

  @Test
  public void testGreaterThanWithNullValue() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnGreaterThanFilter filter = new ColumnGreaterThanFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(null));
  }

  // GreaterThanOrEqual Tests
  @Test
  public void testGreaterThanOrEqualWithIntegers() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(5));
    assertTrue(filter.apply(10));
    assertTrue(filter.apply(15));
  }

  @Test
  public void testGreaterThanOrEqualWithStrings() {
    var primitiveDescriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(primitiveDescriptor, "middle");

    assertFalse(filter.apply("apple"));
    assertTrue(filter.apply("middle"));
    assertTrue(filter.apply("zebra"));
  }

  @Test
  public void testGreaterThanOrEqualWithNullValue() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnGreaterThanOrEqualFilter filter =
        new ColumnGreaterThanOrEqualFilter(primitiveDescriptor, 10);

    assertFalse(filter.apply(null));
  }

  // Mixed type comparison tests
  @Test
  public void testMixedTypesThrowException() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnLessThanFilter filter = new ColumnLessThanFilter(primitiveDescriptor, 10);

    // String vs Integer comparison must throw before being mistaken for a non-match
    assertThrows(IllegalArgumentException.class, () -> filter.apply("not a number"));
  }

  @Test
  public void testComparisonWithLong() {
    var primitiveDescriptor = primitive("col", Type.INT64);
    ColumnLessThanFilter ltFilter = new ColumnLessThanFilter(primitiveDescriptor, 10L);
    assertTrue(ltFilter.apply(5L));
    assertFalse(ltFilter.apply(15L));

    ColumnGreaterThanFilter gtFilter = new ColumnGreaterThanFilter(primitiveDescriptor, 10L);
    assertFalse(gtFilter.apply(5L));
    assertTrue(gtFilter.apply(15L));
  }
}

