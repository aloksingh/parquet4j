package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static io.github.aloksingh.parquet.util.filter.FilterTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.Type;
import org.junit.jupiter.api.Test;

public class ColumnFiltersTest {

  private final ColumnFilters columnFilters = new ColumnFilters();

  @Test
  public void testCreateEqualFilter() {
    var descriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.eq, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnEqualFilter);
  }

  @Test
  public void testCreateNotEqualFilter() {
    var descriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.neq, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnNotEqualFilter);
  }

  @Test
  public void testCreateLessThanFilter() {
    var descriptor = primitive("col", Type.INT32);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.lt, 10);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnLessThanFilter);
  }

  @Test
  public void testCreateLessThanFilterWithNonComparable() {
    var descriptor = primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(descriptor, FilterOperator.lt, new Object());
    });
  }

  @Test
  public void testCreateLessThanOrEqualFilter() {
    var descriptor = primitive("col", Type.INT32);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.lte, 10);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnLessThanOrEqualFilter);
  }

  @Test
  public void testCreateLessThanOrEqualFilterWithNonComparable() {
    var descriptor = primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(descriptor, FilterOperator.lte, new Object());
    });
  }

  @Test
  public void testCreateGreaterThanFilter() {
    var descriptor = primitive("col", Type.INT32);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.gt, 10);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnGreaterThanFilter);
  }

  @Test
  public void testCreateGreaterThanFilterWithNonComparable() {
    var descriptor = primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(descriptor, FilterOperator.gt, new Object());
    });
  }

  @Test
  public void testCreateGreaterThanOrEqualFilter() {
    var descriptor = primitive("col", Type.INT32);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.gte, 10);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnGreaterThanOrEqualFilter);
  }

  @Test
  public void testCreateGreaterThanOrEqualFilterWithNonComparable() {
    var descriptor = primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(descriptor, FilterOperator.gte, new Object());
    });
  }

  @Test
  public void testCreateContainsFilter() {
    var descriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.contains, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnContainsFilter);
  }

  @Test
  public void testCreatePrefixFilter() {
    var descriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.prefix, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnPrefixFilter);
  }

  @Test
  public void testCreatePrefixFilterWithNonString() {
    var descriptor = primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(descriptor, FilterOperator.prefix, 123);
    });
  }

  @Test
  public void testCreateSuffixFilter() {
    var descriptor = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.suffix, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnSuffixFilter);
  }

  @Test
  public void testCreateSuffixFilterWithNonString() {
    var descriptor = primitive("col", Type.INT32);
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(descriptor, FilterOperator.suffix, 123);
    });
  }

  @Test
  public void testCreateIsNullFilter() {
    var descriptor = primitive("col", Type.INT32);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.isNull, null);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnIsNullFilter);
  }

  @Test
  public void testCreateIsNotNullFilter() {
    var descriptor = primitive("col", Type.INT32);
    ColumnFilter filter = columnFilters.createFilter(descriptor, FilterOperator.isNotNull, null);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnIsNotNullFilter);
  }
}

