package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class ColumnFiltersTest {

  private final ColumnFilters columnFilters = new ColumnFilters();

  @Test
  public void testCreateEqualFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.eq, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnEqualFilter);
  }

  @Test
  public void testCreateNotEqualFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.neq, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnNotEqualFilter);
  }

  @Test
  public void testCreateLessThanFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.lt, 10);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnLessThanFilter);
  }

  @Test
  public void testCreateLessThanFilterWithNonComparable() {
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(FilterOperator.lt, new Object());
    });
  }

  @Test
  public void testCreateLessThanOrEqualFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.lte, 10);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnLessThanOrEqualFilter);
  }

  @Test
  public void testCreateLessThanOrEqualFilterWithNonComparable() {
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(FilterOperator.lte, new Object());
    });
  }

  @Test
  public void testCreateGreaterThanFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.gt, 10);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnGreaterThanFilter);
  }

  @Test
  public void testCreateGreaterThanFilterWithNonComparable() {
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(FilterOperator.gt, new Object());
    });
  }

  @Test
  public void testCreateGreaterThanOrEqualFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.gte, 10);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnGreaterThanOrEqualFilter);
  }

  @Test
  public void testCreateGreaterThanOrEqualFilterWithNonComparable() {
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(FilterOperator.gte, new Object());
    });
  }

  @Test
  public void testCreateContainsFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.contains, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnContainsFilter);
  }

  @Test
  public void testCreatePrefixFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.prefix, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnPrefixFilter);
  }

  @Test
  public void testCreatePrefixFilterWithNonString() {
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(FilterOperator.prefix, 123);
    });
  }

  @Test
  public void testCreateSuffixFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.suffix, "test");
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnSuffixFilter);
  }

  @Test
  public void testCreateSuffixFilterWithNonString() {
    assertThrows(IllegalArgumentException.class, () -> {
      columnFilters.createFilter(FilterOperator.suffix, 123);
    });
  }

  @Test
  public void testCreateIsNullFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.isNull, null);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnIsNullFilter);
  }

  @Test
  public void testCreateIsNotNullFilter() {
    ColumnFilter filter = columnFilters.createFilter(FilterOperator.isNotNull, null);
    assertNotNull(filter);
    assertTrue(filter instanceof ColumnIsNotNullFilter);
  }
}
