package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ColumnFilterSetTest {

  private static final LogicalColumnDescriptor PRIMITIVE_DESCRIPTOR =
      new LogicalColumnDescriptor("col", LogicalType.PRIMITIVE, null, null);

  // ========== All (AND) Logic Tests ==========

  @Test
  public void testAllWithEmptyFilters() {
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All);
    // With All logic and no filters, should return true
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "any value"));
  }

  @Test
  public void testAllWithSingleFilterMatching() {
    ColumnFilter filter = new ColumnEqualFilter("test");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All, filter);

    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "test"));
  }

  @Test
  public void testAllWithSingleFilterNotMatching() {
    ColumnFilter filter = new ColumnEqualFilter("test");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All, filter);

    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, "other"));
  }

  @Test
  public void testAllWithMultipleFiltersAllMatch() {
    ColumnFilter filter1 = new ColumnGreaterThanFilter(10);
    ColumnFilter filter2 = new ColumnLessThanFilter(20);
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All, filter1, filter2);

    // Value 15 is > 10 AND < 20
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, 15));
  }

  @Test
  public void testAllWithMultipleFiltersOneDoesNotMatch() {
    ColumnFilter filter1 = new ColumnGreaterThanFilter(10);
    ColumnFilter filter2 = new ColumnLessThanFilter(20);
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All, filter1, filter2);

    // Value 25 is > 10 but NOT < 20
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, 25));
  }

  @Test
  public void testAllWithMultipleFiltersNoneMatch() {
    ColumnFilter filter1 = new ColumnEqualFilter("foo");
    ColumnFilter filter2 = new ColumnEqualFilter("bar");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All, filter1, filter2);

    // Value "baz" doesn't equal "foo" or "bar"
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, "baz"));
  }

  @Test
  public void testAllWithThreeFiltersAllMatch() {
    ColumnFilter filter1 = new ColumnGreaterThanFilter(10);
    ColumnFilter filter2 = new ColumnLessThanFilter(30);
    ColumnFilter filter3 = new ColumnNotEqualFilter(15);
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All, filter1, filter2, filter3);

    // Value 20 is > 10 AND < 30 AND != 15
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, 20));
  }

  @Test
  public void testAllWithThreeFiltersOneDoesNotMatch() {
    ColumnFilter filter1 = new ColumnGreaterThanFilter(10);
    ColumnFilter filter2 = new ColumnLessThanFilter(30);
    ColumnFilter filter3 = new ColumnNotEqualFilter(15);
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All, filter1, filter2, filter3);

    // Value 15 is > 10 AND < 30 but equals 15
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, 15));
  }

  // ========== Any (OR) Logic Tests ==========

  @Test
  public void testAnyWithEmptyFilters() {
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.Any);
    // With Any logic and no filters, should return false
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, "any value"));
  }

  @Test
  public void testAnyWithSingleFilterMatching() {
    ColumnFilter filter = new ColumnEqualFilter("test");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.Any, filter);

    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "test"));
  }

  @Test
  public void testAnyWithSingleFilterNotMatching() {
    ColumnFilter filter = new ColumnEqualFilter("test");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.Any, filter);

    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, "other"));
  }

  @Test
  public void testAnyWithMultipleFiltersFirstMatches() {
    ColumnFilter filter1 = new ColumnEqualFilter("test");
    ColumnFilter filter2 = new ColumnEqualFilter("foo");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.Any, filter1, filter2);

    // Value "test" matches first filter
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "test"));
  }

  @Test
  public void testAnyWithMultipleFiltersSecondMatches() {
    ColumnFilter filter1 = new ColumnEqualFilter("test");
    ColumnFilter filter2 = new ColumnEqualFilter("foo");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.Any, filter1, filter2);

    // Value "foo" matches second filter
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "foo"));
  }

  @Test
  public void testAnyWithMultipleFiltersNoneMatch() {
    ColumnFilter filter1 = new ColumnEqualFilter("test");
    ColumnFilter filter2 = new ColumnEqualFilter("foo");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.Any, filter1, filter2);

    // Value "bar" matches neither filter
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, "bar"));
  }

  @Test
  public void testAnyWithMultipleFiltersAllMatch() {
    ColumnFilter filter1 = new ColumnGreaterThanFilter(10);
    ColumnFilter filter2 = new ColumnLessThanFilter(20);
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.Any, filter1, filter2);

    // Value 15 matches both filters (> 10 OR < 20)
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, 15));
  }

  @Test
  public void testAnyWithThreeFiltersMiddleMatches() {
    ColumnFilter filter1 = new ColumnEqualFilter("foo");
    ColumnFilter filter2 = new ColumnEqualFilter("bar");
    ColumnFilter filter3 = new ColumnEqualFilter("baz");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.Any, filter1, filter2, filter3);

    // Value "bar" matches second filter
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "bar"));
  }

  // ========== Constructor Variant Tests ==========

  @Test
  public void testListConstructor() {
    List<ColumnFilter> filters = Arrays.asList(
        new ColumnGreaterThanFilter(10),
        new ColumnLessThanFilter(20)
    );
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All, filters);

    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, 15));
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, 25));
  }

  @Test
  public void testVarargsConstructor() {
    ColumnFilterSet filterSet = new ColumnFilterSet(
        FilterJoinType.Any,
        new ColumnEqualFilter("foo"),
        new ColumnEqualFilter("bar")
    );

    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "foo"));
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "bar"));
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, "baz"));
  }

  // ========== Nested ColumnFilterSet Tests ==========

  @Test
  public void testNestedFilterSetAllContainingAll() {
    // (A > 10 AND A < 20) AND (A != 12 AND A != 13)
    ColumnFilterSet inner1 = new ColumnFilterSet(
        FilterJoinType.All,
        new ColumnGreaterThanFilter(10),
        new ColumnLessThanFilter(20)
    );
    ColumnFilterSet inner2 = new ColumnFilterSet(
        FilterJoinType.All,
        new ColumnNotEqualFilter(12),
        new ColumnNotEqualFilter(13)
    );
    ColumnFilterSet outer = new ColumnFilterSet(FilterJoinType.All, inner1, inner2);

    assertTrue(outer.apply(PRIMITIVE_DESCRIPTOR, 15));  // 15 is in range [11-19] and != 12,13
    assertFalse(outer.apply(PRIMITIVE_DESCRIPTOR, 12)); // 12 equals excluded value
    assertFalse(outer.apply(PRIMITIVE_DESCRIPTOR, 25)); // 25 is out of range
  }

  @Test
  public void testNestedFilterSetAnyContainingAny() {
    // (A == "foo" OR A == "bar") OR (A == "baz" OR A == "qux")
    ColumnFilterSet inner1 = new ColumnFilterSet(
        FilterJoinType.Any,
        new ColumnEqualFilter("foo"),
        new ColumnEqualFilter("bar")
    );
    ColumnFilterSet inner2 = new ColumnFilterSet(
        FilterJoinType.Any,
        new ColumnEqualFilter("baz"),
        new ColumnEqualFilter("qux")
    );
    ColumnFilterSet outer = new ColumnFilterSet(FilterJoinType.Any, inner1, inner2);

    assertTrue(outer.apply(PRIMITIVE_DESCRIPTOR, "foo"));
    assertTrue(outer.apply(PRIMITIVE_DESCRIPTOR, "bar"));
    assertTrue(outer.apply(PRIMITIVE_DESCRIPTOR, "baz"));
    assertTrue(outer.apply(PRIMITIVE_DESCRIPTOR, "qux"));
    assertFalse(outer.apply(PRIMITIVE_DESCRIPTOR, "other"));
  }

  @Test
  public void testNestedFilterSetComplexCombination() {
    // (A > 10 AND A < 20) OR (A > 50 AND A < 60)
    ColumnFilterSet range1 = new ColumnFilterSet(
        FilterJoinType.All,
        new ColumnGreaterThanFilter(10),
        new ColumnLessThanFilter(20)
    );
    ColumnFilterSet range2 = new ColumnFilterSet(
        FilterJoinType.All,
        new ColumnGreaterThanFilter(50),
        new ColumnLessThanFilter(60)
    );
    ColumnFilterSet outer = new ColumnFilterSet(FilterJoinType.Any, range1, range2);

    assertTrue(outer.apply(PRIMITIVE_DESCRIPTOR, 15));  // In first range
    assertTrue(outer.apply(PRIMITIVE_DESCRIPTOR, 55));  // In second range
    assertFalse(outer.apply(PRIMITIVE_DESCRIPTOR, 30)); // Not in either range
    assertFalse(outer.apply(PRIMITIVE_DESCRIPTOR, 70)); // Not in either range
  }

  @Test
  public void testNestedFilterSetThreeLevelsDeep() {
    // ((A > 10 AND A < 15) OR (A > 20 AND A < 25)) AND A != 22
    ColumnFilterSet range1 = new ColumnFilterSet(
        FilterJoinType.All,
        new ColumnGreaterThanFilter(10),
        new ColumnLessThanFilter(15)
    );
    ColumnFilterSet range2 = new ColumnFilterSet(
        FilterJoinType.All,
        new ColumnGreaterThanFilter(20),
        new ColumnLessThanFilter(25)
    );
    ColumnFilterSet anyRange = new ColumnFilterSet(FilterJoinType.Any, range1, range2);
    ColumnFilterSet outer = new ColumnFilterSet(
        FilterJoinType.All,
        anyRange,
        new ColumnNotEqualFilter(22)
    );

    assertTrue(outer.apply(PRIMITIVE_DESCRIPTOR, 12));  // In first range, != 22
    assertTrue(outer.apply(PRIMITIVE_DESCRIPTOR, 23));  // In second range, != 22
    assertFalse(outer.apply(PRIMITIVE_DESCRIPTOR, 22)); // In second range but equals 22
    assertFalse(outer.apply(PRIMITIVE_DESCRIPTOR, 30)); // Not in either range
  }

  // ========== Integration Tests with Different Value Types ==========

  @Test
  public void testWithStrings() {
    ColumnFilterSet filterSet = new ColumnFilterSet(
        FilterJoinType.All,
        new ColumnNotEqualFilter(""),
        new ColumnNotEqualFilter(null)
    );

    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "test"));
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, ""));
  }

  @Test
  public void testWithIntegers() {
    ColumnFilterSet filterSet = new ColumnFilterSet(
        FilterJoinType.Any,
        new ColumnLessThanFilter(0),
        new ColumnGreaterThanFilter(100)
    );

    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, -5));   // < 0
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, 150));  // > 100
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, 50));  // Neither
  }

  @Test
  public void testWithDoubles() {
    ColumnFilterSet filterSet = new ColumnFilterSet(
        FilterJoinType.All,
        new ColumnGreaterThanFilter(0.0),
        new ColumnLessThanFilter(1.0)
    );

    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, 0.5));
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, 1.5));
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, -0.5));
  }

  @Test
  public void testWithNullValues() {
    ColumnFilterSet filterSet = new ColumnFilterSet(
        FilterJoinType.Any,
        new ColumnEqualFilter("test"),
        new ColumnEqualFilter("foo")
    );

    // Null values typically don't match any filter
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, null));
  }

  // ========== Edge Cases ==========

  @Test
  public void testAllWithMixedMatchResults() {
    // Create a scenario where filters are evaluated in sequence
    ColumnFilter filter1 = new ColumnGreaterThanFilter(10);
    ColumnFilter filter2 = new ColumnEqualFilter(5); // Will fail
    ColumnFilter filter3 = new ColumnLessThanFilter(20);
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.All, filter1, filter2, filter3);

    // Value 15 is > 10 and < 20, but != 5, so All should fail
    assertFalse(filterSet.apply(PRIMITIVE_DESCRIPTOR, 15));
  }

  @Test
  public void testAnyShortCircuits() {
    // Testing that Any returns true as soon as first match is found
    ColumnFilter filter1 = new ColumnEqualFilter("test");
    ColumnFilter filter2 = new ColumnEqualFilter("foo");
    ColumnFilter filter3 = new ColumnEqualFilter("bar");
    ColumnFilterSet filterSet = new ColumnFilterSet(FilterJoinType.Any, filter1, filter2, filter3);

    // Value "test" should match first filter immediately
    assertTrue(filterSet.apply(PRIMITIVE_DESCRIPTOR, "test"));
  }

  @Test
  public void testComplexRealWorldScenario() {
    // Simulate a filter like: (status == "active" OR status == "pending") AND (priority > 5)
    // For this test, we'll use a simplified version with integers
    // (value == 1 OR value == 2) AND (value < 3)
    ColumnFilterSet statusFilter = new ColumnFilterSet(
        FilterJoinType.Any,
        new ColumnEqualFilter(1),
        new ColumnEqualFilter(2)
    );
    ColumnFilterSet combinedFilter = new ColumnFilterSet(
        FilterJoinType.All,
        statusFilter,
        new ColumnLessThanFilter(3)
    );

    assertTrue(combinedFilter.apply(PRIMITIVE_DESCRIPTOR, 1));  // 1 == 1 OR 1 == 2 (true), AND 1 < 3 (true)
    assertTrue(combinedFilter.apply(PRIMITIVE_DESCRIPTOR, 2));  // 2 == 1 OR 2 == 2 (true), AND 2 < 3 (true)
    assertFalse(combinedFilter.apply(PRIMITIVE_DESCRIPTOR, 3)); // 3 == 1 OR 3 == 2 (false)
    assertFalse(combinedFilter.apply(PRIMITIVE_DESCRIPTOR, 0)); // 0 == 1 OR 0 == 2 (false)
  }
}
