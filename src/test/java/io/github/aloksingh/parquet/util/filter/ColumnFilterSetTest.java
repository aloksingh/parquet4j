package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.github.aloksingh.parquet.util.filter.FilterTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import java.util.Arrays;
import java.util.List;
import io.github.aloksingh.parquet.model.Type;
import org.junit.jupiter.api.Test;

public class ColumnFilterSetTest {


  // ========== All (AND) Logic Tests ==========

  @Test
  public void testAllWithEmptyFilters() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    ColumnFilterSet filterSet = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All);
    // With All logic and no filters, should return true
    assertTrue(filterSet.apply("any value"));
  }

  @Test
  public void testAllWithSingleFilterMatching() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "test");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, filter);

    assertTrue(filterSet.apply("test"));
  }

  @Test
  public void testAllWithSingleFilterNotMatching() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "test");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, filter);

    assertFalse(filterSet.apply("other"));
  }

  @Test
  public void testAllWithMultipleFiltersAllMatch() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    ColumnFilter filter1 = new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10);
    ColumnFilter filter2 = new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 20);
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, filter1, filter2);

    // Value 15 is > 10 AND < 20
    assertTrue(filterSet.apply(15));
  }

  @Test
  public void testAllWithMultipleFiltersOneDoesNotMatch() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    ColumnFilter filter1 = new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10);
    ColumnFilter filter2 = new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 20);
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, filter1, filter2);

    // Value 25 is > 10 but NOT < 20
    assertFalse(filterSet.apply(25));
  }

  @Test
  public void testAllWithMultipleFiltersNoneMatch() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter1 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "foo");
    ColumnFilter filter2 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "bar");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, filter1, filter2);

    // Value "baz" doesn't equal "foo" or "bar"
    assertFalse(filterSet.apply("baz"));
  }

  @Test
  public void testAllWithThreeFiltersAllMatch() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    ColumnFilter filter1 = new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10);
    ColumnFilter filter2 = new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 30);
    ColumnFilter filter3 = new ColumnNotEqualFilter(PRIMITIVE_DESCRIPTOR, 15);
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, filter1, filter2, filter3);

    // Value 20 is > 10 AND < 30 AND != 15
    assertTrue(filterSet.apply(20));
  }

  @Test
  public void testAllWithThreeFiltersOneDoesNotMatch() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    ColumnFilter filter1 = new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10);
    ColumnFilter filter2 = new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 30);
    ColumnFilter filter3 = new ColumnNotEqualFilter(PRIMITIVE_DESCRIPTOR, 15);
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, filter1, filter2, filter3);

    // Value 15 is > 10 AND < 30 but equals 15
    assertFalse(filterSet.apply(15));
  }

  // ========== Any (OR) Logic Tests ==========

  @Test
  public void testAnyWithEmptyFilters() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    ColumnFilterSet filterSet = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any);
    // With Any logic and no filters, should return false
    assertFalse(filterSet.apply("any value"));
  }

  @Test
  public void testAnyWithSingleFilterMatching() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "test");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, filter);

    assertTrue(filterSet.apply("test"));
  }

  @Test
  public void testAnyWithSingleFilterNotMatching() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "test");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, filter);

    assertFalse(filterSet.apply("other"));
  }

  @Test
  public void testAnyWithMultipleFiltersFirstMatches() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter1 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "test");
    ColumnFilter filter2 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "foo");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, filter1, filter2);

    // Value "test" matches first filter
    assertTrue(filterSet.apply("test"));
  }

  @Test
  public void testAnyWithMultipleFiltersSecondMatches() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter1 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "test");
    ColumnFilter filter2 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "foo");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, filter1, filter2);

    // Value "foo" matches second filter
    assertTrue(filterSet.apply("foo"));
  }

  @Test
  public void testAnyWithMultipleFiltersNoneMatch() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter1 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "test");
    ColumnFilter filter2 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "foo");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, filter1, filter2);

    // Value "bar" matches neither filter
    assertFalse(filterSet.apply("bar"));
  }

  @Test
  public void testAnyWithMultipleFiltersAllMatch() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    ColumnFilter filter1 = new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10);
    ColumnFilter filter2 = new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 20);
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, filter1, filter2);

    // Value 15 matches both filters (> 10 OR < 20)
    assertTrue(filterSet.apply(15));
  }

  @Test
  public void testAnyWithThreeFiltersMiddleMatches() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilter filter1 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "foo");
    ColumnFilter filter2 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "bar");
    ColumnFilter filter3 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "baz");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, filter1, filter2, filter3);

    // Value "bar" matches second filter
    assertTrue(filterSet.apply("bar"));
  }

  // ========== Constructor Variant Tests ==========

  @Test
  public void testListConstructor() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    List<ColumnFilter> filters = Arrays.asList(
        new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10),
        new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 20)
    );
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, filters);

    assertTrue(filterSet.apply(15));
    assertFalse(filterSet.apply(25));
  }

  @Test
  public void testVarargsConstructor() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilterSet filterSet = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.Any,
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "foo"),
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "bar")
    );

    assertTrue(filterSet.apply("foo"));
    assertTrue(filterSet.apply("bar"));
    assertFalse(filterSet.apply("baz"));
  }

  // ========== Nested ColumnFilterSet Tests ==========

  @Test
  public void testNestedFilterSetAllContainingAll() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    // (A > 10 AND A < 20) AND (A != 12 AND A != 13)
    ColumnFilterSet inner1 = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10),
        new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 20)
    );
    ColumnFilterSet inner2 = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        new ColumnNotEqualFilter(PRIMITIVE_DESCRIPTOR, 12),
        new ColumnNotEqualFilter(PRIMITIVE_DESCRIPTOR, 13)
    );
    ColumnFilterSet outer =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, inner1, inner2);

    assertTrue(outer.apply(15));  // 15 is in range [11-19] and != 12,13
    assertFalse(outer.apply(12)); // 12 equals excluded value
    assertFalse(outer.apply(25)); // 25 is out of range
  }

  @Test
  public void testNestedFilterSetAnyContainingAny() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    // (A == "foo" OR A == "bar") OR (A == "baz" OR A == "qux")
    ColumnFilterSet inner1 = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.Any,
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "foo"),
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "bar")
    );
    ColumnFilterSet inner2 = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.Any,
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "baz"),
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "qux")
    );
    ColumnFilterSet outer =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, inner1, inner2);

    assertTrue(outer.apply("foo"));
    assertTrue(outer.apply("bar"));
    assertTrue(outer.apply("baz"));
    assertTrue(outer.apply("qux"));
    assertFalse(outer.apply("other"));
  }

  @Test
  public void testNestedFilterSetComplexCombination() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    // (A > 10 AND A < 20) OR (A > 50 AND A < 60)
    ColumnFilterSet range1 = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10),
        new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 20)
    );
    ColumnFilterSet range2 = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 50),
        new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 60)
    );
    ColumnFilterSet outer =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, range1, range2);

    assertTrue(outer.apply(15));  // In first range
    assertTrue(outer.apply(55));  // In second range
    assertFalse(outer.apply(30)); // Not in either range
    assertFalse(outer.apply(70)); // Not in either range
  }

  @Test
  public void testNestedFilterSetThreeLevelsDeep() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    // ((A > 10 AND A < 15) OR (A > 20 AND A < 25)) AND A != 22
    ColumnFilterSet range1 = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10),
        new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 15)
    );
    ColumnFilterSet range2 = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 20),
        new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 25)
    );
    ColumnFilterSet anyRange =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, range1, range2);
    ColumnFilterSet outer = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        anyRange,
        new ColumnNotEqualFilter(PRIMITIVE_DESCRIPTOR, 22)
    );

    assertTrue(outer.apply(12));  // In first range, != 22
    assertTrue(outer.apply(23));  // In second range, != 22
    assertFalse(outer.apply(22)); // In second range but equals 22
    assertFalse(outer.apply(30)); // Not in either range
  }

  // ========== Integration Tests with Different Value Types ==========

  @Test
  public void testWithStrings() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilterSet filterSet = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        new ColumnNotEqualFilter(PRIMITIVE_DESCRIPTOR, ""),
        new ColumnNotEqualFilter(PRIMITIVE_DESCRIPTOR, null)
    );

    assertTrue(filterSet.apply("test"));
    assertFalse(filterSet.apply(""));
  }

  @Test
  public void testWithIntegers() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    ColumnFilterSet filterSet = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.Any,
        new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 0),
        new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 100)
    );

    assertTrue(filterSet.apply(-5));   // < 0
    assertTrue(filterSet.apply(150));  // > 100
    assertFalse(filterSet.apply(50));  // Neither
  }

  @Test
  public void testWithDoubles() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.DOUBLE);
    ColumnFilterSet filterSet = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 0.0),
        new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 1.0)
    );

    assertTrue(filterSet.apply(0.5));
    assertFalse(filterSet.apply(1.5));
    assertFalse(filterSet.apply(-0.5));
  }

  @Test
  public void testWithNullValues() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    ColumnFilterSet filterSet = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.Any,
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "test"),
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "foo")
    );

    // Null values typically don't match any filter
    assertFalse(filterSet.apply(null));
  }

  // ========== Edge Cases ==========

  @Test
  public void testAllWithMixedMatchResults() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    // Create a scenario where filters are evaluated in sequence
    ColumnFilter filter1 = new ColumnGreaterThanFilter(PRIMITIVE_DESCRIPTOR, 10);
    ColumnFilter filter2 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, 5); // Will fail
    ColumnFilter filter3 = new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 20);
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.All, filter1, filter2, filter3);

    // Value 15 is > 10 and < 20, but != 5, so All should fail
    assertFalse(filterSet.apply(15));
  }

  @Test
  public void testAnyShortCircuits() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.BYTE_ARRAY);
    // Testing that Any returns true as soon as first match is found
    ColumnFilter filter1 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "test");
    ColumnFilter filter2 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "foo");
    ColumnFilter filter3 = new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, "bar");
    ColumnFilterSet filterSet =
        new ColumnFilterSet(PRIMITIVE_DESCRIPTOR, FilterJoinType.Any, filter1, filter2, filter3);

    // Value "test" should match first filter immediately
    assertTrue(filterSet.apply("test"));
  }

  @Test
  public void testComplexRealWorldScenario() {
    var PRIMITIVE_DESCRIPTOR = primitive("col", Type.INT32);
    // Simulate a filter like: (status == "active" OR status == "pending") AND (priority > 5)
    // For this test, we'll use a simplified version with integers
    // (value == 1 OR value == 2) AND (value < 3)
    ColumnFilterSet statusFilter = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.Any,
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, 1),
        new ColumnEqualFilter(PRIMITIVE_DESCRIPTOR, 2)
    );
    ColumnFilterSet combinedFilter = new ColumnFilterSet(PRIMITIVE_DESCRIPTOR,
        FilterJoinType.All,
        statusFilter,
        new ColumnLessThanFilter(PRIMITIVE_DESCRIPTOR, 3)
    );

    assertTrue(combinedFilter.apply(1));  // 1 == 1 OR 1 == 2 (true), AND 1 < 3 (true)
    assertTrue(combinedFilter.apply(2));  // 2 == 1 OR 2 == 2 (true), AND 2 < 3 (true)
    assertFalse(combinedFilter.apply(3)); // 3 == 1 OR 3 == 2 (false)
    assertFalse(combinedFilter.apply(0)); // 0 == 1 OR 0 == 2 (false)
  }
}

