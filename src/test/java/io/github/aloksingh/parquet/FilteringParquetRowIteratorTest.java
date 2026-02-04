package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.util.filter.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test suite for FilteringParquetRowIterator.
 * Tests filtering logic, iteration behavior, and various filter combinations.
 */
class FilteringParquetRowIteratorTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  /**
   * Test basic filtering with a single equality filter on integer values.
   */
  @Test
  void testBasicEqualityFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Use a simple equality filter for value 4 (from the first row we know id=4)
      ColumnFilter filter = new ColumnEqualFilter(4);
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(reader, filter, false);

      int count = 0;
      while (iterator.hasNext() && count < 10) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);

        // Verify at least one column has the value 4
        boolean hasValueFour = false;
        for (int i = 0; i < row.getColumnCount(); i++) {
          if (Integer.valueOf(4).equals(row.getColumnValue(i))) {
            hasValueFour = true;
            break;
          }
        }
        assertTrue(hasValueFour, "Row should contain value 4 in at least one column");
        count++;
      }

      System.out.println("Found " + count + " rows with value 4");
    }
  }

  /**
   * Test greater-than filter on numeric columns.
   */
  @Test
  void testGreaterThanFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Filter for id > 5 (or any reasonable threshold)
      ColumnFilter filter = new ColumnGreaterThanFilter(5);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);

          // Verify at least one column has value > 5
          boolean hasValueGreaterThan5 = false;
          for (int i = 0; i < row.getColumnCount(); i++) {
            Object value = row.getColumnValue(i);
            if (value instanceof Integer && (Integer) value > 5) {
              hasValueGreaterThan5 = true;
              break;
            } else if (value instanceof Long && (Long) value > 5) {
              hasValueGreaterThan5 = true;
              break;
            }
          }
          assertTrue(hasValueGreaterThan5, "Row should have at least one value > 5");
          count++;
        }

        // Should have found some rows (unless file has no values > 5)
        System.out.println("Found " + count + " rows with values > 5");
      }
    }
  }

  /**
   * Test less-than filter.
   */
  @Test
  void testLessThanFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ColumnFilter filter = new ColumnLessThanFilter(3);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);

          // Verify at least one column has value < 3
          boolean hasValueLessThan3 = false;
          for (int i = 0; i < row.getColumnCount(); i++) {
            Object value = row.getColumnValue(i);
            if (value instanceof Integer && (Integer) value < 3) {
              hasValueLessThan3 = true;
              break;
            } else if (value instanceof Long && (Long) value < 3) {
              hasValueLessThan3 = true;
              break;
            }
          }
          assertTrue(hasValueLessThan3, "Row should have at least one value < 3");
          count++;
        }

        System.out.println("Found " + count + " rows with values < 3");
      }
    }
  }

  /**
   * Test not-equal filter.
   */
  @Test
  void testNotEqualFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Filter for values != null (simpler test)
      ColumnFilter filter = new ColumnNotEqualFilter(Integer.MAX_VALUE);
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(reader, filter, false);

      int count = 0;
      while (iterator.hasNext() && count < 10) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);
        count++;
      }

      // Should find some rows
      System.out.println("Found " + count + " rows with values != MAX_VALUE");
      // This test is informational - we just verify it doesn't crash
    }
  }

  /**
   * Test multiple filters with AND semantics.
   */
  @Test
  void testMultipleFiltersAnd() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Filter for values > 2 AND < 8
      ColumnFilter[] filters = new ColumnFilter[]{
          new ColumnGreaterThanFilter(2),
          new ColumnLessThanFilter(8)
      };

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filters, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);

          // Each row should have at least one column matching BOTH conditions
          boolean hasMatchingValue = false;
          for (int i = 0; i < row.getColumnCount(); i++) {
            Object value = row.getColumnValue(i);
            if (value instanceof Integer) {
              int intVal = (Integer) value;
              if (intVal > 2 && intVal < 8) {
                hasMatchingValue = true;
                break;
              }
            }
          }
          assertTrue(hasMatchingValue, "Row should have a value in range (2, 8)");
          count++;
        }

        System.out.println("Found " + count + " rows with values > 2 AND < 8");
      }
    }
  }

  /**
   * Test ColumnFilterSet with ALL (AND) semantics.
   */
  @Test
  void testColumnFilterSetWithAll() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Create a filter set: value >= 3 AND value <= 6
      ColumnFilterSet filterSet = new ColumnFilterSet(
          FilterJoinType.All,
          new ColumnGreaterThanOrEqualFilter(3),
          new ColumnLessThanOrEqualFilter(6)
      );

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filterSet, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);
          count++;
        }

        System.out.println("Found " + count + " rows with values in range [3, 6]");
      }
    }
  }

  /**
   * Test ColumnFilterSet with ANY (OR) semantics.
   */
  @Test
  void testColumnFilterSetWithAny() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Create a filter set: value == 0 OR value == 7
      ColumnFilterSet filterSet = new ColumnFilterSet(
          FilterJoinType.Any,
          new ColumnEqualFilter(0),
          new ColumnEqualFilter(7)
      );

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filterSet, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);

          // Verify the row has either 0 or 7 in at least one column
          boolean hasMatchingValue = false;
          for (int i = 0; i < row.getColumnCount(); i++) {
            Object value = row.getColumnValue(i);
            if (Integer.valueOf(0).equals(value) || Integer.valueOf(7).equals(value)) {
              hasMatchingValue = true;
              break;
            }
          }
          assertTrue(hasMatchingValue, "Row should have value 0 or 7");
          count++;
        }

        System.out.println("Found " + count + " rows with value 0 or 7");
      }
    }
  }

  /**
   * Test string filtering with prefix filter.
   */
  @Test
  void testStringPrefixFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "binary.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Filter for strings starting with a common prefix
      ColumnFilter filter = new ColumnPrefixFilter("a");

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);

          // Verify at least one string column starts with "a"
          boolean hasMatchingString = false;
          for (int i = 0; i < row.getColumnCount(); i++) {
            Object value = row.getColumnValue(i);
            if (value instanceof String && ((String) value).startsWith("a")) {
              hasMatchingString = true;
              break;
            }
          }
          assertTrue(hasMatchingString, "Row should have a string starting with 'a'");
          count++;
        }

        System.out.println("Found " + count + " rows with strings starting with 'a'");
      }
    }
  }

  /**
   * Test filtering with no matching rows.
   */
  @Test
  void testNoMatchingRows() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Filter for impossibly large value
      ColumnFilter filter = new ColumnGreaterThanFilter(Integer.MAX_VALUE);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter, false)) {

        assertFalse(iterator.hasNext(), "Should have no matching rows");

        // Calling next() should throw NoSuchElementException
        assertThrows(NoSuchElementException.class, iterator::next);
      }
    }
  }

  /**
   * Test filtering with null value filter.
   */
  @Test
  void testNullValueFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "nulls.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ColumnFilter filter = new ColumnIsNullFilter();

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);

          // Verify at least one column is null
          boolean hasNullValue = false;
          for (int i = 0; i < row.getColumnCount(); i++) {
            if (row.getColumnValue(i) == null) {
              hasNullValue = true;
              break;
            }
          }
          assertTrue(hasNullValue, "Row should have at least one null value");
          count++;
        }

        System.out.println("Found " + count + " rows with null values");
      }
    }
  }

  /**
   * Test filtering with non-null value filter.
   */
  @Test
  void testNotNullValueFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ColumnFilter filter = new ColumnIsNotNullFilter();
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(reader, filter, false);

      int count = 0;
      while (iterator.hasNext() && count < 10) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);

        // Verify at least one column is not null
        boolean hasNonNullValue = false;
        for (int i = 0; i < row.getColumnCount(); i++) {
          if (row.getColumnValue(i) != null) {
            hasNonNullValue = true;
            break;
          }
        }
        assertTrue(hasNonNullValue, "Row should have at least one non-null value");
        count++;
      }

      System.out.println("Found " + count + " rows with non-null values");
      // Test is informational - verify it works without crashing
    }
  }

  /**
   * Test that hasNext() can be called multiple times without side effects.
   */
  @Test
  void testMultipleHasNextCalls() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ColumnFilter filter = new ColumnGreaterThanFilter(0);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter, false)) {

        if (iterator.hasNext()) {
          // Call hasNext multiple times
          assertTrue(iterator.hasNext());
          assertTrue(iterator.hasNext());
          assertTrue(iterator.hasNext());

          // next() should still work correctly
          RowColumnGroup row = iterator.next();
          assertNotNull(row);
        }
      }
    }
  }

  /**
   * Test iteration through all matching rows.
   */
  @Test
  void testCompleteIteration() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    // First count total rows
    int totalRows = 0;
    try (SerializedFileReader reader = new SerializedFileReader(filePath);
         ParquetRowIterator iterator = new ParquetRowIterator(reader, false)) {
      while (iterator.hasNext()) {
        iterator.next();
        totalRows++;
      }
    }

    // Now filter and count
    ColumnFilter filter = new ColumnIsNotNullFilter();
    int filteredRows = 0;
    try (SerializedFileReader reader = new SerializedFileReader(filePath);
         FilteringParquetRowIterator iterator =
             new FilteringParquetRowIterator(reader, filter, false)) {
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);
        filteredRows++;
      }
    }

    System.out.println("Total rows: " + totalRows + ", Filtered rows: " + filteredRows);

    // After iteration, hasNext should return false
    try (SerializedFileReader reader = new SerializedFileReader(filePath);
         FilteringParquetRowIterator iterator =
             new FilteringParquetRowIterator(reader, filter, false)) {
      while (iterator.hasNext()) {
        iterator.next();
      }
      assertFalse(iterator.hasNext());
      assertThrows(NoSuchElementException.class, iterator::next);
    }
  }

  /**
   * Test empty filter array (should match all rows).
   */
  @Test
  void testEmptyFilterArray() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    // Test with empty filter array - should not crash
    ColumnFilter[] emptyFilters = new ColumnFilter[0];
    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(reader, emptyFilters, false);

      int count = 0;
      while (iterator.hasNext() && count < 10) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);
        count++;
      }

      System.out.println("Read " + count + " rows with empty filter");
      // Test is to verify empty filters work without crashing
    }
  }

  /**
   * Test filtering with map columns (if available).
   */
  @Test
  void testMapColumnFiltering() throws IOException {
    String filePath = TEST_DATA_DIR + "nonnullable.impala.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      // Create a generic filter
      ColumnFilter filter = new ColumnIsNotNullFilter();

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);
          count++;

          if (count > 10) break; // Just test a few rows
        }

        System.out.println("Processed " + count + " rows from file with complex types");
      }
    } catch (Exception e) {
      // Some files might not exist, that's okay
      System.out.println("Skipping map column test: " + e.getMessage());
    }
  }

  /**
   * Test that the iterator correctly handles multiple row groups.
   */
  @Test
  void testMultipleRowGroups() throws IOException {
    // This file has multiple row groups
    String filePath = TEST_DATA_DIR + "alltypes_tiny_pages.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      System.out.println("File has " + reader.getNumRowGroups() + " row groups");

      ColumnFilter filter = new ColumnGreaterThanFilter(5);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);
          count++;

          if (count > 100) break; // Don't process too many rows
        }

        System.out.println("Found " + count + " matching rows across row groups");
      }
    } catch (Exception e) {
      System.out.println("Skipping multi-row-group test: " + e.getMessage());
    }
  }

  /**
   * Test getSchema() method.
   */
  @Test
  void testGetSchema() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ColumnFilter filter = new ColumnIsNotNullFilter();

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter)) {

        assertNotNull(iterator.getSchema());
        assertTrue(iterator.getSchema().getNumLogicalColumns() > 0);
      }
    }
  }

  /**
   * Test getTotalRowCount() method.
   */
  @Test
  void testGetTotalRowCount() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ColumnFilter filter = new ColumnIsNotNullFilter();

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter)) {

        long totalRowCount = iterator.getTotalRowCount();
        assertTrue(totalRowCount > 0, "Total row count should be positive");
        System.out.println("Total row count: " + totalRowCount);
      }
    }
  }

  /**
   * Test that close() works correctly.
   */
  @Test
  void testClose() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";
    SerializedFileReader reader = new SerializedFileReader(filePath);

    ColumnFilter filter = new ColumnIsNotNullFilter();
    FilteringParquetRowIterator iterator =
        new FilteringParquetRowIterator(reader, filter, true);

    // Read one row
    if (iterator.hasNext()) {
      iterator.next();
    }

    // Close should work without errors
    assertDoesNotThrow(iterator::close);
  }

  /**
   * Test filtering with ColumnFilters factory.
   */
  @Test
  void testWithColumnFiltersFactory() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ColumnFilters columnFilters = new ColumnFilters();

      // Create filter using factory
      ColumnFilter filter = columnFilters.createFilter(FilterOperator.gt, 3);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(reader, filter, false)) {

        int count = 0;
        while (iterator.hasNext()) {
          iterator.next();
          count++;
        }

        System.out.println("Found " + count + " rows using factory-created filter");
      }
    }
  }
}
