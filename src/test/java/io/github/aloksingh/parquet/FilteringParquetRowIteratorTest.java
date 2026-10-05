package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.PrimitiveLogicalType;
import io.github.aloksingh.parquet.model.Type;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.util.filter.ColumnEqualFilter;
import io.github.aloksingh.parquet.util.filter.ColumnFilter;
import io.github.aloksingh.parquet.util.filter.ColumnFilterSet;
import io.github.aloksingh.parquet.util.filter.ColumnFilters;
import io.github.aloksingh.parquet.util.filter.ColumnGreaterThanFilter;
import io.github.aloksingh.parquet.util.filter.ColumnGreaterThanOrEqualFilter;
import io.github.aloksingh.parquet.util.filter.ColumnIsNotNullFilter;
import io.github.aloksingh.parquet.util.filter.ColumnIsNullFilter;
import io.github.aloksingh.parquet.util.filter.ColumnLessThanFilter;
import io.github.aloksingh.parquet.util.filter.ColumnLessThanOrEqualFilter;
import io.github.aloksingh.parquet.util.filter.ColumnNotEqualFilter;
import io.github.aloksingh.parquet.util.filter.ColumnPrefixFilter;
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.FilterOperator;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.io.TempDir;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test suite for FilteringParquetRowIterator.
 * Tests filtering logic, iteration behavior, and various filter combinations.
 */
class FilteringParquetRowIteratorTest {

  @TempDir Path tempDir;

  private static final String TEST_DATA_DIR = "src/test/data/";

  /**
   * Test basic filtering with a single equality filter on integer values.
   */
  @Test
  void testBasicEqualityFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      // Use a simple equality filter for value 4 (from the first row we know id=4)
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnEqualFilter(logicalColumn, 4);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(baseIterator, filter);

      int count = 0;
      var ids = new ArrayList<Integer>();
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
          ids.add((Integer) row.getColumnValue("id"));
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
        assertEquals(List.of(4), ids);
        assertEquals(ids.size(), count);
    }
  }

  /**
   * Test greater-than filter on numeric columns.
   */
  @Test
  void testGreaterThanFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      // Filter for id > 5 (or any reasonable threshold)
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnGreaterThanFilter(logicalColumn, 5);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {

        int count = 0;
      var ids = new ArrayList<Integer>();
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          ids.add((Integer) row.getColumnValue("id"));
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
        assertEquals(List.of(6, 7), ids);
        assertEquals(ids.size(), count);
      }
    }
  }

  /**
   * Test less-than filter.
   */
  @Test
  void testLessThanFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      // Use column 0 (id) which has values 0-7, so < 3 should match 3 rows (0, 1, 2)
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn(0);
      ColumnFilter filter = new ColumnLessThanFilter(logicalColumn, 3);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {

        int count = 0;
      var ids = new ArrayList<Integer>();
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          ids.add((Integer) row.getColumnValue("id"));
          assertNotNull(row);

          // Verify the target column (column 0) has value < 3
          Object value = row.getColumnValue(0);
          assertTrue(value instanceof Integer && (Integer) value < 3,
              "Column 0 should have value < 3, but got: " + value);
          count++;
        }
        assertEquals(3, count, "Should find 3 rows (values 0, 1, 2)");
        assertEquals(List.of(2, 0, 1), ids);
        assertEquals(ids.size(), count);
      }
    }
  }

  /**
   * Test not-equal filter.
   */
  @Test
  void testNotEqualFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      // Filter for values != null (simpler test)
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnNotEqualFilter(logicalColumn, Integer.MAX_VALUE);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(baseIterator, filter);

      int count = 0;
      var ids = new ArrayList<Integer>();
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
          ids.add((Integer) row.getColumnValue("id"));
        assertNotNull(row);
        count++;
      }

      // Should find some rows
        assertEquals(List.of(4, 5, 6, 7, 2, 3, 0, 1), ids);
        assertEquals(ids.size(), count);
      
    }
  }

  /**
   * Test multiple filters with AND semantics.
   */
  @Test
  void testMultipleFiltersAnd() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      // Filter for values > 2 AND < 8
      ColumnFilter[] filters = new ColumnFilter[]{
          new ColumnGreaterThanFilter(logicalColumn, 2),
          new ColumnLessThanFilter(logicalColumn, 8)
      };
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator,
                   new ColumnFilterSet(logicalColumn, FilterJoinType.All, filters))) {

        int count = 0;
      var ids = new ArrayList<Integer>();
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          ids.add((Integer) row.getColumnValue("id"));
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
        assertEquals(List.of(4, 5, 6, 7, 3), ids);
        assertEquals(ids.size(), count);
      }
    }
  }

  /**
   * Test ColumnFilterSet with ALL (AND) semantics.
   */
  @Test
  void testColumnFilterSetWithAll() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      // Create a filter set: value >= 3 AND value <= 6
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilterSet filterSet = new ColumnFilterSet(logicalColumn,
          FilterJoinType.All,
          new ColumnGreaterThanOrEqualFilter(logicalColumn, 3),
          new ColumnLessThanOrEqualFilter(logicalColumn, 6)
      );
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filterSet)) {

        int count = 0;
      var ids = new ArrayList<Integer>();
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          ids.add((Integer) row.getColumnValue("id"));
          assertNotNull(row);
          count++;
        }
        assertEquals(List.of(4, 5, 6, 3), ids);
        assertEquals(ids.size(), count);
      }
    }
  }

  /**
   * Test ColumnFilterSet with ANY (OR) semantics.
   */
  @Test
  void testColumnFilterSetWithAny() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      // Create a filter set: value == 0 OR value == 7
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilterSet filterSet = new ColumnFilterSet(logicalColumn,
          FilterJoinType.Any,
          new ColumnEqualFilter(logicalColumn, 0),
          new ColumnEqualFilter(logicalColumn, 7)
      );
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filterSet)) {

        int count = 0;
      var ids = new ArrayList<Integer>();
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          ids.add((Integer) row.getColumnValue("id"));
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
        assertEquals(List.of(7, 0), ids);
        assertEquals(ids.size(), count);
      }
    }
  }

  /**
   * Test string filtering with prefix filter: prefix semantics are defined for
   * STRING-annotated text. An unannotated BYTE_ARRAY column is raw binary and its row values
   * are byte[]; a String prefix predicate on it is rejected explicitly instead of being
   * applied lossily as UTF-8 text.
   */
  @Test
  void testStringPrefixFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "binary.parquet";
    char prefix = 1;

    // A STRING-annotated column round-trips as text and supports prefix filtering.
    var stringColumn = new LogicalColumnDescriptor(
        "foo", LogicalType.PRIMITIVE, Type.BYTE_ARRAY,
        new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"foo"}, 0, 0, 0,
            io.github.aloksingh.parquet.model.PrimitiveLogicalType.string()));
    var schema = SchemaDescriptor.fromLogicalColumns("strings", List.of(stringColumn));
    Path file = tempDir.resolve("prefix_strings.parquet");
    try (var writer = new ParquetFileWriter(file, schema)) {
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {prefix + "abc"}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {prefix + "xyz"}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {"other"}));
    }
    try (ParquetFileReader reader = new ParquetFileReader(file)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn(0);
      ColumnFilter filter = new ColumnPrefixFilter(logicalColumn, String.valueOf(prefix));
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {
        List<Object> matched = new ArrayList<>();
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);
          matched.add(row.getColumnValue("foo"));
        }
        assertEquals(List.of(prefix + "abc", prefix + "xyz"), matched);
      }
    }

    // The raw binary fixture: the row API yields raw bytes and a String prefix predicate
    // must be rejected rather than matching lossy text.
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn(0);
      ColumnFilter filter = new ColumnPrefixFilter(logicalColumn, String.valueOf(prefix));
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);
      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {
        ParquetException failure = assertThrows(ParquetException.class, iterator::hasNext,
            "String prefix predicates must not match raw binary row values");
        assertInstanceOf(IllegalArgumentException.class, failure.getCause(),
            "the raw binary rejection stays the cause");
        assertTrue(failure.getMessage().contains("prefix"),
            "context must name the predicate expression but was: " + failure.getMessage());
      }
    }
  }

  /**
   * Test filtering with no matching rows.
   */
  @Test
  void testNoMatchingRows() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      // Filter for impossibly large value
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnGreaterThanFilter(logicalColumn, Integer.MAX_VALUE);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {

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

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn(0);
      ColumnFilter filter = new ColumnIsNullFilter(logicalColumn);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {

        int count = 0;
        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          assertNotNull(row);

          assertEquals(null, row.getColumnValue(0));
          count++;
        }

        assertEquals(8, count);
      }
    }
  }

  /**
   * Test filtering with non-null value filter.
   */
  @Test
  void testNotNullValueFilter() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnIsNotNullFilter(logicalColumn);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(baseIterator, filter);

      int count = 0;
      var ids = new ArrayList<Integer>();
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
          ids.add((Integer) row.getColumnValue("id"));
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
        assertEquals(List.of(4, 5, 6, 7, 2, 3, 0, 1), ids);
        assertEquals(ids.size(), count);
      
    }
  }

  /**
   * Test that hasNext() can be called multiple times without side effects.
   */
  @Test
  void testMultipleHasNextCalls() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnGreaterThanFilter(logicalColumn, 0);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {

        assertTrue(iterator.hasNext());
        {
          // Call hasNext multiple times
          assertTrue(iterator.hasNext());
          assertTrue(iterator.hasNext());
          assertTrue(iterator.hasNext());

          // next() should still work correctly
          RowColumnGroup row = iterator.next();
          assertEquals(4, row.getColumnValue("id"));
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
    try (ParquetFileReader reader = new ParquetFileReader(filePath);
         ParquetRowIterator iterator = new ParquetRowIterator(reader, false)) {
      while (iterator.hasNext()) {
        iterator.next();
        totalRows++;
      }
    }

    // Now filter and count


    int filteredRows = 0;
    try (ParquetFileReader reader = new ParquetFileReader(filePath);
         ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnIsNotNullFilter(logicalColumn);
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(baseIterator, filter);
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        assertNotNull(row);
        filteredRows++;
      }
    }

    assertEquals(8, totalRows);
    assertEquals(8, filteredRows);

    // After iteration, hasNext should return false
    try (ParquetFileReader reader = new ParquetFileReader(filePath);
         ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnIsNotNullFilter(logicalColumn);
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(baseIterator, filter);
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
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);
      FilteringParquetRowIterator iterator =
          new FilteringParquetRowIterator(baseIterator,
              new ColumnFilterSet(null, FilterJoinType.All, emptyFilters));

      int count = 0;
      var ids = new ArrayList<Integer>();
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
          ids.add((Integer) row.getColumnValue("id"));
        assertNotNull(row);
        count++;
      }
        assertEquals(List.of(4, 5, 6, 7, 2, 3, 0, 1), ids);
        assertEquals(ids.size(), count);
      
    }
  }

  /**
   * Test filtering with map columns (if available).
   */
  @Test
  void testScalarFilteringInComplexSchemaFixture() throws IOException {
    // This external file has complex physical leaves; assert a real filter on its scalar ID.
    // The generated MAP fixture below verifies keyed MAP binding and null selection.
    try (var reader = new ParquetFileReader(TEST_DATA_DIR + "nonnullable.impala.parquet");
         var iterator = new FilteringParquetRowIterator(new ParquetRowIterator(reader, false),
             new ColumnIsNotNullFilter(reader.getSchema().getLogicalColumn("ID")))) {
      assertTrue(iterator.hasNext());
      assertEquals(8L, iterator.next().getColumnValue("ID"));
      assertFalse(iterator.hasNext());
    }
  }

  /**
   * Test that the iterator correctly handles multiple row groups.
   */
  @Test
  void testMultipleRowGroups() throws IOException {
    var id = new LogicalColumnDescriptor("id", LogicalType.PRIMITIVE, Type.INT32,
        new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0));
    var schema = SchemaDescriptor.fromLogicalColumns("groups", List.of(id));
    Path file = tempDir.resolve("groups.parquet");
    try (var writer = new ParquetFileWriter(file, schema, CompressionCodec.UNCOMPRESSED, 1024, 128)) {
      for (int value = 0; value < 1003; value++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {value}));
    }
    try (var reader = new ParquetFileReader(file);
         var iterator = new FilteringParquetRowIterator(new ParquetRowIterator(reader, false),
             new ColumnGreaterThanOrEqualFilter(reader.getSchema().getLogicalColumn("id"), 997))) {
      assertTrue(reader.getNumRowGroups() > 1, "fixture must genuinely span groups");
      var ids = new ArrayList<Integer>();
      while (iterator.hasNext()) ids.add((Integer) iterator.next().getColumnValue("id"));
      assertEquals(List.of(997, 998, 999, 1000, 1001, 1002), ids);
      assertEquals(1003, iterator.getTotalRowCount());
    }
  }

  /**
   * Test getSchema() method.
   */
  @Test
  void testGetSchema() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnIsNotNullFilter(logicalColumn);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {

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

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
      ColumnFilter filter = new ColumnIsNotNullFilter(logicalColumn);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {

        long totalRowCount = iterator.getTotalRowCount();
        assertEquals(8, totalRowCount);
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
    ParquetFileReader reader = new ParquetFileReader(filePath);
    LogicalColumnDescriptor logicalColumn = reader.getSchema().getLogicalColumn("id");
    ColumnFilter filter = new ColumnIsNotNullFilter(logicalColumn);
    ParquetRowIterator baseIterator = new ParquetRowIterator(reader, true);
    FilteringParquetRowIterator iterator =
        new FilteringParquetRowIterator(baseIterator, filter);

    // Read one row
    assertTrue(iterator.hasNext());
    {
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

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ColumnFilters columnFilters = new ColumnFilters();

      // Create filter using factory
      ColumnFilter filter =
          columnFilters.createFilter(reader.getSchema().getLogicalColumn(0), FilterOperator.gt, 3);
      ParquetRowIterator baseIterator = new ParquetRowIterator(reader, false);

      try (FilteringParquetRowIterator iterator =
               new FilteringParquetRowIterator(baseIterator, filter)) {

        int count = 0;
      var ids = new ArrayList<Integer>();
        while (iterator.hasNext()) {
          ids.add((Integer) iterator.next().getColumnValue("id"));
          count++;
        }
        assertEquals(List.of(4, 5, 6, 7), ids);
        assertEquals(ids.size(), count);
      }
    }
  }

  @Test
  void testTypedMapConstantsAndKeyedNullSelectionReturnExactIds() throws IOException {
    var id = new LogicalColumnDescriptor("id", LogicalType.PRIMITIVE, Type.INT32,
        new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0));
    // String keys (annotated UTF8) with INT64 values: keyed predicates match map keys as text.
    var map = SchemaDescriptor.createMapColumn("attributes",
        new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"attributes", "key_value", "key"},
            2, 1, 0, PrimitiveLogicalType.string()),
        new ColumnDescriptor(Type.INT64, new String[] {"attributes", "key_value", "value"},
            2, 1, 0),
        true, false);
    var schema = SchemaDescriptor.fromLogicalColumns("maps", List.of(id, map));
    Path file = tempDir.resolve("maps.parquet");
    Object[] maps = {null, Map.of(), Map.of("key", 11L), Map.of("key", 12L),
        Map.of("other", 12L), Map.of("key", 13L)};
    try (var writer = new ParquetFileWriter(file, schema)) {
      for (int i = 0; i < maps.length; i++) writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {i, maps[i]}));
    }
    var operators = List.of(FilterOperator.eq, FilterOperator.neq, FilterOperator.isNull, FilterOperator.isNotNull);
    var expected = List.of(List.of(3), List.of(2, 5), List.of(0, 1, 4), List.of(2, 3, 5));
    for (int i = 0; i < operators.size(); i++) {
      try (var reader = new ParquetFileReader(file)) {
        var filter = new ColumnFilters().createFilter(reader.getSchema().getLogicalColumn("attributes"),
            operators.get(i), operators.get(i) == FilterOperator.eq || operators.get(i) == FilterOperator.neq ? "12" : null,
            Optional.of("key"));
        try (var iterator = new FilteringParquetRowIterator(new ParquetRowIterator(reader, false), filter)) {
          var ids = new ArrayList<Integer>();
          while (iterator.hasNext()) ids.add((Integer) iterator.next().getColumnValue("id"));
          assertEquals(expected.get(i), ids, operators.get(i).toString());
          assertEquals(6, iterator.getTotalRowCount());
        }
      }
    }
  }
}
