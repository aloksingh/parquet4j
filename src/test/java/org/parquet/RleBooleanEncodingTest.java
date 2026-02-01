package org.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.RowColumnGroup;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.Type;

/**
 * Test for reading boolean column with RLE (Run Length Encoding) encoding.
 *
 * <h2>Test File</h2>
 * This test validates the {@code rle_boolean_encoding.parquet} file which contains:
 * <ul>
 *   <li>Single column: {@code datatype_boolean} (BOOLEAN type)</li>
 *   <li>68 total rows</li>
 *   <li>Expected distribution: 36 true, 26 false, 6 null values</li>
 *   <li>Null positions (0-indexed): 2, 15, 23, 38, 48, 60</li>
 * </ul>
 *
 * <h2>Known Bug</h2>
 * The Java Parquet reader currently has a bug with RLE boolean null handling:
 * <ul>
 *   <li>Null values are skipped/ignored during decoding</li>
 *   <li>This causes all subsequent values to be shifted/out of sync</li>
 *   <li>The reader returns 0 nulls instead of 6</li>
 *   <li>Boolean values after null positions are incorrect</li>
 * </ul>
 *
 * <h2>Validation Modes</h2>
 * This test supports two validation modes controlled by {@code STRICT_NULL_VALIDATION}:
 * <ul>
 *   <li><b>LENIENT (default, false):</b> Validates against current buggy behavior.
 *       Tests pass with current implementation. Use this for regression testing.</li>
 *   <li><b>STRICT (true):</b> Validates against expected correct behavior from pyarrow.
 *       Tests will fail until the bug is fixed. Set this to true when fixing the bug.</li>
 * </ul>
 *
 * <h2>Verifying with PyArrow</h2>
 * To verify the expected values using Python's pyarrow library:
 * <pre>{@code
 * import pyarrow.parquet as pq
 *
 * # Read the parquet file
 * table = pq.read_table('src/test/data/rle_boolean_encoding.parquet')
 * column = table.column('datatype_boolean')
 *
 * # Print all values
 * print("All values:")
 * for i, val in enumerate(column.to_pylist()):
 *     print(f"Row {i}: {val}")
 *
 * # Print null positions
 * null_positions = [i for i, v in enumerate(column.to_pylist()) if v is None]
 * print(f"\nNull positions: {null_positions}")
 * print(f"Null count: {len(null_positions)}")
 *
 * # Print distribution
 * values = column.to_pylist()
 * true_count = sum(1 for v in values if v is True)
 * false_count = sum(1 for v in values if v is False)
 * null_count = sum(1 for v in values if v is None)
 * print(f"\nDistribution: {true_count} true, {false_count} false, {null_count} null")
 * }</pre>
 *
 * <h2>When the Bug is Fixed</h2>
 * After fixing the RLE boolean null handling bug:
 * <ol>
 *   <li>Set {@code STRICT_NULL_VALIDATION = true}</li>
 *   <li>Run tests - they should now pass</li>
 *   <li>Remove {@code ACTUAL_BUGGY_VALUES} array (no longer needed)</li>
 *   <li>Simplify test logic to only use {@code EXPECTED_VALUES}</li>
 * </ol>
 *
 * @see <a href="https://parquet.apache.org/docs/">Parquet Documentation</a>
 */
class RleBooleanEncodingTest {

  private static final String TEST_DATA_DIR = "src/test/data/";
  private static final String TEST_FILE = TEST_DATA_DIR + "rle_boolean_encoding.parquet";

  /**
   * Expected values from the parquet file as determined by pyarrow.
   * Python code used to generate these values:
   * <pre>
   * import pyarrow.parquet as pq
   * table = pq.read_table('src/test/data/rle_boolean_encoding.parquet')
   * print(table.column('datatype_boolean').to_pylist())
   * </pre>
   * <p>
   * Expected: 68 rows total, 36 true, 26 false, 6 nulls
   * Null positions (0-indexed): 2, 15, 23, 38, 48, 60
   */
  private static final Boolean[] EXPECTED_VALUES = {
      true, false, null, true, true, false, false, true, true, true,
      false, false, true, true, false, null, true, true, false, false,
      true, true, false, null, true, true, false, false, true, true,
      true, false, false, false, false, true, true, false, null, true,
      true, false, false, true, true, true, false, false, null, true,
      true, false, false, true, true, true, false, true, true, false,
      null, true, true, false, false, true, true, true
  };



  @Test
  void testRleBooleanEncodingReadWithRowIterator() throws IOException {
    System.out.println("Testing RLE boolean encoding file: " + TEST_FILE);

    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      // Verify schema
      assertEquals(1, schema.getNumColumns(), "Should have exactly 1 column");
      ColumnDescriptor col = schema.getColumn(0);
      assertEquals("datatype_boolean", col.getPathString(),
          "Column name should be 'datatype_boolean'");
      assertEquals(Type.BOOLEAN, col.physicalType(), "Column type should be BOOLEAN");

      System.out.println("Schema: " + col.getPathString() + " (" + col.physicalType() + ")");

      // Read all rows
      RowColumnGroupIterator iterator = reader.rowIterator();
      int rowCount = 0;
      int nullCount = 0;
      int trueCount = 0;
      int falseCount = 0;
      int mismatchCount = 0;

      // Choose which values to compare against
      Boolean[] comparisonValues = EXPECTED_VALUES;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        assertEquals(1, row.getColumnCount(), "Each row should have 1 column");

        Object value = row.getColumnValue(0);
        Boolean comparisonValue = comparisonValues[rowCount];

        // Count distribution
        if (value == null) {
          nullCount++;
        } else if ((Boolean) value) {
          trueCount++;
        } else {
          falseCount++;
        }

        // Validate against comparison array
        if (comparisonValue == null) {
          if (value != null) {
            mismatchCount++;
          }
          assertNull(value, "Row " + rowCount + " mismatch");
        } else {
          assertNotNull(value, "Row " + rowCount + " should not be null");
          assertTrue(value instanceof Boolean, "Value should be a Boolean");
          if (!comparisonValue.equals(value)) {
            mismatchCount++;
          }
          assertEquals(comparisonValue, value,
              "Row " + rowCount + " value mismatch (expected: " + comparisonValue + ", got: " +
                  value + ")");
        }

        rowCount++;
      }

      // Verify row count
      assertEquals(68, rowCount, "Should have 68 rows");
      System.out.println("Successfully read " + rowCount + " rows");

      // Print distribution
      System.out.println("Distribution:");
      System.out.println("  True values: " + trueCount);
      System.out.println("  False values: " + falseCount);
      System.out.println("  Null values: " + nullCount);

      assertEquals(6, nullCount, "Should have 6 null values");
      assertEquals(36, trueCount, "Should have 36 true values");
      assertEquals(26, falseCount, "Should have 26 false values");
      System.out.println("✓ All values match expected (pyarrow) values");
    }
  }

  @Test
  void testRleBooleanEncodingMetadata() throws IOException {
    System.out.println("\nTesting metadata for: " + TEST_FILE);

    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      // Print schema details
      System.out.println("Number of columns: " + schema.getNumColumns());
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.println("  Column " + i + ":");
        System.out.println("    Name: " + col.getPathString());
        System.out.println("    Physical Type: " + col.physicalType());
        System.out.println("    Max Definition Level: " + col.maxDefinitionLevel());
        System.out.println("    Max Repetition Level: " + col.maxRepetitionLevel());
      }

      // Verify the column supports nulls
      ColumnDescriptor col = schema.getColumn(0);
      assertTrue(col.maxDefinitionLevel() > 0,
          "Column should support null values (max definition level > 0)");
    }
  }

  @Test
  void testRleBooleanEncodingValuePattern() throws IOException {
    System.out.println("\nTesting specific value patterns in: " + TEST_FILE);

    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      RowColumnGroupIterator iterator = reader.rowIterator();
      Boolean[] comparisonValues = EXPECTED_VALUES;

      // Test first 5 rows
      for (int i = 0; i < 5 && iterator.hasNext(); i++) {
        Object value = iterator.next().getColumnValue(0);
        Boolean expected = comparisonValues[i];

        if (expected == null) {
          assertNull(value, "Row " + i + " should be null");
        } else {
          assertEquals(expected, value, "Row " + i + " should be " + expected);
        }
      }

      System.out.println("First 5 rows validated");
    }
  }

  @Test
  void testRleBooleanEncodingNullPositions() throws IOException {
    System.out.println("\nTesting null value positions in: " + TEST_FILE);

    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      // Expected null positions (0-indexed) from pyarrow: 2, 15, 23, 38, 48, 60
      int[] expectedNullPositions = {2, 15, 23, 38, 48, 60};
      int rowIndex = 0;
      int nullCount = 0;

      System.out.println(
          "Expected null positions (from pyarrow): " + Arrays.toString(expectedNullPositions));

      while (iterator.hasNext()) {
        Object value = iterator.next().getColumnValue(0);
        if (value == null) {
          nullCount++;
        }
        rowIndex++;
      }

      System.out.println("Total rows read: " + rowIndex);
      System.out.println("Null values found: " + nullCount);
      assertEquals(6, nullCount, "Should find exactly 6 null values");
      System.out.println("✓ Null count matches expected");
      // Always verify we read all rows
      assertEquals(68, rowIndex, "Should read all 68 rows");
    }
  }
}
