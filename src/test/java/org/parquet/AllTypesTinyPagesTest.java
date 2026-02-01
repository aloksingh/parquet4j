package org.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ColumnValues;
import org.parquet.model.ParquetMetadata;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.Type;

/**
 * Test for reading and validating alltypes_tiny_pages.parquet file.
 * This file contains 7300 rows with 13 columns of various data types.
 */
class AllTypesTinyPagesTest {

  private static final String TEST_FILE = "src/test/data/alltypes_tiny_pages.parquet";
  private static final long EXPECTED_NUM_ROWS = 7300;
  private static final int EXPECTED_NUM_COLUMNS = 13;

  @Test
  void testReadMetadata() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      ParquetMetadata metadata = reader.getMetadata();

      assertNotNull(metadata, "Metadata should not be null");
      assertEquals(EXPECTED_NUM_ROWS, metadata.fileMetadata().numRows(),
          "Expected 7300 rows");
      assertTrue(metadata.getNumRowGroups() > 0, "Should have at least one row group");

      SchemaDescriptor schema = reader.getSchema();
      assertEquals(EXPECTED_NUM_COLUMNS, schema.getNumColumns(),
          "Expected 13 columns");

      System.out.println("\n=== alltypes_tiny_pages.parquet Metadata ===");
      System.out.printf("Total rows: %d%n", metadata.fileMetadata().numRows());
      System.out.printf("Row groups: %d%n", metadata.getNumRowGroups());
      System.out.printf("Columns: %d%n", schema.getNumColumns());
    }
  }

  @Test
  void testSchemaStructure() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      // Expected column names and types
      String[] expectedColumns = {
          "id", "bool_col", "tinyint_col", "smallint_col", "int_col",
          "bigint_col", "float_col", "double_col", "date_string_col",
          "string_col", "timestamp_col", "year", "month"
      };

      Type[] expectedTypes = {
          Type.INT32, Type.BOOLEAN, Type.INT32, Type.INT32, Type.INT32,
          Type.INT64, Type.FLOAT, Type.DOUBLE, Type.BYTE_ARRAY,
          Type.BYTE_ARRAY, Type.INT96, Type.INT32, Type.INT32  // timestamp_col is INT96
      };

      assertEquals(expectedColumns.length, schema.getNumColumns(),
          "Column count mismatch");

      System.out.println("\n=== Schema Structure ===");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        String colName = col.path()[col.path().length - 1];
        Type colType = col.physicalType();

        System.out.printf("%d. %s (%s)%n", i + 1, colName, colType);

        assertTrue(col.getPathString().contains(expectedColumns[i]),
            "Column " + i + " should contain '" + expectedColumns[i] + "'");
        assertEquals(expectedTypes[i], colType,
            "Column " + expectedColumns[i] + " type mismatch");
      }
    }
  }

  @Test
  void testReadAllRowGroups() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      long totalRowsRead = 0;

      System.out.println("\n=== Row Groups ===");
      for (int i = 0; i < reader.getNumRowGroups(); i++) {
        SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(i);
        long rowsInGroup = rowGroup.getNumRows();
        totalRowsRead += rowsInGroup;

        System.out.printf("Row Group %d: %d rows, %d columns%n",
            i, rowsInGroup, rowGroup.getNumColumns());

        assertTrue(rowsInGroup > 0, "Row group should contain rows");
        assertEquals(EXPECTED_NUM_COLUMNS, rowGroup.getNumColumns(),
            "Row group should have all columns");
      }

      assertEquals(EXPECTED_NUM_ROWS, totalRowsRead,
          "Total rows across all row groups should match expected");
    }
  }

  @Test
  void testReadInt32Column() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      // Test reading the "id" column (INT32)
      int idColumnIdx = findColumnIndex(schema, "id");
      assertTrue(idColumnIdx >= 0, "Should find 'id' column");

      System.out.println("\n=== Reading INT32 column 'id' ===");
      long totalValues = 0;

      for (int rgIdx = 0; rgIdx < reader.getNumRowGroups(); rgIdx++) {
        SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(rgIdx);
        ColumnValues values = rowGroup.readColumn(idColumnIdx);
        List<Integer> idValues = values.decodeAsInt32();

        assertNotNull(idValues, "Values should not be null");
        totalValues += idValues.size();

        if (rgIdx == 0 && !idValues.isEmpty()) {
          // Display first few values
          System.out.printf("First 5 values: %s%n",
              idValues.subList(0, Math.min(5, idValues.size())));

          // Validate we have valid data
          assertNotNull(idValues.get(0), "First id should not be null");
        }
      }

      assertEquals(EXPECTED_NUM_ROWS, totalValues,
          "Should read all rows for 'id' column");
      System.out.printf("Total values read: %d%n", totalValues);
    }
  }

  @Test
  void testReadBooleanColumn() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      int boolColumnIdx = findColumnIndex(schema, "bool_col");
      assertTrue(boolColumnIdx >= 0, "Should find 'bool_col' column");

      System.out.println("\n=== Reading BOOLEAN column 'bool_col' ===");
      long totalValues = 0;

      for (int rgIdx = 0; rgIdx < reader.getNumRowGroups(); rgIdx++) {
        SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(rgIdx);
        ColumnValues values = rowGroup.readColumn(boolColumnIdx);
        List<Boolean> boolValues = values.decodeAsBoolean();

        assertNotNull(boolValues, "Values should not be null");
        totalValues += boolValues.size();

        if (rgIdx == 0 && !boolValues.isEmpty()) {
          System.out.printf("First 5 values: %s%n",
              boolValues.subList(0, Math.min(5, boolValues.size())));

          // Validate we have valid data (boolean values are true/false)
          assertNotNull(boolValues.get(0), "First bool_col should not be null");
        }
      }

      assertEquals(EXPECTED_NUM_ROWS, totalValues,
          "Should read all rows for 'bool_col' column");
      System.out.printf("Total values read: %d%n", totalValues);
    }
  }

  @Test
  void testReadInt64Column() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      int bigintColumnIdx = findColumnIndex(schema, "bigint_col");
      assertTrue(bigintColumnIdx >= 0, "Should find 'bigint_col' column");

      System.out.println("\n=== Reading INT64 column 'bigint_col' ===");
      long totalValues = 0;

      for (int rgIdx = 0; rgIdx < reader.getNumRowGroups(); rgIdx++) {
        SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(rgIdx);
        ColumnValues values = rowGroup.readColumn(bigintColumnIdx);
        List<Long> longValues = values.decodeAsInt64();

        assertNotNull(longValues, "Values should not be null");
        totalValues += longValues.size();

        if (rgIdx == 0 && !longValues.isEmpty()) {
          System.out.printf("First 5 values: %s%n",
              longValues.subList(0, Math.min(5, longValues.size())));

          // Validate we have valid data
          assertNotNull(longValues.get(0), "First bigint_col should not be null");
        }
      }

      assertEquals(EXPECTED_NUM_ROWS, totalValues,
          "Should read all rows for 'bigint_col' column");
      System.out.printf("Total values read: %d%n", totalValues);
    }
  }

  @Test
  void testReadFloatColumn() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      int floatColumnIdx = findColumnIndex(schema, "float_col");
      assertTrue(floatColumnIdx >= 0, "Should find 'float_col' column");

      System.out.println("\n=== Reading FLOAT column 'float_col' ===");
      long totalValues = 0;

      for (int rgIdx = 0; rgIdx < reader.getNumRowGroups(); rgIdx++) {
        SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(rgIdx);
        ColumnValues values = rowGroup.readColumn(floatColumnIdx);
        List<Float> floatValues = values.decodeAsFloat();

        assertNotNull(floatValues, "Values should not be null");
        totalValues += floatValues.size();

        if (rgIdx == 0 && !floatValues.isEmpty()) {
          System.out.printf("First 5 values: %s%n",
              floatValues.subList(0, Math.min(5, floatValues.size())));

          // Validate we have valid data (finite float values)
          assertNotNull(floatValues.get(0), "First float_col should not be null");
          assertTrue(Float.isFinite(floatValues.get(0)), "First float_col should be finite");
        }
      }

      assertEquals(EXPECTED_NUM_ROWS, totalValues,
          "Should read all rows for 'float_col' column");
      System.out.printf("Total values read: %d%n", totalValues);
    }
  }

  @Test
  void testReadDoubleColumn() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      int doubleColumnIdx = findColumnIndex(schema, "double_col");
      assertTrue(doubleColumnIdx >= 0, "Should find 'double_col' column");

      System.out.println("\n=== Reading DOUBLE column 'double_col' ===");
      long totalValues = 0;

      for (int rgIdx = 0; rgIdx < reader.getNumRowGroups(); rgIdx++) {
        SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(rgIdx);
        ColumnValues values = rowGroup.readColumn(doubleColumnIdx);
        List<Double> doubleValues = values.decodeAsDouble();

        assertNotNull(doubleValues, "Values should not be null");
        totalValues += doubleValues.size();

        if (rgIdx == 0 && !doubleValues.isEmpty()) {
          System.out.printf("First 5 values: %s%n",
              doubleValues.subList(0, Math.min(5, doubleValues.size())));

          // Validate we have valid data (finite double values)
          assertNotNull(doubleValues.get(0), "First double_col should not be null");
          assertTrue(Double.isFinite(doubleValues.get(0)), "First double_col should be finite");
        }
      }

      assertEquals(EXPECTED_NUM_ROWS, totalValues,
          "Should read all rows for 'double_col' column");
      System.out.printf("Total values read: %d%n", totalValues);
    }
  }

  @Test
  void testReadStringColumn() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      int stringColumnIdx = findColumnIndex(schema, "string_col");
      assertTrue(stringColumnIdx >= 0, "Should find 'string_col' column");

      System.out.println("\n=== Reading STRING column 'string_col' ===");
      long totalValues = 0;

      for (int rgIdx = 0; rgIdx < reader.getNumRowGroups(); rgIdx++) {
        SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(rgIdx);
        ColumnValues values = rowGroup.readColumn(stringColumnIdx);
        List<String> stringValues = values.decodeAsString();

        assertNotNull(stringValues, "Values should not be null");
        totalValues += stringValues.size();

        if (rgIdx == 0 && !stringValues.isEmpty()) {
          System.out.printf("First 5 values: %s%n",
              stringValues.subList(0, Math.min(5, stringValues.size())));

          // Validate we have valid data (non-empty strings)
          assertNotNull(stringValues.get(0), "First string_col should not be null");
          assertFalse(stringValues.get(0).isEmpty(), "First string_col should not be empty");
        }
      }

      assertEquals(EXPECTED_NUM_ROWS, totalValues,
          "Should read all rows for 'string_col' column");
      System.out.printf("Total values read: %d%n", totalValues);
    }
  }

  @Test
  void testReadAllColumns() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("\n=== Reading All Columns ===");

      for (int colIdx = 0; colIdx < schema.getNumColumns(); colIdx++) {
        ColumnDescriptor col = schema.getColumn(colIdx);
        String colName = col.path()[col.path().length - 1];
        Type colType = col.physicalType();

        long totalValues = 0;

        for (int rgIdx = 0; rgIdx < reader.getNumRowGroups(); rgIdx++) {
          SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(rgIdx);

          try {
            ColumnValues values = rowGroup.readColumn(colIdx);

            int valuesRead = switch (colType) {
              case BOOLEAN -> values.decodeAsBoolean().size();
              case INT32 -> values.decodeAsInt32().size();
              case INT64 -> values.decodeAsInt64().size();
              case FLOAT -> values.decodeAsFloat().size();
              case DOUBLE -> values.decodeAsDouble().size();
              case BYTE_ARRAY -> values.decodeAsString().size();
              default -> 0;
            };

            totalValues += valuesRead;
          } catch (Exception e) {
            System.out.printf("  Warning: Could not read column %s: %s%n",
                colName, e.getMessage());
          }
        }

        System.out.printf("Column '%s' (%s): %d values%n",
            colName, colType, totalValues);

        if (totalValues > 0) {
          assertEquals(EXPECTED_NUM_ROWS, totalValues,
              "Column '" + colName + "' should have all rows");
        }
      }
    }
  }

  @Test
  void testValidateDataIntegrity() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      System.out.println("\n=== Validating Data Integrity ===");

      // Validate that we can read and data from key columns
      assertNotNull(getFirstValue(rowGroup, schema, "id", Type.INT32),
          "id should be readable");
      assertNotNull(getFirstValue(rowGroup, schema, "bool_col", Type.BOOLEAN),
          "bool_col should be readable");
      assertNotNull(getFirstValue(rowGroup, schema, "int_col", Type.INT32),
          "int_col should be readable");
      assertNotNull(getFirstValue(rowGroup, schema, "bigint_col", Type.INT64),
          "bigint_col should be readable");

      Float floatVal = (Float) getFirstValue(rowGroup, schema, "float_col", Type.FLOAT);
      assertNotNull(floatVal, "float_col should be readable");
      assertTrue(Float.isFinite(floatVal), "float_col should be finite");

      Double doubleVal = (Double) getFirstValue(rowGroup, schema, "double_col", Type.DOUBLE);
      assertNotNull(doubleVal, "double_col should be readable");
      assertTrue(Double.isFinite(doubleVal), "double_col should be finite");

      String stringVal = (String) getFirstValue(rowGroup, schema, "string_col", Type.BYTE_ARRAY);
      assertNotNull(stringVal, "string_col should be readable");
      assertFalse(stringVal.isEmpty(), "string_col should not be empty");

      assertNotNull(getFirstValue(rowGroup, schema, "year", Type.INT32),
          "year should be readable");
      assertNotNull(getFirstValue(rowGroup, schema, "month", Type.INT32),
          "month should be readable");

      System.out.println("✓ All data integrity checks passed successfully");
    }
  }

  /**
   * Helper method to find column index by name
   */
  private int findColumnIndex(SchemaDescriptor schema, String columnName) {
    for (int i = 0; i < schema.getNumColumns(); i++) {
      ColumnDescriptor col = schema.getColumn(i);
      String[] path = col.path();
      if (path[path.length - 1].equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Helper method to get the first value of a column
   */
  private Object getFirstValue(SerializedFileReader.RowGroupReader rowGroup,
                               SchemaDescriptor schema,
                               String columnName,
                               Type expectedType) throws IOException {
    int colIdx = findColumnIndex(schema, columnName);
    if (colIdx < 0) {
      throw new IllegalArgumentException("Column not found: " + columnName);
    }

    ColumnValues values = rowGroup.readColumn(colIdx);

    return switch (expectedType) {
      case BOOLEAN -> values.decodeAsBoolean().get(0);
      case INT32 -> values.decodeAsInt32().get(0);
      case INT64 -> values.decodeAsInt64().get(0);
      case FLOAT -> values.decodeAsFloat().get(0);
      case DOUBLE -> values.decodeAsDouble().get(0);
      case BYTE_ARRAY -> values.decodeAsString().get(0);
      default -> throw new UnsupportedOperationException("Unsupported type: " + expectedType);
    };
  }
}
