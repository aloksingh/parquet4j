package org.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ParquetMetadata;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.Type;

/**
 * Tests for reading files with DELTA_BINARY_PACKED and DELTA_BYTE_ARRAY encodings
 * on required (non-null) columns.
 * <p>
 * Test file: delta_encoding_required_column.parquet
 * - Created by: parquet-mr version 1.12.1
 * - Format version: 1.0
 * - Rows: 100
 * - Columns: 17 (9 int32, 8 string)
 * - All columns are required (no nulls)
 * - Compression: UNCOMPRESSED
 * - Integer encoding: DELTA_BINARY_PACKED
 * - String encoding: DELTA_BYTE_ARRAY
 * <p>
 * Expected data (verified with pyarrow):
 * - c_customer_sk: [105, 104, 103, 102, 101, ...] (descending from 105 to 1)
 * - c_birth_year: [1945, 1936, 1947, 1937, 1951, ...]
 * - c_customer_id: ['AAAAAAAAJGAAAAAA', 'AAAAAAAAIGAAAAAA', ...]
 * - c_first_name: ['Frank', 'Benjamin', 'James', 'Jodi', 'Jeffrey', ...]
 */
public class DeltaEncodingRequiredColumnTest {

  @Test
  void testFileStructure() throws IOException {
    String filePath = "src/test/data/delta_encoding_required_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = reader.getSchema();

      // Verify file structure
      assertEquals(100, metadata.fileMetadata().numRows());
      assertEquals(17, schema.getNumColumns());

      // Verify integer columns use DELTA_BINARY_PACKED (through INT32 type)
      assertEquals("c_customer_sk:", schema.getColumn(0).getPathString());
      assertEquals(Type.INT32, schema.getColumn(0).physicalType());

      // Verify string columns use DELTA_BYTE_ARRAY (through BYTE_ARRAY type)
      assertEquals("c_customer_id:", schema.getColumn(9).getPathString());
      assertEquals(Type.BYTE_ARRAY, schema.getColumn(9).physicalType());
    }
  }

  @Test
  void testDeltaBinaryPackedInt32() throws IOException {
    String filePath = "src/test/data/delta_encoding_required_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read c_customer_sk column (index 0) - descending sequence
      List<Integer> values = rowGroup.readColumn(0).decodeAsInt32();

      assertEquals(100, values.size(), "Should have 100 values");

      // Verify first 5 values (descending from 105)
      assertEquals(105, values.get(0));
      assertEquals(104, values.get(1));
      assertEquals(103, values.get(2));
      assertEquals(102, values.get(3));
      assertEquals(101, values.get(4));

      // Verify last value
      assertEquals(1, values.get(99));

      // Verify no nulls
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }

      // Verify descending sequence
      for (int i = 0; i < values.size() - 1; i++) {
        assertTrue(values.get(i) > values.get(i + 1),
            "Values should be in descending order");
      }
    }
  }

  @Test
  void testDeltaBinaryPackedBirthColumns() throws IOException {
    String filePath = "src/test/data/delta_encoding_required_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Test c_birth_day (column 6)
      List<Integer> birthDays = rowGroup.readColumn(6).decodeAsInt32();
      assertEquals(100, birthDays.size());
      assertEquals(14, birthDays.get(0));
      assertEquals(29, birthDays.get(1));
      assertEquals(3, birthDays.get(2));

      // Test c_birth_month (column 7)
      List<Integer> birthMonths = rowGroup.readColumn(7).decodeAsInt32();
      assertEquals(100, birthMonths.size());
      assertEquals(1, birthMonths.get(0));
      assertEquals(11, birthMonths.get(1));
      assertEquals(5, birthMonths.get(2));

      // Test c_birth_year (column 8)
      List<Integer> birthYears = rowGroup.readColumn(8).decodeAsInt32();
      assertEquals(100, birthYears.size());
      assertEquals(1945, birthYears.get(0));
      assertEquals(1936, birthYears.get(1));
      assertEquals(1947, birthYears.get(2));

      // Verify value ranges
      int minYear = birthYears.stream().mapToInt(Integer::intValue).min().orElseThrow();
      int maxYear = birthYears.stream().mapToInt(Integer::intValue).max().orElseThrow();
      assertEquals(1925, minYear, "Min birth year should be 1925");
      assertEquals(1991, maxYear, "Max birth year should be 1991");
    }
  }

  @Test
  void testDeltaByteArrayCustomerId() throws IOException {
    String filePath = "src/test/data/delta_encoding_required_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read c_customer_id column (index 9)
      List<String> values = rowGroup.readColumn(9).decodeAsString();

      assertEquals(100, values.size(), "Should have 100 values");

      // Verify first 5 values
      assertEquals("AAAAAAAAJGAAAAAA", values.get(0));
      assertEquals("AAAAAAAAIGAAAAAA", values.get(1));
      assertEquals("AAAAAAAAHGAAAAAA", values.get(2));
      assertEquals("AAAAAAAAGGAAAAAA", values.get(3));
      assertEquals("AAAAAAAAFGAAAAAA", values.get(4));

      // Verify no nulls
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }
    }
  }

  @Test
  void testDeltaByteArrayNames() throws IOException {
    String filePath = "src/test/data/delta_encoding_required_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Test c_salutation (column 10)
      List<String> salutations = rowGroup.readColumn(10).decodeAsString();
      assertEquals(100, salutations.size());
      assertEquals("Dr.", salutations.get(0));
      assertEquals("Dr.", salutations.get(1));
      assertEquals("Dr.", salutations.get(2));
      assertEquals("Ms.", salutations.get(3));
      assertEquals("Dr.", salutations.get(4));

      // Test c_first_name (column 11)
      List<String> firstNames = rowGroup.readColumn(11).decodeAsString();
      assertEquals(100, firstNames.size());
      assertEquals("Frank", firstNames.get(0));
      assertEquals("Benjamin", firstNames.get(1));
      assertEquals("James", firstNames.get(2));
      assertEquals("Jodi", firstNames.get(3));
      assertEquals("Jeffrey", firstNames.get(4));

      // Test c_last_name (column 12)
      List<String> lastNames = rowGroup.readColumn(12).decodeAsString();
      assertEquals(100, lastNames.size());
      assertEquals("Strain", lastNames.get(0));
      assertEquals("Johnson", lastNames.get(1));
      assertEquals("Porter", lastNames.get(2));
      assertEquals("Silva", lastNames.get(3));
      assertEquals("Bruce", lastNames.get(4));

      // Verify no nulls in any name column
      assertFalse(firstNames.contains(null), "First names should not contain nulls");
      assertFalse(lastNames.contains(null), "Last names should not contain nulls");
    }
  }

  @Test
  void testAllIntegerColumns() throws IOException {
    String filePath = "src/test/data/delta_encoding_required_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read all integer columns (0-8) and verify they all have 100 non-null values
      for (int i = 0; i < 9; i++) {
        ColumnDescriptor col = schema.getColumn(i);

        if (col.physicalType() == Type.INT32) {
          List<Integer> values = rowGroup.readColumn(i).decodeAsInt32();

          assertEquals(100, values.size(),
              "Column " + col.getPathString() + " should have 100 values");

          // Verify no nulls
          long nullCount = values.stream().filter(v -> v == null).count();
          assertEquals(0, nullCount,
              "Column " + col.getPathString() + " should have no nulls");
        }
      }
    }
  }

  @Test
  void testAllStringColumns() throws IOException {
    String filePath = "src/test/data/delta_encoding_required_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read all string columns (9-16) and verify they all have 100 non-null values
      for (int i = 9; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);

        if (col.physicalType() == Type.BYTE_ARRAY) {
          List<String> values = rowGroup.readColumn(i).decodeAsString();

          assertEquals(100, values.size(),
              "Column " + col.getPathString() + " should have 100 values");

          // Verify no nulls
          long nullCount = values.stream().filter(v -> v == null).count();
          assertEquals(0, nullCount,
              "Column " + col.getPathString() + " should have no nulls");
        }
      }
    }
  }

  @Test
  void testMixedColumnReading() throws IOException {
    String filePath = "src/test/data/delta_encoding_required_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read multiple columns to test that decoders don't interfere with each other
      List<Integer> customerSk = rowGroup.readColumn(0).decodeAsInt32();
      List<String> customerId = rowGroup.readColumn(9).decodeAsString();
      List<Integer> birthYear = rowGroup.readColumn(8).decodeAsInt32();
      List<String> firstName = rowGroup.readColumn(11).decodeAsString();

      // Verify all have correct size
      assertEquals(100, customerSk.size());
      assertEquals(100, customerId.size());
      assertEquals(100, birthYear.size());
      assertEquals(100, firstName.size());

      // Spot check first row across different column types
      assertEquals(105, customerSk.get(0));
      assertEquals("AAAAAAAAJGAAAAAA", customerId.get(0));
      assertEquals(1945, birthYear.get(0));
      assertEquals("Frank", firstName.get(0));
    }
  }
}
