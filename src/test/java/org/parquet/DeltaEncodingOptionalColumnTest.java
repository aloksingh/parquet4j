package org.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ParquetMetadata;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.Type;

/**
 * Tests for reading files with DELTA_BINARY_PACKED and DELTA_BYTE_ARRAY encodings
 * on optional (nullable) columns.
 * <p>
 * Test file: delta_encoding_optional_column.parquet
 * - Created by: parquet-mr version 1.10.0
 * - Format version: 1.0
 * - Rows: 100
 * - Columns: 17 (9 int64, 8 string)
 * - Several columns contain null values (optional fields)
 * - Compression: UNCOMPRESSED
 * - Integer encoding: DELTA_BINARY_PACKED
 * - String encoding: DELTA_BYTE_ARRAY
 * <p>
 * Expected data (verified with pyarrow):
 * - c_customer_sk: [100, 99, 98, 97, 96, ...] (descending from 100 to 1, no nulls)
 * - c_current_cdemo_sk: Has 3 null values at rows 66, 77, 85
 * - c_current_hdemo_sk: Has 2 null values at rows 60, 66
 * - c_birth_day: Has 3 null values at rows 66, 77, 85
 * - c_birth_month: Has 3 null values at rows 55, 60, 66
 * - c_birth_year: Has 3 null values at rows 55, 66, 77
 * - c_customer_id: No nulls
 * - c_salutation: Has 3 null values at rows 55, 60, 66
 * - c_first_name: Has 3 null values at rows 55, 66, 77
 * - c_last_name: Has 1 null value at row 85
 * - c_preferred_cust_flag: Has 4 null values at rows 55, 60, 66, 85
 * - c_birth_country: Has 4 null values at rows 60, 66, 77, 85
 * - c_email_address: Has 3 null values at rows 55, 60, 85
 * - c_last_review_date: Has 3 null values at rows 55, 60, 77
 * <p>
 * Both DELTA_BINARY_PACKED (for integers) and DELTA_BYTE_ARRAY (for strings)
 * correctly handle optional columns by encoding only non-null values and using
 * definition levels to indicate null positions.
 */
public class DeltaEncodingOptionalColumnTest {

  @Test
  void testFileStructure() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = reader.getSchema();

      // Verify file structure
      assertEquals(100, metadata.fileMetadata().numRows());
      assertEquals(17, schema.getNumColumns());

      // Verify integer columns (note: file uses INT64/int64, which maps to INT64 physical type)
      assertEquals("c_customer_sk", schema.getColumn(0).getPathString());
      assertEquals(Type.INT64, schema.getColumn(0).physicalType());

      // Verify string columns use BYTE_ARRAY
      assertEquals("c_customer_id", schema.getColumn(9).getPathString());
      assertEquals(Type.BYTE_ARRAY, schema.getColumn(9).physicalType());
    }
  }

  @Test
  void testDeltaBinaryPackedInt64WithNoNulls() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read c_customer_sk column (index 0) - descending sequence with no nulls
      List<Long> values = rowGroup.readColumn(0).decodeAsInt64();

      assertEquals(100, values.size(), "Should have 100 values");

      // Verify first 5 values (descending from 100)
      assertEquals(100L, values.get(0));
      assertEquals(99L, values.get(1));
      assertEquals(98L, values.get(2));
      assertEquals(97L, values.get(3));
      assertEquals(96L, values.get(4));

      // Verify last value
      assertEquals(1L, values.get(99));

      // Verify no nulls
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }

      // Verify descending sequence
      for (int i = 0; i < values.size() - 1; i++) {
        assertEquals(100L - i, values.get(i).longValue(),
            "Values should be in descending order");
      }
    }
  }

  @Test
  void testDeltaBinaryPackedInt64WithNulls() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read c_current_cdemo_sk column (index 1) - has 3 nulls at rows 66, 77, 85
      List<Long> values = rowGroup.readColumn(1).decodeAsInt64();

      assertEquals(100, values.size(), "Should have 100 values");

      // Verify first few values
      assertEquals(1254468L, values.get(0));
      assertEquals(622676L, values.get(1));
      assertEquals(574977L, values.get(2));

      assertEquals(339036L, values.get(65));

      // Verify null values at expected positions
      assertNull(values.get(66), "Row 66 should be null");
      assertNull(values.get(77), "Row 77 should be null");
      assertNull(values.get(85), "Row 85 should be null");

      // Count nulls
      long nullCount = values.stream().filter(v -> v == null).count();
      assertEquals(3, nullCount, "Should have exactly 3 null values");

      // Verify value ranges for non-null values
      long minValue = values.stream()
          .filter(v -> v != null)
          .mapToLong(Long::longValue)
          .min()
          .orElseThrow();
      long maxValue = values.stream()
          .filter(v -> v != null)
          .mapToLong(Long::longValue)
          .max()
          .orElseThrow();

      assertEquals(8817L, minValue, "Min value should be 8817");
      assertEquals(1895444L, maxValue, "Max value should be 1895444");
    }
  }

  @Test
  void testDeltaBinaryPackedBirthColumnsWithNulls() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Test c_birth_day (column 6) - has 3 nulls at rows 66, 77, 85
      List<Long> birthDays = rowGroup.readColumn(6).decodeAsInt64();
      assertEquals(100, birthDays.size());
      assertEquals(13L, birthDays.get(0));
      assertEquals(9L, birthDays.get(1));
      assertEquals(23L, birthDays.get(2));
      assertNull(birthDays.get(66), "Row 66 c_birth_day should be null");
      assertNull(birthDays.get(77), "Row 77 c_birth_day should be null");
      assertNull(birthDays.get(85), "Row 85 c_birth_day should be null");
      assertEquals(3, birthDays.stream().filter(v -> v == null).count());

      // Test c_birth_month (column 7) - has 3 nulls at rows 55, 60, 66
      List<Long> birthMonths = rowGroup.readColumn(7).decodeAsInt64();
      assertEquals(100, birthMonths.size());
      assertEquals(7L, birthMonths.get(0));
      assertEquals(12L, birthMonths.get(1));
      assertEquals(6L, birthMonths.get(2));
      assertNull(birthMonths.get(55), "Row 55 c_birth_month should be null");
      assertNull(birthMonths.get(60), "Row 60 c_birth_month should be null");
      assertNull(birthMonths.get(66), "Row 66 c_birth_month should be null");
      assertEquals(3, birthMonths.stream().filter(v -> v == null).count());

      // Test c_birth_year (column 8) - has 3 nulls at rows 55, 66, 77
      List<Long> birthYears = rowGroup.readColumn(8).decodeAsInt64();
      assertEquals(100, birthYears.size());
      assertEquals(1958L, birthYears.get(0));
      assertEquals(1961L, birthYears.get(1));
      assertEquals(1965L, birthYears.get(2));
      assertNull(birthYears.get(55), "Row 55 c_birth_year should be null");
      assertNull(birthYears.get(66), "Row 66 c_birth_year should be null");
      assertNull(birthYears.get(77), "Row 77 c_birth_year should be null");
      assertEquals(3, birthYears.stream().filter(v -> v == null).count());

      // Verify value ranges for non-null values
      long minYear = birthYears.stream()
          .filter(v -> v != null)
          .mapToLong(Long::longValue)
          .min()
          .orElseThrow();
      long maxYear = birthYears.stream()
          .filter(v -> v != null)
          .mapToLong(Long::longValue)
          .max()
          .orElseThrow();
      assertEquals(1925L, minYear, "Min birth year should be 1925");
      assertEquals(1991L, maxYear, "Max birth year should be 1991");
    }
  }

  @Test
  void testDeltaByteArrayCustomerIdNoNulls() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read c_customer_id column (index 9) - no nulls
      List<String> values = rowGroup.readColumn(9).decodeAsString();

      assertEquals(100, values.size(), "Should have 100 values");

      // Verify first 5 values
      assertEquals("AAAAAAAAEGAAAAAA", values.get(0));
      assertEquals("AAAAAAAADGAAAAAA", values.get(1));
      assertEquals("AAAAAAAACGAAAAAA", values.get(2));
      assertEquals("AAAAAAAABGAAAAAA", values.get(3));
      assertEquals("AAAAAAAAAGAAAAAA", values.get(4));

      // Verify no nulls
      long nullCount = values.stream().filter(v -> v == null).count();
      assertEquals(0, nullCount, "c_customer_id should have no nulls");
    }
  }

  @Test
  void testDeltaByteArrayNamesWithNulls() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Test c_salutation (column 10) - has 3 nulls at rows 55, 60, 66
      List<String> salutations = rowGroup.readColumn(10).decodeAsString();
      assertEquals(100, salutations.size());
      assertEquals("Ms.", salutations.get(0));
      assertEquals("Sir", salutations.get(1));
      assertEquals("Dr.", salutations.get(2));
      assertNull(salutations.get(55), "Row 55 c_salutation should be null");
      assertNull(salutations.get(60), "Row 60 c_salutation should be null");
      assertNull(salutations.get(66), "Row 66 c_salutation should be null");
      assertEquals(3, salutations.stream().filter(v -> v == null).count());

      // Test c_first_name (column 11) - has 3 nulls at rows 55, 66, 77
      List<String> firstNames = rowGroup.readColumn(11).decodeAsString();
      assertEquals(100, firstNames.size());
      assertEquals("Jeannette", firstNames.get(0));
      assertEquals("Austin", firstNames.get(1));
      assertEquals("David", firstNames.get(2));
      assertNull(firstNames.get(55), "Row 55 c_first_name should be null");
      assertNull(firstNames.get(66), "Row 66 c_first_name should be null");
      assertNull(firstNames.get(77), "Row 77 c_first_name should be null");
      assertEquals(3, firstNames.stream().filter(v -> v == null).count());

      // Test c_last_name (column 12) - has 1 null at row 85
      List<String> lastNames = rowGroup.readColumn(12).decodeAsString();
      assertEquals(100, lastNames.size());
      assertEquals("Johnson", lastNames.get(0));
      assertEquals("Tran", lastNames.get(1));
      assertEquals("Lewis", lastNames.get(2));
      assertNull(lastNames.get(85), "Row 85 c_last_name should be null");
      assertEquals(1, lastNames.stream().filter(v -> v == null).count());
    }
  }

  @Test
  void testDeltaByteArrayEmailAndCountryWithNulls() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Test c_birth_country (column 14) - has 4 nulls at rows 60, 66, 77, 85
      List<String> countries = rowGroup.readColumn(14).decodeAsString();
      assertEquals(100, countries.size());
      assertEquals("BANGLADESH", countries.get(0));
      assertEquals("NAMIBIA", countries.get(1));
      assertEquals("KIRIBATI", countries.get(2));
      assertNull(countries.get(60), "Row 60 c_birth_country should be null");
      assertNull(countries.get(66), "Row 66 c_birth_country should be null");
      assertNull(countries.get(77), "Row 77 c_birth_country should be null");
      assertNull(countries.get(85), "Row 85 c_birth_country should be null");
      assertEquals(4, countries.stream().filter(v -> v == null).count());

      // Test c_email_address (column 15) - has 3 nulls at rows 55, 60, 85
      List<String> emails = rowGroup.readColumn(15).decodeAsString();
      assertEquals(100, emails.size());
      assertEquals("Jeannette.Johnson@8BvSqgp.com", emails.get(0));
      assertEquals("Austin.Tran@ect7cnjLsucbd.edu", emails.get(1));
      assertEquals("David.Lewis@5mhvq.org", emails.get(2));
      assertNull(emails.get(55), "Row 55 c_email_address should be null");
      assertNull(emails.get(60), "Row 60 c_email_address should be null");
      assertNull(emails.get(85), "Row 85 c_email_address should be null");
      assertEquals(3, emails.stream().filter(v -> v == null).count());

      // Test c_last_review_date (column 16) - has 3 nulls at rows 55, 60, 77
      List<String> reviewDates = rowGroup.readColumn(16).decodeAsString();
      assertEquals(100, reviewDates.size());
      assertEquals("2452635", reviewDates.get(0));
      assertEquals("2452437", reviewDates.get(1));
      assertEquals("2452558", reviewDates.get(2));
      assertNull(reviewDates.get(55), "Row 55 c_last_review_date should be null");
      assertNull(reviewDates.get(60), "Row 60 c_last_review_date should be null");
      assertNull(reviewDates.get(77), "Row 77 c_last_review_date should be null");
      assertEquals(3, reviewDates.stream().filter(v -> v == null).count());
    }
  }

  @Test
  void testPreferredCustFlagWithNulls() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Test c_preferred_cust_flag (column 13) - has 4 nulls at rows 55, 60, 66, 85
      List<String> flags = rowGroup.readColumn(13).decodeAsString();
      assertEquals(100, flags.size());
      assertEquals("Y", flags.get(0));
      assertEquals("Y", flags.get(1));
      assertEquals("N", flags.get(2));
      assertNull(flags.get(55), "Row 55 c_preferred_cust_flag should be null");
      assertNull(flags.get(60), "Row 60 c_preferred_cust_flag should be null");
      assertNull(flags.get(66), "Row 66 c_preferred_cust_flag should be null");
      assertNull(flags.get(85), "Row 85 c_preferred_cust_flag should be null");
      assertEquals(4, flags.stream().filter(v -> v == null).count());
    }
  }

  @Test
  void testMixedColumnReadingWithNulls() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read multiple columns to test that decoders handle nulls correctly
      List<Long> customerSk = rowGroup.readColumn(0).decodeAsInt64();
      List<String> customerId = rowGroup.readColumn(9).decodeAsString();
      List<String> firstName = rowGroup.readColumn(11).decodeAsString();
      List<String> lastName = rowGroup.readColumn(12).decodeAsString();

      // Verify all have correct size
      assertEquals(100, customerSk.size());
      assertEquals(100, customerId.size());
      assertEquals(100, firstName.size());
      assertEquals(100, lastName.size());

      // Spot check first row across different column types (all non-null)
      assertEquals(100L, customerSk.get(0));
      assertEquals("AAAAAAAAEGAAAAAA", customerId.get(0));
      assertEquals("Jeannette", firstName.get(0));
      assertEquals("Johnson", lastName.get(0));

      // Spot check row with some nulls (row 66)
      assertEquals(34L, customerSk.get(66), "c_customer_sk at row 66 should be 34");
      assertEquals("AAAAAAAACCAAAAAA", customerId.get(66));
      assertNull(firstName.get(66), "c_first_name at row 66 should be null");
      assertNotNull(lastName.get(66), "c_last_name at row 66 should not be null");
      assertEquals("Woods", lastName.get(66));
    }
  }

  @Test
  void testRowWithMostNulls() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Row 66 has many null values - test all columns for this row
      int rowIndex = 66;

      // Integer columns
      assertEquals(34L, rowGroup.readColumn(0).decodeAsInt64().get(rowIndex)); // c_customer_sk
      assertNull(rowGroup.readColumn(1).decodeAsInt64().get(rowIndex)); // c_current_cdemo_sk - null
      assertNull(rowGroup.readColumn(2).decodeAsInt64().get(rowIndex)); // c_current_hdemo_sk - null
      assertEquals(37501L,
          rowGroup.readColumn(3).decodeAsInt64().get(rowIndex)); // c_current_addr_sk
      assertNull(rowGroup.readColumn(6).decodeAsInt64().get(rowIndex)); // c_birth_day - null
      assertNull(rowGroup.readColumn(7).decodeAsInt64().get(rowIndex)); // c_birth_month - null
      assertNull(rowGroup.readColumn(8).decodeAsInt64().get(rowIndex)); // c_birth_year - null

      // String columns
      assertEquals("AAAAAAAACCAAAAAA",
          rowGroup.readColumn(9).decodeAsString().get(rowIndex)); // c_customer_id
      assertNull(rowGroup.readColumn(10).decodeAsString().get(rowIndex)); // c_salutation - null
      assertNull(rowGroup.readColumn(11).decodeAsString().get(rowIndex)); // c_first_name - null
      assertEquals("Woods", rowGroup.readColumn(12).decodeAsString().get(rowIndex)); // c_last_name
      assertNull(
          rowGroup.readColumn(13).decodeAsString().get(rowIndex)); // c_preferred_cust_flag - null
      assertNull(rowGroup.readColumn(14).decodeAsString().get(rowIndex)); // c_birth_country - null
    }
  }

  @Test
  void testAllIntegerColumnsNullCounts() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Verify expected null counts for integer columns (verified with pyarrow)
      int[] expectedNullCounts = {0, 3, 2, 0, 1, 1, 3, 3, 3};

      for (int i = 0; i < 9; i++) {
        ColumnDescriptor col = schema.getColumn(i);

        if (col.physicalType() == Type.INT64) {
          List<Long> values = rowGroup.readColumn(i).decodeAsInt64();

          assertEquals(100, values.size(),
              "Column " + col.getPathString() + " should have 100 values");

          long nullCount = values.stream().filter(v -> v == null).count();
          assertEquals(expectedNullCounts[i], nullCount,
              "Column " + col.getPathString() + " should have " + expectedNullCounts[i] + " nulls");
        }
      }
    }
  }

  @Test
  void testAllStringColumnsNullCounts() throws IOException {
    String filePath = "src/test/data/delta_encoding_optional_column.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Verify expected null counts for string columns (columns 9-16)
      int[] expectedNullCounts = {0, 3, 3, 1, 4, 4, 3, 3};

      for (int i = 0; i < 8; i++) {
        int colIndex = i + 9;
        ColumnDescriptor col = schema.getColumn(colIndex);

        if (col.physicalType() == Type.BYTE_ARRAY) {
          List<String> values = rowGroup.readColumn(colIndex).decodeAsString();
          long nullCount = values.stream().filter(v -> v == null).count();

          assertEquals(expectedNullCounts[i], nullCount,
              "Column " + col.getPathString() + " should have " + expectedNullCounts[i] + " nulls");
        }
      }
    }
  }
}
