package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for reading DELTA_BYTE_ARRAY encoding.
 * <p>
 * Test file: delta_byte_array.parquet
 * - Created by: parquet-mr version 1.10.0
 * - Format version: 1.0
 * - Rows: 1000
 * - Columns: 9 string columns (c_customer_id, c_salutation, c_first_name, c_last_name,
 * c_preferred_cust_flag, c_birth_country, c_login, c_email_address, c_last_review_date)
 * - Compression: UNCOMPRESSED
 * - Encoding: DELTA_BYTE_ARRAY
 * <p>
 * Expected data (verified with pyarrow):
 * - Column 'c_customer_id' (first 10): ['AAAAAAAAIODAAAAA', 'AAAAAAAAHODAAAAA', 'AAAAAAAAGODAAAAA',
 * 'AAAAAAAAFODAAAAA', 'AAAAAAAAEODAAAAA', 'AAAAAAAADODAAAAA', 'AAAAAAAACODAAAAA', 'AAAAAAAABODAAAAA',
 * 'AAAAAAAAAODAAAAA', 'AAAAAAAAPNDAAAAA']
 * - Column 'c_salutation' has nulls (30 nulls out of 1000 rows)
 * - Column 'c_first_name' (first 10): ['Mark', 'Lisa', 'Evelyn', 'Harvey', 'Chris', 'Richie',
 * 'Geneva', 'Joseph', 'Elnora', 'William']
 */

public class DeltaByteArrayTest {

  @Test
  void testDeltaByteArrayFileStructure() throws IOException {
    String filePath = "src/test/data/delta_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = reader.getSchema();

      // Verify file structure
      assertEquals(1000, metadata.fileMetadata().numRows());
      assertEquals(9, schema.getNumColumns());

      // Verify first column
      assertEquals("c_customer_id", schema.getColumn(0).getPathString());
      assertEquals(Type.BYTE_ARRAY, schema.getColumn(0).physicalType());

      // Verify second column
      assertEquals("c_salutation", schema.getColumn(1).getPathString());
      assertEquals(Type.BYTE_ARRAY, schema.getColumn(1).physicalType());
    }
  }

  @Test
  void testDeltaByteArrayCustomerId() throws IOException {
    String filePath = "src/test/data/delta_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read c_customer_id column (index 0)
      List<String> values = rowGroup.readColumn(0).decodeAsString();

      assertEquals(1000, values.size(), "Should have 1000 values");

      // Verify first 10 values
      assertEquals("AAAAAAAAIODAAAAA", values.get(0));
      assertEquals("AAAAAAAAHODAAAAA", values.get(1));
      assertEquals("AAAAAAAAGODAAAAA", values.get(2));
      assertEquals("AAAAAAAAFODAAAAA", values.get(3));
      assertEquals("AAAAAAAAEODAAAAA", values.get(4));
      assertEquals("AAAAAAAADODAAAAA", values.get(5));
      assertEquals("AAAAAAAACODAAAAA", values.get(6));
      assertEquals("AAAAAAAABODAAAAA", values.get(7));
      assertEquals("AAAAAAAAAODAAAAA", values.get(8));
      assertEquals("AAAAAAAAPNDAAAAA", values.get(9));

      // Verify no nulls in customer ID column
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }
    }
  }

  @Test
  void testDeltaByteArraySalutationWithNulls() throws IOException {
    String filePath = "src/test/data/delta_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read c_salutation column (index 1) - has nulls
      List<String> values = rowGroup.readColumn(1).decodeAsString();

      assertEquals(1000, values.size(), "Should have 1000 values");

      // Verify first 10 values (including a null at position 6)
      assertEquals("Sir", values.get(0));
      assertEquals("Mrs.", values.get(1));
      assertEquals("Ms.", values.get(2));
      assertEquals("Sir", values.get(3));
      assertEquals("Dr.", values.get(4));
      assertEquals("Sir", values.get(5));
      assertNull(values.get(6), "Row 6 should be null");
      assertEquals("Mr.", values.get(7));
      assertEquals("Mrs.", values.get(8));
      assertEquals("Sir", values.get(9));

      // Count nulls
      long nullCount = values.stream().filter(v -> v == null).count();
      assertEquals(30, nullCount, "Should have 30 null values");
    }
  }

  @Test
  void testDeltaByteArrayFirstName() throws IOException {
    String filePath = "src/test/data/delta_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read c_first_name column (index 2)
      List<String> values = rowGroup.readColumn(2).decodeAsString();

      assertEquals(1000, values.size(), "Should have 1000 values");

      // Verify first 10 values
      assertEquals("Mark", values.get(0));
      assertEquals("Lisa", values.get(1));
      assertEquals("Evelyn", values.get(2));
      assertEquals("Harvey", values.get(3));
      assertEquals("Chris", values.get(4));
      assertEquals("Richie", values.get(5));
      assertEquals("Geneva", values.get(6));
      assertEquals("Joseph", values.get(7));
      assertEquals("Elnora", values.get(8));
      assertEquals("William", values.get(9));
    }
  }

  @Test
  void testDeltaByteArrayAllColumns() throws IOException {
    String filePath = "src/test/data/delta_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      SchemaDescriptor schema = reader.getSchema();
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Verify we can read all columns with DELTA_BYTE_ARRAY encoding
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        ColumnValues columnValues = rowGroup.readColumn(i);

        if (col.physicalType() == Type.BYTE_ARRAY) {
          List<String> values = columnValues.decodeAsString();
          assertEquals(1000, values.size(),
              "Column " + col.getPathString() + " should have 1000 values");
        }
      }
    }
  }

  @Test
  void testDeltaByteArrayValueRanges() throws IOException {
    String filePath = "src/test/data/delta_byte_array.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Test c_salutation - should have specific values
      List<String> salutations = rowGroup.readColumn(1).decodeAsString();

      // Get unique non-null salutations
      long uniqueCount = salutations.stream()
          .filter(v -> v != null)
          .distinct()
          .count();

      // Should have these salutations: Sir, Mrs., Ms., Dr., Mr., Miss
      assertTrue(uniqueCount >= 5 && uniqueCount <= 7,
          "Should have 5-7 unique salutations, got " + uniqueCount);

      // Verify some expected salutations exist
      assertTrue(salutations.contains("Sir"));
      assertTrue(salutations.contains("Mrs."));
      assertTrue(salutations.contains("Mr."));
    }
  }
}
