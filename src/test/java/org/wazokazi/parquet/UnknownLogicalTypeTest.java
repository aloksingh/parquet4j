package org.wazokazi.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.wazokazi.parquet.model.ColumnDescriptor;
import org.wazokazi.parquet.model.ColumnValues;
import org.wazokazi.parquet.model.Encoding;
import org.wazokazi.parquet.model.Page;
import org.wazokazi.parquet.model.ParquetMetadata;
import org.wazokazi.parquet.model.SchemaDescriptor;
import org.wazokazi.parquet.model.Type;

/**
 * Tests for reading parquet files with unknown logical types.
 * This file tests the unknown-logical-type.parquet file which contains:
 * - Column 1: "column with known type" - STRING type (BYTE_ARRAY with UTF8 logical type)
 * - Column 2: "column with unknown type" - BINARY type with geoarrow.wkb extension (unknown logical type)
 * <p>
 * The file contains 3 rows with dictionary-encoded string values.
 * <p>
 * Expected data:
 * Row 0: {"column with known type": "known string 1", "column with unknown type": "unknown string 1"}
 * Row 1: {"column with known type": "known string 2", "column with unknown type": "unknown string 2"}
 * Row 2: {"column with known type": "known string 3", "column with unknown type": "unknown string 3"}
 */
public class UnknownLogicalTypeTest {

  private static final String TEST_FILE = "src/test/data/unknown-logical-type.parquet";

  @Test
  void testMetadataReading() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      ParquetMetadata metadata = reader.getMetadata();

      // Validate metadata
      assertNotNull(metadata, "Metadata should not be null");
      assertEquals(3, metadata.fileMetadata().numRows(), "Should have 3 rows");

      SchemaDescriptor schema = reader.getSchema();
      assertEquals(2, schema.getNumColumns(), "Should have 2 columns");

      System.out.println("\n=== Testing unknown-logical-type.parquet ===");
      System.out.println("Rows: " + metadata.fileMetadata().numRows());
      System.out.println("Columns: " + schema.getNumColumns());

      // Verify column schemas
      ColumnDescriptor col0 = schema.getColumn(0);
      ColumnDescriptor col1 = schema.getColumn(1);

      System.out.println("\nColumn 0: " + col0.getPathString());
      System.out.println("  Physical type: " + col0.physicalType());

      System.out.println("\nColumn 1: " + col1.getPathString());
      System.out.println("  Physical type: " + col1.physicalType());

      // Both columns should be BYTE_ARRAY
      assertEquals(Type.BYTE_ARRAY, col0.physicalType(), "Column 0 should be BYTE_ARRAY");
      assertEquals(Type.BYTE_ARRAY, col1.physicalType(), "Column 1 should be BYTE_ARRAY");
    }
  }

  @Test
  void testPageStructure() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("\n=== Testing page structure ===");

      // Test both columns
      for (int colIdx = 0; colIdx < schema.getNumColumns(); colIdx++) {
        ColumnDescriptor col = schema.getColumn(colIdx);
        System.out.println("\nColumn " + colIdx + ": " + col.getPathString());

        // Get page reader
        PageReader pageReader = rowGroup.getColumnPageReader(colIdx);
        assertNotNull(pageReader, "PageReader should not be null");

        // Read all pages
        List<Page> pages = pageReader.readAllPages();
        assertNotNull(pages, "Pages list should not be null");
        assertTrue(pages.size() > 0, "Should have at least one page");

        System.out.println("  Pages found: " + pages.size());

        // Examine each page
        for (int i = 0; i < pages.size(); i++) {
          Page page = pages.get(i);
          System.out.println("  Page " + i + ": " + page.getClass().getSimpleName());

          if (page instanceof Page.DictionaryPage dictPage) {
            System.out.println("    Dictionary entries: " + dictPage.numValues());
            System.out.println("    Dictionary encoding: " + dictPage.encoding());
          } else if (page instanceof Page.DataPage dataPage) {
            System.out.println("    Num values: " + dataPage.numValues());
            System.out.println("    Encoding: " + dataPage.encoding());
            System.out.println(
                "    Definition level byte len: " + dataPage.definitionLevelByteLen());
          } else if (page instanceof Page.DataPageV2 v2Page) {
            System.out.println("    Num values: " + v2Page.numValues());
            System.out.println("    Num nulls: " + v2Page.numNulls());
            System.out.println("    Encoding: " + v2Page.encoding());
          }
        }
      }

      System.out.println("\n  ✓ Page structure validated successfully");
    }
  }

  @Test
  void testDictionaryPages() throws IOException {
    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("\n=== Testing dictionary pages ===");

      // Both columns should have dictionary pages
      for (int colIdx = 0; colIdx < schema.getNumColumns(); colIdx++) {
        ColumnDescriptor col = schema.getColumn(colIdx);
        System.out.println("\nColumn " + colIdx + ": " + col.getPathString());

        PageReader pageReader = rowGroup.getColumnPageReader(colIdx);
        List<Page> pages = pageReader.readAllPages();

        // Find dictionary page
        Page.DictionaryPage dictPage = null;
        for (Page page : pages) {
          if (page instanceof Page.DictionaryPage dp) {
            dictPage = dp;
            break;
          }
        }

        assertNotNull(dictPage, "Should have a dictionary page");
        assertEquals(3, dictPage.numValues(), "Dictionary should have 3 entries");
        assertEquals(Encoding.PLAIN, dictPage.encoding(), "Dictionary should use PLAIN encoding");

        System.out.println("  ✓ Dictionary page validated: " + dictPage.numValues() + " entries");
      }
    }
  }

  @Test
  void testDataReadingKnownIssue() throws IOException {
    System.out.println("\n=== Testing data reading (known RLE decoder issue) ===");

    try (SerializedFileReader reader = new SerializedFileReader(TEST_FILE)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Try to read the first column

      ColumnValues values = rowGroup.readColumn(0);
      List<String> stringValues = values.decodeAsString();

      // If we get here, the RLE decoder has been fixed!
      System.out.println("  ✓ Successfully read " + stringValues.size() + " values");
      System.out.println("  Values: " + stringValues);

      // Validate the values
      assertEquals(3, stringValues.size(), "Should have 3 values");

      // DEBUG: Print what we actually get
      System.out.println("  Actual values:");
      for (int i = 0; i < stringValues.size(); i++) {
        System.out.println("    [" + i + "]: \"" + stringValues.get(i) + "\"");
      }

      // The actual data in the file has all rows with index 1
      assertEquals("known string 1", stringValues.get(0));
      assertEquals("known string 2", stringValues.get(1));
      assertEquals("known string 3", stringValues.get(2));

    }
  }
}
