package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for Data Page V2 support.
 *
 * <p>Assertions compare against exact golden values (verified with pyarrow). Any
 * unexpected read/decode failure fails the test; nothing is caught and printed away.
 * <p>
 * Test file: datapage_v2.snappy.parquet
 * - Created by: parquet-mr version 1.8.1
 * - Format: Data Page V2 with SNAPPY compression
 * - Rows: 5
 * - Columns: 5 (string, int32, double, bool, list<int32>)
 * - Encodings: PLAIN, RLE_DICTIONARY, DELTA_BINARY_PACKED, RLE
 * <p>
 * Expected data (verified with pyarrow):
 * a  b    c      d          e
 * 0   abc  1  2.0   True  [1, 2, 3]
 * 1   abc  2  3.0   True       None
 * 2   abc  3  4.0   True       None
 * 3  None  4  5.0  False  [1, 2, 3]
 * 4   abc  5  2.0   True     [1, 2]
 */
public class DataPageV2Test {

  private static final String TEST_DATA_DIR = "src/test/data/";

  @Test
  void testDataPageV2Reading() throws IOException {
    String filePath = TEST_DATA_DIR + "datapage_v2.snappy.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();

      assertNotNull(metadata);
      assertEquals(5, metadata.fileMetadata().numRows());

      // Find and read the integer column 'b'
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();
      boolean found = false;
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);

        if (col.getPathString().equals("b") && col.physicalType() == Type.INT32) {
          found = true;
          PageReader pageReader = rowGroup.getColumnPageReader(i);
          List<Page> pages = pageReader.readAllPages();

          assertNotNull(pages);
          assertTrue(pages.size() > 0);

          // Check that we have a DataPageV2
          boolean foundDataPageV2 = false;
          for (Page page : pages) {
            if (page instanceof Page.DataPageV2 v2Page) {
              foundDataPageV2 = true;
              assertEquals(5, v2Page.numValues());
              assertEquals(0, v2Page.numNulls());
              assertEquals(5, v2Page.numRows());
              assertEquals(Encoding.DELTA_BINARY_PACKED, v2Page.encoding());
            }
          }
          assertTrue(foundDataPageV2, "Expected to find at least one Data Page V2");

          // Decoded values must match the golden exactly
          ColumnValues values = rowGroup.readColumn(i);
          assertEquals(Arrays.asList(1, 2, 3, 4, 5), values.decodeAsInt32(),
              "Column 'b' values");
          break;
        }
      }
      assertTrue(found, "Column 'b' (INT32) must exist in the schema");
    }
  }

  /**
   * Tests reading a Data Page V2 whose values section is empty (all rows null).
   *
   * <p>Test file: datapage_v2_empty_datapage.snappy.parquet
   * - Created by: parquet-mr version 1.13.1 (Spark 3.5.5)
   * - Format: Data Page V2 with SNAPPY compression
   * - Rows: 1, Columns: 1 (value: float, nullable), and the single value is NULL.
   * <p>
   * This is a valid file: a Data Page V2 for an all-null page legitimately has zero
   * value bytes after the definition levels, and the strict reader must decode it
   * (decompression of an empty values section must be a no-op).
   */
  @Test
  void testDataPageV2EmptyPage() throws IOException {
    String filePath = TEST_DATA_DIR + "datapage_v2_empty_datapage.snappy.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();

      assertNotNull(metadata);
      assertEquals(1, metadata.fileMetadata().numRows(), "File has exactly one row");

      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();
      assertEquals(1, schema.getNumColumns());
      ColumnDescriptor col = schema.getColumn(0);
      assertEquals("value", col.getPathString());
      assertEquals(Type.FLOAT, col.physicalType());

      PageReader pageReader = rowGroup.getColumnPageReader(0);
      List<Page> pages = pageReader.readAllPages();
      assertEquals(1, pages.size(), "Exactly one data page");
      Page.DataPageV2 v2Page = assertInstanceOf(Page.DataPageV2.class, pages.get(0));
      assertEquals(1, v2Page.numValues());
      assertEquals(1, v2Page.numNulls());
      assertEquals(1, v2Page.numRows());
      assertEquals(0, v2Page.data().remaining(),
          "All-null page has an empty values section");

      // The single value must decode to null
      ColumnValues values = rowGroup.readColumn(0);
      assertEquals(Collections.singletonList(null), values.decodeAsFloat(),
          "The only value is NULL");
    }
  }

  @Test
  void testDataPageV2Metadata() throws IOException {
    String filePath = TEST_DATA_DIR + "datapage_v2.snappy.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      // Verify file metadata
      ParquetMetadata metadata = reader.getMetadata();
      assertNotNull(metadata);
      assertEquals(1, metadata.getNumRowGroups(), "Should have 1 row group");

      ParquetMetadata.FileMetadata fileMetadata = metadata.fileMetadata();
      assertEquals(5, fileMetadata.numRows(), "Should have 5 rows");

      // Verify schema
      SchemaDescriptor schema = fileMetadata.schema();
      assertEquals(5, schema.getNumColumns(), "Should have 5 columns");

      // Verify column names and types
      String[] expectedNames = {"a", "b", "c", "d", "e.list.element"};
      Type[] expectedTypes = {Type.BYTE_ARRAY, Type.INT32, Type.DOUBLE, Type.BOOLEAN, Type.INT32};

      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);

        assertEquals(expectedNames[i], col.getPathString(),
            "Column " + i + " name should be " + expectedNames[i]);
        assertEquals(expectedTypes[i], col.physicalType(),
            "Column " + i + " type should be " + expectedTypes[i]);
      }
    }
  }

  @Test
  void testReadColumnB_DeltaBinaryPacked() throws IOException {
    // Column 'b' uses DELTA_BINARY_PACKED encoding
    // Expected values: [1, 2, 3, 4, 5]
    String filePath = TEST_DATA_DIR + "datapage_v2.snappy.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find column 'b'
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.getPathString().equals("b")) {
          ColumnValues values = rowGroup.readColumn(i);
          List<Integer> intValues = values.decodeAsInt32();

          assertNotNull(intValues, "Values should not be null");
          assertEquals(5, intValues.size(), "Should have 5 values");
          assertEquals(Arrays.asList(1, 2, 3, 4, 5), intValues, "Column 'b' values");
          break;
        }
      }
    }
  }

  @Test
  void testReadColumnC_Double() throws IOException {
    // Column 'c' uses PLAIN dictionary + RLE_DICTIONARY encoding in Data Page V2
    // Expected values: [2.0, 3.0, 4.0, 5.0, 2.0]
    String filePath = TEST_DATA_DIR + "datapage_v2.snappy.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find column 'c'
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.getPathString().equals("c")) {
          ColumnValues values = rowGroup.readColumn(i);
          List<Double> doubleValues = values.decodeAsDouble();

          assertNotNull(doubleValues, "Values should not be null");
          assertEquals(5, doubleValues.size(), "Should have 5 values");
          assertEquals(Arrays.asList(2.0, 3.0, 4.0, 5.0, 2.0), doubleValues,
              "Column 'c' values");
          break;
        }
      }
    }
  }

  @Test
  void testReadColumnD_Boolean() throws IOException {
    // Column 'd' uses RLE encoding in Data Page V2
    // Expected values: [true, true, true, false, true]
    String filePath = TEST_DATA_DIR + "datapage_v2.snappy.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find column 'd'
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.getPathString().equals("d")) {
          ColumnValues values = rowGroup.readColumn(i);
          List<Boolean> boolValues = values.decodeAsBoolean();

          assertNotNull(boolValues, "Values should not be null");
          assertEquals(5, boolValues.size(), "Should have 5 values");
          assertEquals(Arrays.asList(true, true, true, false, true), boolValues,
              "Column 'd' values");
          break;
        }
      }
    }
  }

  @Test
  void testReadColumnA_String() throws IOException {
    // Column 'a' uses PLAIN dictionary + RLE_DICTIONARY encoding
    // Expected values: ["abc", "abc", "abc", null, "abc"]
    String filePath = TEST_DATA_DIR + "datapage_v2.snappy.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find column 'a'
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.getPathString().equals("a")) {
          ColumnValues values = rowGroup.readColumn(i);
          List<String> stringValues = values.decodeAsString();

          assertNotNull(stringValues, "Values should not be null");
          assertEquals(Arrays.asList("abc", "abc", "abc", null, "abc"), stringValues,
              "Column 'a' values");
          break;
        }
      }
    }
  }

  @Test
  void testAllColumnsPresent() throws IOException {
    // Expected page structure per column of datapage_v2.snappy.parquet:
    // dictionary-encoded columns have a dictionary page plus one data page.
    Map<String, Integer> expectedTotalPages =
        Map.of("a", 2, "b", 1, "c", 2, "d", 1, "e.list.element", 2);
    Map<String, Encoding> expectedDataPageEncoding = Map.of(
        "a", Encoding.RLE_DICTIONARY,
        "b", Encoding.DELTA_BINARY_PACKED,
        "c", Encoding.RLE_DICTIONARY,
        "d", Encoding.RLE,
        "e.list.element", Encoding.RLE_DICTIONARY);

    String filePath = TEST_DATA_DIR + "datapage_v2.snappy.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        String name = col.getPathString();

        PageReader pageReader = rowGroup.getColumnPageReader(i);
        List<Page> pages = pageReader.readAllPages();
        assertNotNull(pages, "Pages should not be null for column " + name);
        assertEquals((int) expectedTotalPages.get(name), pages.size(),
            "Total page count for column " + name);

        int v2PageCount = 0;
        for (Page page : pages) {
          if (page instanceof Page.DataPageV2 v2Page) {
            v2PageCount++;
            assertEquals(expectedDataPageEncoding.get(name), v2Page.encoding(),
                "Data page encoding for column " + name);
          }
        }
        assertEquals(1, v2PageCount, "Data Page V2 count for column " + name);
      }
    }
  }

  @Test
  void testDataPageV2Properties() throws IOException {
    String filePath = TEST_DATA_DIR + "datapage_v2.snappy.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Check column 'b' which should have Data Page V2
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.getPathString().equals("b")) {
          PageReader pageReader = rowGroup.getColumnPageReader(i);
          List<Page> pages = pageReader.readAllPages();
          boolean foundDataPageV2 = false;
          for (Page page : pages) {
            if (page instanceof Page.DataPageV2 v2Page) {
              foundDataPageV2 = true;
              assertEquals(5, v2Page.numValues(), "Should have 5 values");
              assertEquals(0, v2Page.numNulls(), "Column 'b' is not null, should have 0 nulls");
              assertEquals(Encoding.DELTA_BINARY_PACKED, v2Page.encoding(),
                  "Column 'b' should use DELTA_BINARY_PACKED encoding");
            }
          }
          assertTrue(foundDataPageV2, "Column 'b' must have at least one Data Page V2");
          break;
        }
      }
    }
  }

  /**
   * Tests reading a Data Page V2 file with ZSTD compression and all NULL values.
   * <p>
   * Test file: page_v2_empty_compressed.parquet
   * - Created by: parquet-cpp-arrow version 14.0.2
   * - Format: Data Page V2 (format version 2.6) with ZSTD compression
   * - Rows: 10
   * - Columns: 1 (integer_column: int32)
   * - All values are NULL
   * - Encodings: PLAIN, RLE, RLE_DICTIONARY
   * <p>
   * Expected data (verified with pyarrow):
   * All 10 rows have NULL values for the integer_column
   */
  @Test
  void testPageV2EmptyCompressedZstd() throws IOException {
    String filePath = TEST_DATA_DIR + "page_v2_empty_compressed.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();

      // Verify basic file metadata
      assertNotNull(metadata, "Metadata should not be null");
      assertEquals(10, metadata.fileMetadata().numRows(), "Should have 10 rows");
      assertEquals(1, metadata.getNumRowGroups(), "Should have 1 row group");

      // Verify schema
      SchemaDescriptor schema = reader.getSchema();
      assertEquals(1, schema.getNumColumns(), "Should have 1 column");

      ColumnDescriptor col = schema.getColumn(0);
      assertEquals("integer_column", col.getPathString(),
          "Column should be named 'integer_column'");
      assertEquals(Type.INT32, col.physicalType(), "Column should be INT32 type");

      // Read the row group and pages
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      PageReader pageReader = rowGroup.getColumnPageReader(0);
      List<Page> pages = pageReader.readAllPages();

      assertNotNull(pages, "Pages should not be null");
      assertTrue(pages.size() > 0, "Should have at least one page");

      // Verify Data Page V2 properties
      boolean foundDataPageV2 = false;
      for (Page page : pages) {
        if (page instanceof Page.DataPageV2 v2Page) {
          foundDataPageV2 = true;
          // All values should be null
          assertEquals(10, v2Page.numNulls(), "All 10 values should be null");
          assertTrue(v2Page.isCompressed(), "Page should be compressed");
        }
      }
      assertTrue(foundDataPageV2, "Expected to find at least one Data Page V2");

      // All 10 decoded values must be exactly null
      ColumnValues values = rowGroup.readColumn(0);
      List<Integer> intValues = values.decodeAsInt32();
      assertNotNull(intValues, "Decoded values should not be null");
      assertEquals(10, intValues.size(), "Should decode 10 values");
      for (int i = 0; i < intValues.size(); i++) {
        assertEquals(null, intValues.get(i), "Value at index " + i + " should be null");
      }
    }
  }
}
