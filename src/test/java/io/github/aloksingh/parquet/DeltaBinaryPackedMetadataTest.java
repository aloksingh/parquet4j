package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * Tests for DELTA_BINARY_PACKED file metadata reading.
 * This validates the Java Parquet reader can open and read metadata from
 * DELTA_BINARY_PACKED encoded files.
 * <p>
 * Full decoding tests are in DeltaBinaryPackedTest.
 */
class DeltaBinaryPackedMetadataTest {

  @Test
  void testCanOpenDeltaBinaryPackedFile() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();

      assertNotNull(metadata);
      assertEquals(200, metadata.fileMetadata().numRows());

      SchemaDescriptor schema = reader.getSchema();
      assertEquals(66, schema.getNumColumns());

      // Verify column names
      assertTrue(findColumnIndex(schema, "bitwidth0") >= 0);
      assertTrue(findColumnIndex(schema, "bitwidth1") >= 0);
      assertTrue(findColumnIndex(schema, "bitwidth64") >= 0);
      assertTrue(findColumnIndex(schema, "int_value") >= 0);
    }
  }

  @Test
  void testCanReadPageMetadata() throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      int columnIndex = findColumnIndex(schema, "bitwidth0");
      assertNotEquals(-1, columnIndex);

      PageReader pageReader = rowGroup.getColumnPageReader(columnIndex);
      java.util.List<Page> pages = pageReader.readAllPages();

      assertTrue(pages.size() > 0, "Should have at least one page");

      // Verify it's a DataPageV2 with DELTA_BINARY_PACKED encoding
      Page firstPage = pages.get(0);
      if (firstPage instanceof Page.DataPageV2 dataPageV2) {
        assertEquals(200, dataPageV2.numValues());
        assertEquals(Encoding.DELTA_BINARY_PACKED, dataPageV2.encoding());
      } else {
        fail("Expected DataPageV2");
      }
    }
  }

  private int findColumnIndex(SchemaDescriptor schema, String columnName) {
    for (int i = 0; i < schema.getNumColumns(); i++) {
      ColumnDescriptor col = schema.getColumn(i);
      if (col.getPathString().equals(columnName)) {
        return i;
      }
    }
    return -1;
  }
}
