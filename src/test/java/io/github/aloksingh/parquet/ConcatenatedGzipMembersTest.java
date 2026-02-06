package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for reading files with concatenated GZIP members.
 * <p>
 * Test file: concatenated_gzip_members.parquet
 * - Created by: parquet-mr
 * - Rows: 513
 * - Column: 'long_col' - UINT64 (stored as INT64)
 * - Encoding: PLAIN with RLE
 * - Compression: GZIP with concatenated members
 * - Contains sequential values 1, 2, 3, ..., 513
 * <p>
 * Expected data (verified with pyarrow):
 * - Row 0: 1
 * - Row 1: 2
 * - Row 512: 513
 * - All values are sequential
 * - No null values
 */
class ConcatenatedGzipMembersTest {

  @Test
  void testConcatenatedGzipMembersFileStructure() throws IOException {
    String filePath = "src/test/data/concatenated_gzip_members.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = reader.getSchema();

      // Verify file structure
      assertEquals(513, metadata.fileMetadata().numRows());
      assertEquals(1, schema.getNumColumns());

      // Verify column type
      ColumnDescriptor col = schema.getColumn(0);
      assertEquals("long_col", col.getPathString());
      assertEquals(Type.INT64, col.physicalType());
    }
  }

  @Test
  void testConcatenatedGzipMembersSequentialValues() throws IOException {
    String filePath = "src/test/data/concatenated_gzip_members.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> values = rowGroup.readColumn(0).decodeAsInt64();

      assertEquals(513, values.size(), "Should have 513 total values");

      // Verify all values are sequential: 1, 2, 3, ..., 513
      for (int i = 0; i < values.size(); i++) {
        assertEquals(i + 1, values.get(i),
            "Row " + i + " should be " + (i + 1));
      }
    }
  }

  @Test
  void testConcatenatedGzipMembersFirstTenValues() throws IOException {
    String filePath = "src/test/data/concatenated_gzip_members.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> values = rowGroup.readColumn(0).decodeAsInt64();

      // Verify first 10 values: 1, 2, 3, ..., 10
      for (int i = 0; i < 10; i++) {
        assertEquals(i + 1, values.get(i),
            "Row " + i + " should be " + (i + 1));
      }
    }
  }

  @Test
  void testConcatenatedGzipMembersLastTenValues() throws IOException {
    String filePath = "src/test/data/concatenated_gzip_members.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> values = rowGroup.readColumn(0).decodeAsInt64();

      // Verify last 10 values: 504, 505, ..., 513
      for (int i = 503; i < 513; i++) {
        assertEquals(i + 1, values.get(i),
            "Row " + i + " should be " + (i + 1));
      }
    }
  }

  @Test
  void testConcatenatedGzipMembersMiddleValues() throws IOException {
    String filePath = "src/test/data/concatenated_gzip_members.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> values = rowGroup.readColumn(0).decodeAsInt64();

      // Verify middle values around row 256 (halfway point)
      for (int i = 250; i < 260; i++) {
        assertEquals(i + 1, values.get(i),
            "Row " + i + " should be " + (i + 1));
      }
    }
  }

  @Test
  void testConcatenatedGzipMembersNoNulls() throws IOException {
    String filePath = "src/test/data/concatenated_gzip_members.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> values = rowGroup.readColumn(0).decodeAsInt64();

      // Verify no null values
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }
    }
  }

  @Test
  void testConcatenatedGzipMembersValueRange() throws IOException {
    String filePath = "src/test/data/concatenated_gzip_members.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> values = rowGroup.readColumn(0).decodeAsInt64();

      // Find min/max values
      long min = values.stream()
          .mapToLong(Long::longValue)
          .min()
          .orElseThrow();

      long max = values.stream()
          .mapToLong(Long::longValue)
          .max()
          .orElseThrow();

      // Verify range
      assertEquals(1L, min, "Minimum value should be 1");
      assertEquals(513L, max, "Maximum value should be 513");
    }
  }

  @Test
  void testConcatenatedGzipMembersGzipDecompression() throws IOException {
    String filePath = "src/test/data/concatenated_gzip_members.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();

      // Verify GZIP compression is used
      // This is implicit - if decompression fails, reading will fail

      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> values = rowGroup.readColumn(0).decodeAsInt64();

      // If we can successfully read all 513 values, GZIP decompression worked
      assertEquals(513, values.size(), "GZIP decompression should produce all 513 values");

      // Spot-check key values to ensure decompression was correct
      assertEquals(1L, values.get(0), "First value after decompression");
      assertEquals(257L, values.get(256), "Middle value after decompression");
      assertEquals(513L, values.get(512), "Last value after decompression");
    }
  }
}
