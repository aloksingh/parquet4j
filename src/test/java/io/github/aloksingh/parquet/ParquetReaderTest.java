package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for Parquet reader implementation.
 *
 * <p>Value assertions compare against exact golden values (pyarrow exports of the
 * corpus files). Unexpected read/decode failures fail the test.
 */
class ParquetReaderTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  @Test
  void testReadMetadata() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertNotNull(metadata);
      assertEquals(1, metadata.getNumRowGroups());
      assertEquals(8, metadata.fileMetadata().numRows());
      assertEquals(11, metadata.fileMetadata().schema().getNumColumns());
    }
  }

  @Test
  void testReadAllTypesPlain() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      assertEquals(1, reader.getNumRowGroups());

      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      assertEquals(11, rowGroup.getNumColumns());
      assertEquals(8, rowGroup.getNumRows());

      // Every column must yield at least one page
      SchemaDescriptor schema = reader.getSchema();
      for (int i = 0; i < schema.getNumColumns(); i++) {
        PageReader pageReader = rowGroup.getColumnPageReader(i);
        List<Page> pages = pageReader.readAllPages();

        assertNotNull(pages);
        assertTrue(pages.size() > 0,
            "Column " + schema.getColumn(i).getPathString() + " must have pages");
      }
    }
  }

  @Test
  void testReadInt32Values() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Exact golden values for every INT32 column
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.physicalType() == Type.INT32) {
          ColumnValues values = rowGroup.readColumn(i);
          List<Integer> int32Values = values.decodeAsInt32();

          assertNotNull(int32Values);
          List<Integer> expected = switch (col.getPathString()) {
            case "id" -> List.of(4, 5, 6, 7, 2, 3, 0, 1);
            case "tinyint_col", "smallint_col", "int_col" -> List.of(0, 1, 0, 1, 0, 1, 0, 1);
            default -> throw new AssertionError(
                "Unexpected INT32 column " + col.getPathString());
          };
          assertEquals(expected, int32Values, col.getPathString() + " values");
        }
      }
    }
  }

  @Test
  void testReadStringValues() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Exact golden values for every BYTE_ARRAY column
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.physicalType() == Type.BYTE_ARRAY) {
          ColumnValues values = rowGroup.readColumn(i);
          List<String> stringValues = values.decodeAsString();

          assertNotNull(stringValues);
          List<String> expected = switch (col.getPathString()) {
            case "date_string_col" -> List.of("03/01/09", "03/01/09", "04/01/09",
                "04/01/09", "02/01/09", "02/01/09", "01/01/09", "01/01/09");
            case "string_col" -> List.of("0", "1", "0", "1", "0", "1", "0", "1");
            default -> throw new AssertionError(
                "Unexpected BYTE_ARRAY column " + col.getPathString());
          };
          assertEquals(expected, stringValues, col.getPathString() + " values");
        }
      }
    }
  }

  @Test
  void testReadBooleanValues() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find the BOOLEAN column and assert its exact golden values
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.physicalType() == Type.BOOLEAN) {
          ColumnValues values = rowGroup.readColumn(i);
          List<Boolean> boolValues = values.decodeAsBoolean();

          assertNotNull(boolValues);
          assertEquals(List.of(true, false, true, false, true, false, true, false),
              boolValues, col.getPathString() + " values");
          break;
        }
      }
    }
  }

  @Test
  void testReadFloatValues() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find the FLOAT column and assert its exact golden values
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        if (col.physicalType() == Type.FLOAT) {
          ColumnValues values = rowGroup.readColumn(i);
          List<Float> floatValues = values.decodeAsFloat();

          assertNotNull(floatValues);
          assertEquals(List.of(0.0f, 1.1f, 0.0f, 1.1f, 0.0f, 1.1f, 0.0f, 1.1f),
              floatValues, col.getPathString() + " values");
          break;
        }
      }
    }
  }

  @Test
  void testReadSnappyCompressed() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_plain.snappy.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(1, metadata.getNumRowGroups());
      assertEquals(2, metadata.fileMetadata().numRows());

      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      assertEquals(2, rowGroup.getNumRows());

      // Read first column and verify its exact golden values (id)
      ColumnValues values = rowGroup.readColumn(0);
      assertEquals(List.of(6, 7), values.decodeAsInt32(), "id values");
    }
  }

  @Test
  void testReadMultipleFiles() throws IOException {
    // Exact golden row/column counts per file; read failures fail the test.
    Object[][] expected = {
        {"alltypes_plain.parquet", 8, 11},
        {"binary.parquet", 12, 1},
        {"nulls.snappy.parquet", 8, 1},
    };
    for (Object[] spec : expected) {
      String fileName = (String) spec[0];
      String filePath = TEST_DATA_DIR + fileName;
      try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
        ParquetMetadata metadata = reader.getMetadata();
        assertNotNull(metadata);
        assertEquals((int) spec[1], metadata.fileMetadata().numRows(),
            fileName + " row count");
        assertEquals((int) spec[2], metadata.fileMetadata().schema().getNumColumns(),
            fileName + " column count");
        assertTrue(metadata.getNumRowGroups() > 0, fileName + " row groups");
      }
    }
  }

  @Test
  void testInvalidFile() {
    assertThrows(ParquetException.class, () -> {
      try (ParquetFileReader reader = new ParquetFileReader("pom.xml")) {
        reader.getMetadata();
      }
    });
  }

  @Test
  void testVerifyMagic() throws IOException {
    String validFile = TEST_DATA_DIR + "alltypes_plain.parquet";
    try (FileChunkReader reader = new FileChunkReader(validFile)) {
      assertTrue(ParquetMetadataReader.verifyMagic(reader));
    }

    try (FileChunkReader reader = new FileChunkReader("pom.xml")) {
      assertFalse(ParquetMetadataReader.verifyMagic(reader));
    }
  }
}
