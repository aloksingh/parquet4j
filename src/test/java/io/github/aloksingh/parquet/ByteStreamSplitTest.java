package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Test for BYTE_STREAM_SPLIT encoding support
 */
public class ByteStreamSplitTest {

  @Test
  public void testByteStreamSplitFloat() throws IOException {
    // Read the parquet file with BYTE_STREAM_SPLIT encoding
    String filePath = "src/test/data/byte_stream_split.zstd.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find the f32 column (should be column 0)
      ColumnDescriptor col = schema.getColumn(0);
      assertEquals(Type.FLOAT, col.physicalType());

      // Read the f32 column
      ColumnValues f32Column = rowGroup.readColumn(0);
      List<Float> f32Values = f32Column.decodeAsFloat();

      // Verify total number of values
      assertEquals(300, f32Values.size(), "Should have 300 float values");

      // Verify first 10 values (from pyarrow)
      assertEquals(1.764052391052246f, f32Values.get(0), 0.0001f);
      assertEquals(0.40015721321105957f, f32Values.get(1), 0.0001f);
      assertEquals(0.978738009929657f, f32Values.get(2), 0.0001f);
      assertEquals(2.2408931255340576f, f32Values.get(3), 0.0001f);
      assertEquals(1.8675580024719238f, f32Values.get(4), 0.0001f);
      assertEquals(-0.9772778749465942f, f32Values.get(5), 0.0001f);
      assertEquals(0.9500884413719177f, f32Values.get(6), 0.0001f);
      assertEquals(-0.15135720372200012f, f32Values.get(7), 0.0001f);
      assertEquals(-0.10321885347366333f, f32Values.get(8), 0.0001f);
      assertEquals(0.4105985164642334f, f32Values.get(9), 0.0001f);

      // Verify last value
      assertEquals(0.3700558841228485f, f32Values.get(299), 0.0001f);
    }
  }

  @Test
  public void testByteStreamSplitDouble() throws IOException {
    // Read the parquet file with BYTE_STREAM_SPLIT encoding
    String filePath = "src/test/data/byte_stream_split.zstd.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find the f64 column (should be column 1)
      ColumnDescriptor col = schema.getColumn(1);
      assertEquals(Type.DOUBLE, col.physicalType());

      // Read the f64 column
      ColumnValues f64Column = rowGroup.readColumn(1);
      List<Double> f64Values = f64Column.decodeAsDouble();

      // Verify total number of values
      assertEquals(300, f64Values.size(), "Should have 300 double values");

      // Verify first 10 values (from pyarrow)
      assertEquals(-1.3065268517353166, f64Values.get(0), 0.0000001);
      assertEquals(1.658130679618188, f64Values.get(1), 0.0000001);
      assertEquals(-0.11816404512856976, f64Values.get(2), 0.0000001);
      assertEquals(-0.6801782039968504, f64Values.get(3), 0.0000001);
      assertEquals(0.6663830820319143, f64Values.get(4), 0.0000001);
      assertEquals(-0.4607197873885533, f64Values.get(5), 0.0000001);
      assertEquals(-1.3342584714027534, f64Values.get(6), 0.0000001);
      assertEquals(-1.3467175057975553, f64Values.get(7), 0.0000001);
      assertEquals(0.6937731526901325, f64Values.get(8), 0.0000001);
      assertEquals(-0.1595734381462669, f64Values.get(9), 0.0000001);

      // Verify last value
      assertEquals(-0.17858909208732915, f64Values.get(299), 0.0000001);
    }
  }

  @Test
  public void testByteStreamSplitBothColumns() throws IOException {
    // Test reading both columns together
    String filePath = "src/test/data/byte_stream_split.zstd.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read both columns
      ColumnValues f32Column = rowGroup.readColumn(0);
      ColumnValues f64Column = rowGroup.readColumn(1);

      List<Float> f32Values = f32Column.decodeAsFloat();
      List<Double> f64Values = f64Column.decodeAsDouble();

      // Verify we have the same number of values
      assertEquals(300, f32Values.size());
      assertEquals(300, f64Values.size());

      // Verify first row
      assertEquals(1.764052391052246f, f32Values.get(0), 0.0001f);
      assertEquals(-1.3065268517353166, f64Values.get(0), 0.0000001);

      // Verify last row
      assertEquals(0.3700558841228485f, f32Values.get(299), 0.0001f);
      assertEquals(-0.17858909208732915, f64Values.get(299), 0.0000001);
    }
  }
}
