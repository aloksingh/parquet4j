package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConditionalCompressionTest {

  @TempDir
  Path tempDir;

  private SchemaDescriptor int32Schema(String name) {
    List<LogicalColumnDescriptor> cols = Arrays.asList(
        new LogicalColumnDescriptor(
            name,
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {name}, 0, 0, 0)
        )
    );
    return SchemaDescriptor.fromLogicalColumns(name + "_schema", cols);
  }

  @Test
  void testUncompressedCodecAlwaysStoresUncompressed() throws Exception {
    SchemaDescriptor schema = int32Schema("val");
    Path outputFile = tempDir.resolve("uncompressed.parquet");

    try (ParquetFileWriter writer = new ParquetFileWriter(
        outputFile, schema, CompressionCodec.UNCOMPRESSED, 1024 * 1024, 128 * 1024 * 1024)) {
      for (int i = 0; i < 200; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {i}));
      }
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(CompressionCodec.UNCOMPRESSED,
          metadata.rowGroups().get(0).columns().get(0).codec());
    }
  }

  @Test
  void testCompressibleDataKeepsCompression() throws Exception {
    SchemaDescriptor schema = int32Schema("val");
    Path outputFile = tempDir.resolve("compressible.parquet");

    try (ParquetFileWriter writer = new ParquetFileWriter(
        outputFile, schema, CompressionCodec.ZSTD, 1024 * 1024, 128 * 1024 * 1024)) {
      for (int i = 0; i < 2000; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {1}));
      }
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(CompressionCodec.ZSTD,
          metadata.rowGroups().get(0).columns().get(0).codec());
    }
  }

  @Test
  void testIncompressibleDataFallsBackToUncompressed() throws Exception {
    SchemaDescriptor schema = int32Schema("val");
    Path outputFile = tempDir.resolve("incompressible.parquet");

    Random rng = new Random(42);
    try (ParquetFileWriter writer = new ParquetFileWriter(
        outputFile, schema, CompressionCodec.ZSTD, 1024 * 1024, 128 * 1024 * 1024)) {
      for (int i = 0; i < 2000; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {rng.nextInt()}));
      }
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(CompressionCodec.UNCOMPRESSED,
          metadata.rowGroups().get(0).columns().get(0).codec());
    }
  }

  @Test
  void testThresholdZeroAvoidsCompression() throws Exception {
    SchemaDescriptor schema = int32Schema("val");
    Path outputFile = tempDir.resolve("threshold_zero.parquet");

    try (ParquetFileWriter writer = new ParquetFileWriter(
        outputFile, schema, CompressionCodec.ZSTD, 1024 * 1024, 128 * 1024 * 1024, 0.0)) {
      for (int i = 0; i < 2000; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {1}));
      }
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(CompressionCodec.UNCOMPRESSED,
          metadata.rowGroups().get(0).columns().get(0).codec());
    }
  }

  @Test
  void testThresholdOneAlwaysKeepsCompression() throws Exception {
    SchemaDescriptor schema = int32Schema("val");
    Path outputFile = tempDir.resolve("threshold_one.parquet");

    try (ParquetFileWriter writer = new ParquetFileWriter(
        outputFile, schema, CompressionCodec.ZSTD, 1024 * 1024, 128 * 1024 * 1024, 1.0)) {
      for (int i = 0; i < 2000; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {i % 10}));
      }
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(CompressionCodec.ZSTD,
          metadata.rowGroups().get(0).columns().get(0).codec());
    }
  }

  @Test
  void testMultiColumnIndependentDecision() throws Exception {
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor(
            "compressible",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"compressible"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "random",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"random"}, 1, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("two_col_schema", logicalColumns);
    Path outputFile = tempDir.resolve("two_col.parquet");

    Random rng = new Random(42);
    try (ParquetFileWriter writer = new ParquetFileWriter(
        outputFile, schema, CompressionCodec.ZSTD, 1024 * 1024, 128 * 1024 * 1024)) {
      for (int i = 0; i < 2000; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
            1,
            rng.nextInt()
        }));
      }
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      var rg = metadata.rowGroups().get(0);
      assertEquals(CompressionCodec.ZSTD,
          rg.columns().get(0).codec(),
          "Compressible column should keep ZSTD");
      assertEquals(CompressionCodec.UNCOMPRESSED,
          rg.columns().get(1).codec(),
          "Random column should fall back to UNCOMPRESSED");
    }
  }

  @Test
  void testRoundtripWithConditionalCompression() throws Exception {
    SchemaDescriptor schema = int32Schema("val");
    Path outputFile = tempDir.resolve("roundtrip_conditional.parquet");

    List<Integer> expectedValues = new ArrayList<>();
    Random rng = new Random(42);
    try (ParquetFileWriter writer = new ParquetFileWriter(
        outputFile, schema, CompressionCodec.ZSTD, 1024 * 1024, 128 * 1024 * 1024)) {
      for (int i = 0; i < 500; i++) {
        int value = rng.nextInt();
        expectedValues.add(value);
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {value}));
      }
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(500, metadata.fileMetadata().numRows());

      ParquetRowIterator iterator = (ParquetRowIterator) reader.rowIterator();
      int rowCount = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        assertEquals(expectedValues.get(rowCount), row.getColumnValue(0));
        rowCount++;
      }
      assertEquals(500, rowCount);
    }
  }
}
