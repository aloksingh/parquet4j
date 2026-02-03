package org.wazokazi.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wazokazi.parquet.model.ColumnDescriptor;
import org.wazokazi.parquet.model.CompressionCodec;
import org.wazokazi.parquet.model.LogicalColumnDescriptor;
import org.wazokazi.parquet.model.LogicalType;
import org.wazokazi.parquet.model.ParquetMetadata;
import org.wazokazi.parquet.model.RowColumnGroup;
import org.wazokazi.parquet.model.SchemaDescriptor;
import org.wazokazi.parquet.model.SimpleRowColumnGroup;
import org.wazokazi.parquet.model.Type;

/**
 * Test compression functionality in ParquetFileWriter
 */
class ParquetCompressionTest {

  @TempDir
  Path tempDir;

  @Test
  void testGzipCompression() throws Exception {
    // Create a schema using logical columns
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor(
            "id",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "data",
            LogicalType.PRIMITIVE,
            Type.BYTE_ARRAY,
            new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"data"}, 0, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "compressed_schema",
        logicalColumns
    );

    Path uncompressedFile = tempDir.resolve("uncompressed.parquet");
    Path compressedFile = tempDir.resolve("compressed.parquet");

    // Create test data with repetitive content (compresses well)
    String repetitiveData = "This is a test string that repeats. ".repeat(100);

    // Write uncompressed file
    try (ParquetFileWriter writer = new ParquetFileWriter(
        uncompressedFile, schema, CompressionCodec.UNCOMPRESSED, 1024 * 1024, 128 * 1024 * 1024)) {
      for (int i = 0; i < 1000; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
            i,
            repetitiveData
        }));
      }
    }

    // Write compressed file
    try (ParquetFileWriter writer = new ParquetFileWriter(
        compressedFile, schema, CompressionCodec.GZIP, 1024 * 1024, 128 * 1024 * 1024)) {
      for (int i = 0; i < 1000; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
            i,
            repetitiveData
        }));
      }
    }

    // Verify compressed file is smaller
    long uncompressedSize = Files.size(uncompressedFile);
    long compressedSize = Files.size(compressedFile);

    System.out.println("Uncompressed file size: " + uncompressedSize + " bytes");
    System.out.println("Compressed file size: " + compressedSize + " bytes");
    System.out.println("Compression ratio: " + String.format("%.2f%%",
        (1.0 - (double) compressedSize / uncompressedSize) * 100));

    assertTrue(compressedSize < uncompressedSize,
        "Compressed file should be smaller than uncompressed file");

    // Verify we can read back the compressed file correctly
    try (SerializedFileReader reader = new SerializedFileReader(compressedFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(1000, metadata.fileMetadata().numRows());

      // Verify compression codec is set correctly
      assertEquals(CompressionCodec.GZIP,
          metadata.rowGroups().get(0).columns().get(0).codec());

      // Read and verify a few rows
      ParquetRowIterator iterator = (ParquetRowIterator) reader.rowIterator();
      for (int i = 0; i < 10; i++) {
        assertTrue(iterator.hasNext());
        RowColumnGroup row = iterator.next();
        assertEquals(i, row.getColumnValue("id"));
        assertEquals(repetitiveData, row.getColumnValue("data"));
      }
    }

    System.out.println("GZIP compression test successful!");
  }

  @Test
  void testGzipCompressionRoundtrip() throws Exception {
    // Create a more complex schema using logical columns
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor(
            "id",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "name",
            LogicalType.PRIMITIVE,
            Type.BYTE_ARRAY,
            new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"name"}, 1, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "value",
            LogicalType.PRIMITIVE,
            Type.DOUBLE,
            new ColumnDescriptor(Type.DOUBLE, new String[] {"value"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "timestamp",
            LogicalType.PRIMITIVE,
            Type.INT64,
            new ColumnDescriptor(Type.INT64, new String[] {"timestamp"}, 1, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "complex_schema",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("gzip_roundtrip.parquet");

    // Write data with GZIP compression
    try (ParquetFileWriter writer = new ParquetFileWriter(
        outputFile, schema, CompressionCodec.GZIP, 1024 * 1024, 128 * 1024 * 1024)) {

      for (int i = 0; i < 5000; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
            i,
            (i % 3 == 0) ? null : "Name_" + i,  // Every 3rd row has null name
            i * 1.5,
            (i % 5 == 0) ? null : (long) i * 1000  // Every 5th row has null timestamp
        }));
      }
    }

    // Read back and verify
    try (SerializedFileReader reader = new SerializedFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(5000, metadata.fileMetadata().numRows());

      ParquetRowIterator iterator = (ParquetRowIterator) reader.rowIterator();
      int count = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        int id = (Integer) row.getColumnValue("id");

        assertEquals(count, id);

        if (count % 3 == 0) {
          assertEquals(null, row.getColumnValue("name"));
        } else {
          assertEquals("Name_" + count, row.getColumnValue("name"));
        }

        assertEquals(count * 1.5, (Double) row.getColumnValue("value"), 0.001);

        if (count % 5 == 0) {
          assertEquals(null, row.getColumnValue("timestamp"));
        } else {
          assertEquals((long) count * 1000, row.getColumnValue("timestamp"));
        }

        count++;
      }

      assertEquals(5000, count);
    }

    System.out.println("GZIP compression round-trip test successful! Rows: 5000");
  }
}
