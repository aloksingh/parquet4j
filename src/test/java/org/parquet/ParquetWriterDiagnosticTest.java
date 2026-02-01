package org.parquet;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.LogicalColumnDescriptor;
import org.parquet.model.LogicalType;
import org.parquet.model.ParquetMetadata;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.SimpleRowColumnGroup;
import org.parquet.model.Type;

/**
 * Diagnostic test to inspect what's actually being written.
 */
class ParquetWriterDiagnosticTest {

  @TempDir
  Path tempDir;

  @Test
  void testInspectWrittenFile() throws Exception {
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
            new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"name"}, 0, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "simple",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("diagnostic.parquet");

    // Write a minimal file with just 3 rows
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {1, "Alice"}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {2, "Bob"}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {3, "Charlie"}));
    }

    // Inspect the file
    try (RandomAccessFile raf = new RandomAccessFile(outputFile.toFile(), "r")) {
      long fileSize = raf.length();
      System.out.println("File size: " + fileSize + " bytes");

      // Read magic at start
      byte[] startMagic = new byte[4];
      raf.seek(0);
      raf.read(startMagic);
      System.out.println("Start magic: " + new String(startMagic));

      // Read magic at end
      byte[] endMagic = new byte[4];
      raf.seek(fileSize - 4);
      raf.read(endMagic);
      System.out.println("End magic: " + new String(endMagic));

      // Read metadata length (4 bytes before end magic)
      raf.seek(fileSize - 8);
      int metadataLength = Integer.reverseBytes(raf.readInt());
      System.out.println("Metadata length: " + metadataLength + " bytes");

      // Calculate where row group data should be
      long metadataStart = fileSize - 8 - metadataLength;
      long rowGroupDataLength = metadataStart - 4; // minus start magic
      System.out.println("Row group data starts at: 4");
      System.out.println("Row group data length: " + rowGroupDataLength + " bytes");
      System.out.println("Metadata starts at: " + metadataStart);

      // Try to read the file with the reader
      System.out.println("\nAttempting to read with ParquetMetadataReader...");
      try (FileChunkReader reader = new FileChunkReader(outputFile)) {
        ParquetMetadata metadata = ParquetMetadataReader.readMetadata(reader);
        System.out.println("Schema: " + metadata.fileMetadata().schema().name());
        System.out.println("Num rows: " + metadata.fileMetadata().numRows());
        System.out.println("Num row groups: " + metadata.getNumRowGroups());

        if (metadata.getNumRowGroups() > 0) {
          ParquetMetadata.RowGroupMetadata rg = metadata.rowGroups().get(0);
          System.out.println("\nRow group 0:");
          System.out.println("  Num rows: " + rg.numRows());
          System.out.println("  Total byte size: " + rg.totalByteSize());
          System.out.println("  Num columns: " + rg.columns().size());

          for (int i = 0; i < rg.columns().size(); i++) {
            ParquetMetadata.ColumnChunkMetadata col = rg.columns().get(i);
            System.out.println("\n  Column " + i + ":");
            System.out.println("    Type: " + col.type());
            System.out.println("    Num values: " + col.numValues());
            System.out.println("    Data page offset: " + col.getFirstDataPageOffset());
            System.out.println("    Total compressed size: " + col.totalCompressedSize());
            System.out.println("    Total uncompressed size: " + col.totalUncompressedSize());
          }
        }
      }
    }
  }
}
