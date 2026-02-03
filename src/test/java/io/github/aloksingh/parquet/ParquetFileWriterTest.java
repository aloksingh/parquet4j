package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

class ParquetFileWriterTest {

  @TempDir
  Path tempDir;

  @Test
  void testWriteSimpleParquetFile() throws Exception {
    // Create a simple schema with two columns using logical columns
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
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "test_schema",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("test.parquet");

    // Create writer
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      // Add some rows
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {1, "Alice"}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {2, "Bob"}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {3, "Charlie"}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {4, null})); // null name
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {5, "Eve"}));
    }

    // Verify file was created
    assertTrue(outputFile.toFile().exists());
    assertTrue(outputFile.toFile().length() > 0);

    System.out.println("Parquet file written to: " + outputFile);
    System.out.println("File size: " + outputFile.toFile().length() + " bytes");
  }

  @Test
  void testWriteMultipleTypes() throws Exception {
    // Create a schema with various data types using logical columns
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor(
            "int_col",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"int_col"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "long_col",
            LogicalType.PRIMITIVE,
            Type.INT64,
            new ColumnDescriptor(Type.INT64, new String[] {"long_col"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "float_col",
            LogicalType.PRIMITIVE,
            Type.FLOAT,
            new ColumnDescriptor(Type.FLOAT, new String[] {"float_col"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "double_col",
            LogicalType.PRIMITIVE,
            Type.DOUBLE,
            new ColumnDescriptor(Type.DOUBLE, new String[] {"double_col"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "string_col",
            LogicalType.PRIMITIVE,
            Type.BYTE_ARRAY,
            new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"string_col"}, 1, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "multi_type_schema",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("multi_type.parquet");

    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      for (int i = 0; i < 100; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
            i,                          // int
            (long) i * 1000,           // long
            (float) i * 1.5,           // float
            (double) i * 2.5,          // double
            "value_" + i               // string
        }));
      }
    }

    assertTrue(outputFile.toFile().exists());
    System.out.println("Multi-type parquet file written to: " + outputFile);
    System.out.println("File size: " + outputFile.toFile().length() + " bytes");
  }

  @Test
  void testWriteLargeFile() throws Exception {
    // Test writing a larger file that will span multiple row groups
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor(
            "id",
            LogicalType.PRIMITIVE,
            Type.INT64,
            new ColumnDescriptor(Type.INT64, new String[] {"id"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "data",
            LogicalType.PRIMITIVE,
            Type.BYTE_ARRAY,
            new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"data"}, 0, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "large_schema",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("large.parquet");

    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      // Write 5000 rows (should create multiple row groups)
      for (long i = 0; i < 5000; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
            i,
            "This is row number " + i + " with some data"
        }));
      }
    }

    assertTrue(outputFile.toFile().exists());
    assertTrue(outputFile.toFile().length() > 1000);  // Should be reasonably sized
    System.out.println("Large parquet file written to: " + outputFile);
    System.out.println("File size: " + outputFile.toFile().length() + " bytes");
  }

  @Test
  void testEmptyFile() throws Exception {
    List<LogicalColumnDescriptor> logicalColumns = List.of(
        new LogicalColumnDescriptor(
            "id",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "empty_schema",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("empty.parquet");

    // Write file with no rows
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      // Don't add any rows
    }

    assertTrue(outputFile.toFile().exists());
    System.out.println("Empty parquet file written to: " + outputFile);
  }
}
