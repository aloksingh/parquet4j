package org.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.LogicalColumnDescriptor;
import org.parquet.model.LogicalType;
import org.parquet.model.RowColumnGroup;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.SimpleRowColumnGroup;
import org.parquet.model.Type;

/**
 * Debug test to isolate issues with nullable columns.
 */
class ParquetWriterDebugTest {

  @TempDir
  Path tempDir;

  @Test
  void testRequiredStringColumn() throws Exception {
    // Test with REQUIRED string column (no nulls allowed)
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
        "required_string",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("required_string.parquet");

    // Write data (all non-null)
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {1, "Alice"}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {2, "Bob"}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {3, "Charlie"}));
    }

    // Read data back
    try (SerializedFileReader reader = new SerializedFileReader(outputFile)) {
      ParquetRowIterator iterator = (ParquetRowIterator) reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup row1 = iterator.next();
      assertEquals(1, row1.getColumnValue("id"));
      assertEquals("Alice", row1.getColumnValue("name"));

      assertTrue(iterator.hasNext());
      RowColumnGroup row2 = iterator.next();
      assertEquals(2, row2.getColumnValue("id"));
      assertEquals("Bob", row2.getColumnValue("name"));

      assertTrue(iterator.hasNext());
      RowColumnGroup row3 = iterator.next();
      assertEquals(3, row3.getColumnValue("id"));
      assertEquals("Charlie", row3.getColumnValue("name"));

      assertFalse(iterator.hasNext());
    }

    System.out.println("Required string column test successful!");
  }

  @Test
  void testOptionalIntColumn() throws Exception {
    // Test with optional INT32 column
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor(
            "id",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"id"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "value",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"value"}, 1, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "optional_int",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("optional_int.parquet");

    // Write data with null
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {1, 100}));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {2, null}));  // null value
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {3, 300}));
    }

    // Read data back
    try (SerializedFileReader reader = new SerializedFileReader(outputFile)) {
      ParquetRowIterator iterator = (ParquetRowIterator) reader.rowIterator();

      assertTrue(iterator.hasNext());
      RowColumnGroup row1 = iterator.next();
      assertEquals(1, row1.getColumnValue("id"));
      assertEquals(100, row1.getColumnValue("value"));

      assertTrue(iterator.hasNext());
      RowColumnGroup row2 = iterator.next();
      assertEquals(2, row2.getColumnValue("id"));
      assertNull(row2.getColumnValue("value"));

      assertTrue(iterator.hasNext());
      RowColumnGroup row3 = iterator.next();
      assertEquals(3, row3.getColumnValue("id"));
      assertEquals(300, row3.getColumnValue("value"));

      assertFalse(iterator.hasNext());
    }

    System.out.println("Optional int column test successful!");
  }
}
