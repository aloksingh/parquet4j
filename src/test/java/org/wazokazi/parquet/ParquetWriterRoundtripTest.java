package org.wazokazi.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wazokazi.parquet.model.ColumnDescriptor;
import org.wazokazi.parquet.model.LogicalColumnDescriptor;
import org.wazokazi.parquet.model.LogicalType;
import org.wazokazi.parquet.model.ParquetMetadata;
import org.wazokazi.parquet.model.RowColumnGroup;
import org.wazokazi.parquet.model.SchemaDescriptor;
import org.wazokazi.parquet.model.SimpleRowColumnGroup;
import org.wazokazi.parquet.model.Type;

/**
 * Round-trip test to verify we can write and then read back Parquet files.
 */
class ParquetWriterRoundtripTest {

  @TempDir
  Path tempDir;

  @Test
  void testWriteAndReadBack() throws Exception {
    // Create a schema using logical columns
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
            "timestamp",
            LogicalType.PRIMITIVE,
            Type.INT64,
            new ColumnDescriptor(Type.INT64, new String[] {"timestamp"}, 0, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "test_schema",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("roundtrip.parquet");

    // Write data
    List<Object[]> expectedRows = new ArrayList<>();
    expectedRows.add(new Object[] {1, "Alice", 1000L});
    expectedRows.add(new Object[] {2, "Bob", 2000L});
    expectedRows.add(new Object[] {3, null, 3000L});  // null name
    expectedRows.add(new Object[] {4, "Charlie", 4000L});
    expectedRows.add(new Object[] {5, "David", 5000L});

    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      for (Object[] row : expectedRows) {
        writer.addRow(new SimpleRowColumnGroup(schema, row));
      }
    }

    // Read data back
    try (SerializedFileReader reader = new SerializedFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();

      // Verify metadata
      assertEquals(5, metadata.fileMetadata().numRows());
      assertEquals(3, metadata.fileMetadata().schema().getNumColumns());

      // Read rows
      ParquetRowIterator iterator = (ParquetRowIterator) reader.rowIterator();
      List<Object[]> actualRows = new ArrayList<>();

      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        actualRows.add(new Object[] {
            row.getColumnValue("id"),
            row.getColumnValue("name"),
            row.getColumnValue("timestamp")
        });
      }

      // Verify row count
      assertEquals(expectedRows.size(), actualRows.size());

      // Verify each row
      for (int i = 0; i < expectedRows.size(); i++) {
        Object[] expected = expectedRows.get(i);
        Object[] actual = actualRows.get(i);

        assertEquals(expected[0], actual[0], "Row " + i + " id mismatch");
        assertEquals(expected[1], actual[1], "Row " + i + " name mismatch");
        assertEquals(expected[2], actual[2], "Row " + i + " timestamp mismatch");
      }
    }

    System.out.println("Round-trip test successful!");
  }

  @Test
  void testWriteLargeFileAndReadBack() throws Exception {
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor(
            "index",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"index"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "value",
            LogicalType.PRIMITIVE,
            Type.DOUBLE,
            new ColumnDescriptor(Type.DOUBLE, new String[] {"value"}, 0, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "large_schema",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("large_roundtrip.parquet");
    int numRows = 10000;

    // Write data
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      for (int i = 0; i < numRows; i++) {
        writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
            i,
            i * 1.5
        }));
      }
    }

    // Read data back
    try (SerializedFileReader reader = new SerializedFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();
      assertEquals(numRows, metadata.fileMetadata().numRows());

      // Verify a few random rows
      ParquetRowIterator iterator = (ParquetRowIterator) reader.rowIterator();
      int count = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        int index = (Integer) row.getColumnValue("index");
        double value = (Double) row.getColumnValue("value");

        assertEquals(count, index);
        assertEquals(count * 1.5, value, 0.001);
        count++;
      }

      assertEquals(numRows, count);
    }

    System.out.println("Large round-trip test successful! Rows: " + numRows);
  }

  @Test
  void testWriteAllPrimitiveTypes() throws Exception {
    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        new LogicalColumnDescriptor(
            "bool_col",
            LogicalType.PRIMITIVE,
            Type.BOOLEAN,
            new ColumnDescriptor(Type.BOOLEAN, new String[] {"bool_col"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "int32_col",
            LogicalType.PRIMITIVE,
            Type.INT32,
            new ColumnDescriptor(Type.INT32, new String[] {"int32_col"}, 0, 0, 0)
        ),
        new LogicalColumnDescriptor(
            "int64_col",
            LogicalType.PRIMITIVE,
            Type.INT64,
            new ColumnDescriptor(Type.INT64, new String[] {"int64_col"}, 0, 0, 0)
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
            new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {"string_col"}, 0, 0, 0)
        )
    );
    SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns(
        "all_types",
        logicalColumns
    );

    Path outputFile = tempDir.resolve("all_types.parquet");

    // Write data
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
          true, 42, 9876543210L, 3.14f, 2.718281828, "Hello"
      }));
      writer.addRow(new SimpleRowColumnGroup(schema, new Object[] {
          false, -100, -123456L, -1.5f, -99.99, "World"
      }));
    }

    // Read data back
    try (SerializedFileReader reader = new SerializedFileReader(outputFile)) {
      ParquetRowIterator iterator = (ParquetRowIterator) reader.rowIterator();

      // First row
      assertTrue(iterator.hasNext());
      RowColumnGroup row1 = iterator.next();
      assertEquals(true, row1.getColumnValue("bool_col"));
      assertEquals(42, row1.getColumnValue("int32_col"));
      assertEquals(9876543210L, row1.getColumnValue("int64_col"));
      assertEquals(3.14f, (Float) row1.getColumnValue("float_col"), 0.001f);
      assertEquals(2.718281828, (Double) row1.getColumnValue("double_col"), 0.000001);
      assertEquals("Hello", row1.getColumnValue("string_col"));

      // Second row
      assertTrue(iterator.hasNext());
      RowColumnGroup row2 = iterator.next();
      assertEquals(false, row2.getColumnValue("bool_col"));
      assertEquals(-100, row2.getColumnValue("int32_col"));
      assertEquals(-123456L, row2.getColumnValue("int64_col"));
      assertEquals(-1.5f, (Float) row2.getColumnValue("float_col"), 0.001f);
      assertEquals(-99.99, (Double) row2.getColumnValue("double_col"), 0.000001);
      assertEquals("World", row2.getColumnValue("string_col"));

      assertFalse(iterator.hasNext());
    }

    System.out.println("All primitive types round-trip test successful!");
  }
}
