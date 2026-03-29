package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Test for reading Parquet file with struct data type.
 * <p>
 * Schema:
 * - id: int64
 * - coords: struct&lt;x: int32, y: double&gt;
 * <p>
 * Test data contains 10 rows:
 * - Row 0-8: id values 0-8 with coords {x: 10-18, y: 20.5-28.5}
 * - Row 9: id=10 with coords=null
 */
public class DataWithStructsTest {

  private static final String TEST_FILE = "src/test/data/data_with_structs.parquet";

  @Test
  void testReadSchema() throws IOException {
    try (ParquetFileReader reader = new ParquetFileReader(TEST_FILE)) {
      SchemaDescriptor schema = reader.getSchema();

      assertNotNull(schema);
      assertEquals(3, schema.getNumColumns(), "Should have 3 columns (id, coords.x, coords.y)");

      // Column 0: id
      ColumnDescriptor idCol = schema.getColumn(0);
      assertEquals("id", idCol.getPathString());
      assertEquals(io.github.aloksingh.parquet.model.Type.INT64, idCol.physicalType());

      // Column 1: coords.x
      ColumnDescriptor xCol = schema.getColumn(1);
      assertEquals("coords.x", xCol.getPathString());
      assertEquals(io.github.aloksingh.parquet.model.Type.INT32, xCol.physicalType());

      // Column 2: coords.y
      ColumnDescriptor yCol = schema.getColumn(2);
      assertEquals("coords.y", yCol.getPathString());
      assertEquals(io.github.aloksingh.parquet.model.Type.DOUBLE, yCol.physicalType());

      System.out.println("\n=== Schema for " + TEST_FILE + " ===");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.printf("  Column %d: %s (type=%s, maxDef=%d, maxRep=%d)%n",
            i, col.getPathString(), col.physicalType(),
            col.maxDefinitionLevel(), col.maxRepetitionLevel());
      }
    }
  }

  @Test
  void testReadIdColumn() throws IOException {
    try (ParquetFileReader reader = new ParquetFileReader(TEST_FILE)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read id column (column 0)
      List<Long> ids = rowGroup.readColumn(0).decodeAsInt64();

      assertEquals(10, ids.size(), "Should have 10 rows");

      // Verify id values (0, 1, 2, ..., 8, 10)
      for (int i = 0; i < 9; i++) {
        assertEquals((long) i, ids.get(i), "Row " + i + " should have id=" + i);
      }
      assertEquals(10L, ids.get(9), "Row 9 should have id=10");

      System.out.println("\n=== ID Column Values ===");
      System.out.println("  IDs: " + ids);
    }
  }

  @Test
  void testReadStructColumns() throws IOException {
    try (ParquetFileReader reader = new ParquetFileReader(TEST_FILE)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Create NestedStructureReader
      NestedStructureReader nestedReader = new NestedStructureReader(rowGroup, schema);

      // Read coords struct (columns 1 and 2)
      int[] columnIndices = {1, 2}; // coords.x and coords.y
      String[] fieldNames = {"x", "y"};

      List<Map<String, Object>> structs = nestedReader.readStruct(columnIndices, fieldNames);

      assertEquals(10, structs.size(), "Should have 10 structs");

      System.out.println("\n=== Coords Struct Values ===");

      // Verify first 9 rows
      for (int i = 0; i < 9; i++) {
        Map<String, Object> coords = structs.get(i);
        assertNotNull(coords, "Row " + i + " coords should not be null");

        Integer x = (Integer) coords.get("x");
        Double y = (Double) coords.get("y");

        assertEquals(10 + i, x, "Row " + i + " coords.x should be " + (10 + i));
        assertEquals(20.5 + i, y, 0.001, "Row " + i + " coords.y should be " + (20.5 + i));

        System.out.printf("  Row %d: {x: %d, y: %.1f}%n", i, x, y);
      }

      // Verify row 9 (id=10) has null coords
      // Note: In Parquet structs, null struct means null values for all fields
      Map<String, Object> lastCoords = structs.get(9);
      assertNotNull(lastCoords, "Map itself exists but fields should be null");
      assertNull(lastCoords.get("x"), "Row 9 coords.x should be null");
      assertNull(lastCoords.get("y"), "Row 9 coords.y should be null");

      System.out.printf("  Row 9: {x: %s, y: %s}%n",
          lastCoords.get("x"), lastCoords.get("y"));
    }
  }

  @Test
  void testReadCompleteData() throws IOException {
    try (ParquetFileReader reader = new ParquetFileReader(TEST_FILE)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Read all columns
      List<Long> ids = rowGroup.readColumn(0).decodeAsInt64();
      List<Integer> xValues = rowGroup.readColumn(1).decodeAsInt32();
      List<Double> yValues = rowGroup.readColumn(2).decodeAsDouble();

      System.out.println("\n=== Complete Data ===");
      System.out.printf("%-5s %-10s %-10s%n", "id", "coords.x", "coords.y");
      System.out.println("--------------------------------");

      for (int i = 0; i < ids.size(); i++) {
        System.out.printf("%-5d %-10s %-10s%n",
            ids.get(i),
            xValues.get(i),
            yValues.get(i));
      }

      // Verify data integrity
      assertEquals(10, ids.size());
      assertEquals(10, xValues.size());
      assertEquals(10, yValues.size());

      // Check specific values
      assertEquals(0L, ids.get(0));
      assertEquals(10, xValues.get(0));
      assertEquals(20.5, yValues.get(0), 0.001);

      assertEquals(8L, ids.get(8));
      assertEquals(18, xValues.get(8));
      assertEquals(28.5, yValues.get(8), 0.001);

      // Row with null struct
      assertEquals(10L, ids.get(9));
      assertNull(xValues.get(9));
      assertNull(yValues.get(9));
    }
  }
}
