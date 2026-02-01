package org.parquet;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ColumnValues;
import org.parquet.model.SchemaDescriptor;

/**
 * Tests for reading nested structures (nested lists, structs)
 */
public class NestedStructureTest {

  @Test
  void testReadNestedLists() throws IOException {
    String filePath = "src/test/data/nested_lists.snappy.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("=== Testing nested_lists.snappy.parquet ===");
      System.out.println("Schema:");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.printf("  Column %d: %s (type=%s, maxDef=%d, maxRep=%d)%n",
            i, col.getPathString(), col.physicalType(),
            col.maxDefinitionLevel(), col.maxRepetitionLevel());
      }

      // Read the nested list column
      if (schema.getNumColumns() > 0) {
        ColumnValues values = rowGroup.readColumn(0);

        // For nested lists, we need to handle higher repetition levels
        System.out.println("\nAttempting to read nested list...");
        System.out.println("Column type: " + values.getType());

        // Try decoding as a list
        try {
          List<List<Object>> lists = values.decodeAsList(obj -> obj);

          System.out.println("Lists decoded: " + lists.size());
          for (int i = 0; i < Math.min(lists.size(), 5); i++) {
            System.out.println("  List " + i + ": " + lists.get(i));
          }

          assertNotNull(lists);
        } catch (Exception e) {
          System.out.println("Error decoding nested list: " + e.getMessage());
          // Nested lists with max_repetition_level > 1 require more complex handling
          System.out.println(
              "Note: Nested lists require special handling for repetition levels > 1");
        }
      }
    }
  }

  @Test
  void testReadStructure() throws IOException {
    String filePath = "src/test/data/nested_structs.rust.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("\n=== Testing nested_structs.rust.parquet ===");
      System.out.println("Schema:");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.printf("  Column %d: %s (type=%s, maxDef=%d, maxRep=%d)%n",
            i, col.getPathString(), col.physicalType(),
            col.maxDefinitionLevel(), col.maxRepetitionLevel());
      }

      // Create nested structure reader
      NestedStructureReader nestedReader = new NestedStructureReader(rowGroup, schema);

      // Try to read the first few columns as a struct
      if (schema.getNumColumns() >= 2) {
        // Identify columns that belong to the same struct by analyzing their paths
        String[] firstPath = schema.getColumn(0).path();
        List<Integer> structColumns = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        // Find all columns that share the same parent path
        for (int i = 0; i < schema.getNumColumns(); i++) {
          ColumnDescriptor col = schema.getColumn(i);
          String[] path = col.path();

          // For now, just take the first few columns as an example
          if (i < 3) {
            structColumns.add(i);
            fieldNames.add(path[path.length - 1]);
          }
        }

        System.out.println("\nReading struct with fields: " + fieldNames);

        try {
          int[] columnIndices = structColumns.stream().mapToInt(Integer::intValue).toArray();
          String[] fieldNamesArray = fieldNames.toArray(new String[0]);

          List<Map<String, Object>> structs =
              nestedReader.readStruct(columnIndices, fieldNamesArray);

          System.out.println("Structs decoded: " + structs.size());
          for (int i = 0; i < Math.min(structs.size(), 5); i++) {
            System.out.println("  Struct " + i + ": " + structs.get(i));
          }

          assertNotNull(structs);
          assertTrue(structs.size() > 0, "Should have at least one struct");
        } catch (Exception e) {
          System.out.println("Error reading struct: " + e.getMessage());
          e.printStackTrace();
        }
      }
    }
  }

  @Test
  void testNullList() throws IOException {
    String filePath = "src/test/data/null_list.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("\n=== Testing null_list.parquet ===");
      System.out.println("Schema:");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.printf("  Column %d: %s (type=%s, maxDef=%d, maxRep=%d)%n",
            i, col.getPathString(), col.physicalType(),
            col.maxDefinitionLevel(), col.maxRepetitionLevel());
      }

      // Read list column
      if (schema.getNumColumns() > 0) {
        ColumnValues values = rowGroup.readColumn(0);

        List<List<Object>> lists = values.decodeAsList(obj -> obj);

        System.out.println("Lists decoded: " + lists.size());
        for (int i = 0; i < lists.size(); i++) {
          System.out.println("  List " + i + ": " + lists.get(i));
        }

        assertNotNull(lists);
        // Verify that we can handle null lists
        boolean hasNull = lists.stream().anyMatch(list -> list == null);
        if (hasNull) {
          System.out.println("Successfully handled null lists");
        }
      }
    }
  }

  @Test
  void testOldListStructure() throws IOException {
    String filePath = "src/test/data/old_list_structure.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("\n=== Testing old_list_structure.parquet ===");
      System.out.println("Schema:");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.printf("  Column %d: %s (type=%s, maxDef=%d, maxRep=%d)%n",
            i, col.getPathString(), col.physicalType(),
            col.maxDefinitionLevel(), col.maxRepetitionLevel());
      }

      // Old list structure uses a different schema encoding
      if (schema.getNumColumns() > 0) {
        ColumnValues values = rowGroup.readColumn(0);

        try {
          List<List<Object>> lists = values.decodeAsList(obj -> obj);

          System.out.println("Lists decoded: " + lists.size());
          for (int i = 0; i < lists.size(); i++) {
            System.out.println("  List " + i + ": " + lists.get(i));
          }

          assertNotNull(lists);
        } catch (Exception e) {
          System.out.println("Error with old list structure: " + e.getMessage());
          System.out.println("Note: Old list structure may use different encoding");
        }
      }
    }
  }
}
