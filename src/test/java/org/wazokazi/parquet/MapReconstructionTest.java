package org.wazokazi.parquet;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.wazokazi.parquet.model.ColumnValues;
import org.wazokazi.parquet.model.ParquetMetadata;
import org.wazokazi.parquet.model.SchemaDescriptor;

/**
 * Test for reconstructing Map objects from Parquet columns using definition and repetition levels.
 * <p>
 * This demonstrates how to use ColumnValues.decodeMapFromKeyValueColumns() to properly
 * reconstruct Map<K,V> objects from the two separate physical columns that Parquet uses
 * to store maps (one for keys, one for values).
 */
public class MapReconstructionTest {

  /**
   * Example showing how to read a Map column from a Parquet file.
   * <p>
   * The test file has a schema like:
   * - id: INT64
   * - item: MAP<STRING, STRING>
   * <p>
   * The "item" map is stored as TWO physical columns:
   * - item.key_value.key (BYTE_ARRAY)
   * - item.key_value.value (BYTE_ARRAY)
   */
  @Test
  void testMapReconstructionFromSeparateColumns() throws IOException {
    Path testFile = Path.of("src/test/data/data_with_map_column.parquet");

    if (!testFile.toFile().exists()) {
      System.out.println("Test file not found, skipping test");
      return;
    }

    // 1. Read the Parquet file metadata
    try (SerializedFileReader reader = new SerializedFileReader(testFile.toString())) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = metadata.fileMetadata().schema();

      System.out.println("Schema: " + schema);
      System.out.println("Number of physical columns: " + schema.getNumColumns());

      // The file has 3 physical columns:
      // [0] id
      // [1] item.key_value.key
      // [2] item.key_value.value

      // 2. Get the first row group reader
      int rowGroupIndex = 0;
      SerializedFileReader.RowGroupReader rowGroupReader = reader.getRowGroup(rowGroupIndex);

      // 3. Read the key column (column index 1)
      int keyColumnIndex = 1;
      ColumnValues keyColumn = rowGroupReader.readColumn(keyColumnIndex);
      System.out.println("Key column type: " + keyColumn.getType());

      // 4. Read the value column (column index 2)
      int valueColumnIndex = 2;
      ColumnValues valueColumn = rowGroupReader.readColumn(valueColumnIndex);
      System.out.println("Value column type: " + valueColumn.getType());

      // 5. Use the definition and repetition levels to reconstruct Maps
      List<Map<String, String>> maps = ColumnValues.decodeMapFromKeyValueColumns(
          keyColumn,
          valueColumn,
          obj -> {
            // Decode keys: BYTE_ARRAY -> String
            if (obj instanceof byte[]) {
              return new String((byte[]) obj, java.nio.charset.StandardCharsets.UTF_8);
            }
            return (String) obj;
          },
          obj -> {
            // Decode values: BYTE_ARRAY -> String
            if (obj instanceof byte[]) {
              return new String((byte[]) obj, java.nio.charset.StandardCharsets.UTF_8);
            }
            return (String) obj;
          }
      );

      // 6. Verify the results
      System.out.println("\nReconstructed " + maps.size() + " maps:");
      for (int i = 0; i < Math.min(10, maps.size()); i++) {
        Map<String, String> map = maps.get(i);
        System.out.println("  Row " + i + ": " + map);
      }

      // Basic assertions
      assertNotNull(maps);
      assertTrue(maps.size() > 0, "Should have decoded at least one map");

      // Check the first few maps
      for (int i = 0; i < Math.min(5, maps.size()); i++) {
        Map<String, String> map = maps.get(i);
        if (map != null) {
          System.out.println("\nMap " + i + " details:");
          System.out.println("  Size: " + map.size());
          System.out.println("  Keys: " + map.keySet());
          System.out.println("  Values: " + map.values());
        }
      }
    }
  }

  /**
   * Example showing the structure of definition and repetition levels for maps.
   * <p>
   * This is a conceptual test that demonstrates how the levels work.
   */
  @Test
  void testMapLevelStructure() {
    System.out.println("Map Level Structure:");
    System.out.println();
    System.out.println("Parquet MAP structure:");
    System.out.println("  optional group item (MAP) {");
    System.out.println("    repeated group key_value {");
    System.out.println("      required BYTE_ARRAY key;");
    System.out.println("      optional BYTE_ARRAY value;");
    System.out.println("    }");
    System.out.println("  }");
    System.out.println();
    System.out.println("Example data and corresponding levels:");
    System.out.println();
    System.out.println("Row 0: {\"a\": \"1\", \"b\": \"2\"}");
    System.out.println("  Rep: [0, 1]         - 0=new map, 1=additional entry");
    System.out.println("  Key Def: [2, 2]     - 2=key present");
    System.out.println("  Val Def: [3, 3]     - 3=value present");
    System.out.println("  Keys: [\"a\", \"b\"]");
    System.out.println("  Values: [\"1\", \"2\"]");
    System.out.println();
    System.out.println("Row 1: null");
    System.out.println("  Rep: [0]            - 0=new map");
    System.out.println("  Key Def: [0]        - 0=map is NULL");
    System.out.println("  Val Def: [0]        - 0=map is NULL");
    System.out.println("  Keys: []");
    System.out.println("  Values: []");
    System.out.println();
    System.out.println("Row 2: {}");
    System.out.println("  Rep: [0]            - 0=new map");
    System.out.println("  Key Def: [1]        - 1=map is empty");
    System.out.println("  Val Def: [1]        - 1=map is empty");
    System.out.println("  Keys: []");
    System.out.println("  Values: []");
    System.out.println();
    System.out.println("Row 3: {\"c\": null}");
    System.out.println("  Rep: [0]            - 0=new map");
    System.out.println("  Key Def: [2]        - 2=key present");
    System.out.println("  Val Def: [2]        - 2=value is NULL (entry exists but value null)");
    System.out.println("  Keys: [\"c\"]");
    System.out.println("  Values: []          - NULL values are not stored");
  }
}
