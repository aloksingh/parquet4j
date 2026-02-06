package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for reading LIST logical type
 */
public class ListTypeTest {

  @Test
  void testReadInt64List() throws IOException {
    String filePath = "src/test/data/list_columns.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("=== Testing list_columns.parquet ===");
      System.out.println("Schema: " + schema);

      // Read the int64_list column (column 0)
      System.out.println("\nReading int64_list column...");
      ColumnValues values = rowGroup.readColumn(0);

      // Decode as list of int64
      List<List<Long>> lists = values.decodeAsList(obj -> {
        if (obj instanceof Long) {
          return (Long) obj;
        } else if (obj instanceof Integer) {
          return ((Integer) obj).longValue();
        } else {
          throw new IllegalArgumentException("Unexpected type: " + obj.getClass());
        }
      });

      System.out.println("Lists decoded: " + lists.size());
      for (int i = 0; i < lists.size(); i++) {
        System.out.println("  List " + i + ": " + lists.get(i));
      }

      // Verify the data matches expected from JSON
      assertEquals(3, lists.size(), "Should have 3 lists");

      // List 0: [1, 2, 3]
      assertNotNull(lists.get(0));
      assertEquals(3, lists.get(0).size());
      assertEquals(Long.valueOf(1), lists.get(0).get(0));
      assertEquals(Long.valueOf(2), lists.get(0).get(1));
      assertEquals(Long.valueOf(3), lists.get(0).get(2));

      // List 1: [null, 1]
      assertNotNull(lists.get(1));
      assertEquals(2, lists.get(1).size());
      assertNull(lists.get(1).get(0));
      assertEquals(Long.valueOf(1), lists.get(1).get(1));

      // List 2: [4]
      assertNotNull(lists.get(2));
      assertEquals(1, lists.get(2).size());
      assertEquals(Long.valueOf(4), lists.get(2).get(0));
    }
  }

  @Test
  void testReadStringList() throws IOException {
    String filePath = "src/test/data/list_columns.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read the utf8_list column (column 1)
      System.out.println("\n=== Reading utf8_list column ===");
      ColumnValues values = rowGroup.readColumn(1);

      // Decode as list of strings
      List<List<String>> lists = values.decodeAsList(obj -> {
        if (obj instanceof byte[]) {
          return new String((byte[]) obj, java.nio.charset.StandardCharsets.UTF_8);
        } else {
          return obj.toString();
        }
      });

      System.out.println("Lists decoded: " + lists.size());
      for (int i = 0; i < lists.size(); i++) {
        System.out.println("  List " + i + ": " + lists.get(i));
      }

      // Verify the data
      assertEquals(3, lists.size(), "Should have 3 lists");

      // List 0: ["abc", "efg", "hij"]
      assertNotNull(lists.get(0));
      assertEquals(3, lists.get(0).size());
      assertEquals("abc", lists.get(0).get(0));
      assertEquals("efg", lists.get(0).get(1));
      assertEquals("hij", lists.get(0).get(2));

      // List 1: null
      assertNull(lists.get(1));

      // List 2: ["efg", null, "hij", "xyz"]
      assertNotNull(lists.get(2));
      assertEquals(4, lists.get(2).size());
      assertEquals("efg", lists.get(2).get(0));
      assertNull(lists.get(2).get(1));
      assertEquals("hij", lists.get(2).get(2));
      assertEquals("xyz", lists.get(2).get(3));
    }
  }
}
