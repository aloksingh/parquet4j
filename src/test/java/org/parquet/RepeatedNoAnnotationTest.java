package org.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ColumnValues;
import org.parquet.model.SchemaDescriptor;

/**
 * Tests for reading repeated groups without LIST annotation.
 * <p>
 * This test verifies reading of the "repeated_no_annotation.parquet" file which contains:
 * - user.id: int32 (required)
 * - user.phoneNumbers.phone: repeated group (old-style list without LIST annotation)
 * - number: int64 (required)
 * - kind: string (optional)
 * <p>
 * The file contains 6 rows with varying phone number lists:
 * - Row 0: id=1, phoneNumbers=null
 * - Row 1: id=2, phoneNumbers=null
 * - Row 2: id=3, phoneNumbers with empty phone list
 * - Row 3: id=4, phoneNumbers with 1 phone: number=5555555555, kind=null
 * - Row 4: id=5, phoneNumbers with 1 phone: number=1111111111, kind='home'
 * - Row 5: id=6, phoneNumbers with 3 phones:
 * [number=1111111111, kind='home'],
 * [number=2222222222, kind=null],
 * [number=3333333333, kind='mobile']
 */
public class RepeatedNoAnnotationTest {

  @Test
  void testReadRepeatedNoAnnotation() throws IOException {
    String filePath = "src/test/data/repeated_no_annotation.parquet";

    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      SerializedFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      System.out.println("=== Testing repeated_no_annotation.parquet ===");
      System.out.println("Schema:");
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        System.out.printf("  Column %d: %s (type=%s, maxDef=%d, maxRep=%d)%n",
            i, col.getPathString(), col.physicalType(),
            col.maxDefinitionLevel(), col.maxRepetitionLevel());
      }

      // Verify schema structure
      assertEquals(3, schema.getNumColumns(), "Should have 3 leaf columns");

      // Column 0: id
      assertEquals("id", schema.getColumn(0).getPathString());
      assertEquals(0, schema.getColumn(0).maxRepetitionLevel(), "id should not be repeated");

      // Column 1: phoneNumbers.phone.number
      assertEquals("phoneNumbers.phone.number", schema.getColumn(1).getPathString());
      assertEquals(1, schema.getColumn(1).maxRepetitionLevel(),
          "phone.number should have repetition level 1");

      // Column 2: phoneNumbers.phone.kind
      assertEquals("phoneNumbers.phone.kind", schema.getColumn(2).getPathString());
      assertEquals(1, schema.getColumn(2).maxRepetitionLevel(),
          "phone.kind should have repetition level 1");

      // Read id column
      System.out.println("\n=== Reading id column ===");
      ColumnValues idValues = rowGroup.readColumn(0);
      List<Integer> ids = idValues.decodeAsInt32();

      System.out.println("IDs: " + ids);
      assertEquals(6, ids.size(), "Should have 6 rows");
      assertEquals(Integer.valueOf(1), ids.get(0));
      assertEquals(Integer.valueOf(2), ids.get(1));
      assertEquals(Integer.valueOf(3), ids.get(2));
      assertEquals(Integer.valueOf(4), ids.get(3));
      assertEquals(Integer.valueOf(5), ids.get(4));
      assertEquals(Integer.valueOf(6), ids.get(5));

      // Read phone number column as list
      System.out.println("\n=== Reading phoneNumbers.phone.number column ===");
      ColumnValues numberValues = rowGroup.readColumn(1);
      List<List<Long>> phoneNumbers = numberValues.decodeAsList(obj -> {
        if (obj instanceof Long) {
          return (Long) obj;
        } else if (obj instanceof Integer) {
          return ((Integer) obj).longValue();
        } else {
          throw new IllegalArgumentException("Unexpected type: " + obj.getClass());
        }
      });

      System.out.println("Phone numbers:");
      for (int i = 0; i < phoneNumbers.size(); i++) {
        System.out.println("  Row " + i + ": " + phoneNumbers.get(i));
      }

      assertEquals(6, phoneNumbers.size(), "Should have 6 rows");

      // Row 0: null (phoneNumbers is null)
      assertNull(phoneNumbers.get(0), "Row 0 should have null phoneNumbers");

      // Row 1: null (phoneNumbers is null)
      assertNull(phoneNumbers.get(1), "Row 1 should have null phoneNumbers");

      // Row 2: empty list (phoneNumbers exists but phone list is empty)
      assertNotNull(phoneNumbers.get(2), "Row 2 should have non-null phoneNumbers");
      assertEquals(0, phoneNumbers.get(2).size(), "Row 2 should have empty phone list");

      // Row 3: [5555555555]
      assertNotNull(phoneNumbers.get(3), "Row 3 should have non-null phoneNumbers");
      assertEquals(1, phoneNumbers.get(3).size(), "Row 3 should have 1 phone number");
      assertEquals(Long.valueOf(5555555555L), phoneNumbers.get(3).get(0));

      // Row 4: [1111111111]
      assertNotNull(phoneNumbers.get(4), "Row 4 should have non-null phoneNumbers");
      assertEquals(1, phoneNumbers.get(4).size(), "Row 4 should have 1 phone number");
      assertEquals(Long.valueOf(1111111111L), phoneNumbers.get(4).get(0));

      // Row 5: [1111111111, 2222222222, 3333333333]
      assertNotNull(phoneNumbers.get(5), "Row 5 should have non-null phoneNumbers");
      assertEquals(3, phoneNumbers.get(5).size(), "Row 5 should have 3 phone numbers");
      assertEquals(Long.valueOf(1111111111L), phoneNumbers.get(5).get(0));
      assertEquals(Long.valueOf(2222222222L), phoneNumbers.get(5).get(1));
      assertEquals(Long.valueOf(3333333333L), phoneNumbers.get(5).get(2));

      // Read phone kind column as list
      System.out.println("\n=== Reading phoneNumbers.phone.kind column ===");
      ColumnValues kindValues = rowGroup.readColumn(2);
      List<List<String>> phoneKinds = kindValues.decodeAsList(obj -> {
        if (obj == null) {
          return null;
        } else if (obj instanceof byte[]) {
          return new String((byte[]) obj, java.nio.charset.StandardCharsets.UTF_8);
        } else {
          return obj.toString();
        }
      });

      System.out.println("Phone kinds:");
      for (int i = 0; i < phoneKinds.size(); i++) {
        System.out.println("  Row " + i + ": " + phoneKinds.get(i));
      }

      assertEquals(6, phoneKinds.size(), "Should have 6 rows");

      // Row 0: null
      assertNull(phoneKinds.get(0), "Row 0 should have null phoneNumbers");

      // Row 1: null
      assertNull(phoneKinds.get(1), "Row 1 should have null phoneNumbers");

      // Row 2: empty list
      assertNotNull(phoneKinds.get(2), "Row 2 should have non-null phoneNumbers");
      assertEquals(0, phoneKinds.get(2).size(), "Row 2 should have empty phone list");

      // Row 3: [null] (kind is optional and null)
      assertNotNull(phoneKinds.get(3), "Row 3 should have non-null phoneNumbers");
      assertEquals(1, phoneKinds.get(3).size(), "Row 3 should have 1 phone");
      assertNull(phoneKinds.get(3).get(0), "Row 3 phone kind should be null");

      // Row 4: ["home"]
      assertNotNull(phoneKinds.get(4), "Row 4 should have non-null phoneNumbers");
      assertEquals(1, phoneKinds.get(4).size(), "Row 4 should have 1 phone");
      assertEquals("home", phoneKinds.get(4).get(0));

      // Row 5: ["home", null, "mobile"]
      assertNotNull(phoneKinds.get(5), "Row 5 should have non-null phoneNumbers");
      assertEquals(3, phoneKinds.get(5).size(), "Row 5 should have 3 phones");
      assertEquals("home", phoneKinds.get(5).get(0));
      assertNull(phoneKinds.get(5).get(1), "Row 5 second phone kind should be null");
      assertEquals("mobile", phoneKinds.get(5).get(2));

      // Verify that the number and kind lists are aligned
      System.out.println("\n=== Verifying number and kind lists are aligned ===");
      for (int i = 0; i < phoneNumbers.size(); i++) {
        List<Long> numbers = phoneNumbers.get(i);
        List<String> kinds = phoneKinds.get(i);

        if (numbers == null && kinds == null) {
          System.out.println("  Row " + i + ": Both null (phoneNumbers is null)");
          continue;
        }

        assertNotNull(numbers, "Row " + i + ": numbers should not be null if kinds is not null");
        assertNotNull(kinds, "Row " + i + ": kinds should not be null if numbers is not null");
        assertEquals(numbers.size(), kinds.size(),
            "Row " + i + ": number and kind lists should have same size");

        System.out.println("  Row " + i + " phone entries:");
        for (int j = 0; j < numbers.size(); j++) {
          System.out.println(
              "    Phone " + j + ": number=" + numbers.get(j) + ", kind=" + kinds.get(j));
        }
      }

      System.out.println("\n=== Test passed! ===");
    }
  }
}
