package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Validates the Java Parquet reader against JSON data exported by Python's pyarrow.
 *
 * <p>Comparison policy (strict):
 * <ul>
 *   <li>Every column of every fixture is compared cell-by-cell with EXACT values for
 *       ALL rows (binary compared by full content via {@link Arrays#equals}).</li>
 *   <li>The only columns not compared are the ones explicitly declared in
 *       {@link #UNSUPPORTED_COLUMNS} (documented reader capability gaps). A declared
 *       column that silently disappears fails the test.</li>
 *   <li>Known golden-export conventions are declared in {@link #EXPORT_QUIRKS} and
 *       asserted with exact expectations there (never silently skipped).</li>
 *   <li>Fixtures that the strict reader must reject are declared in
 *       {@link #EXPECTED_READ_FAILURES} with the exact expected exception type and
 *       message; any other read failure fails the test.</li>
 * </ul>
 */
public class ParquetJsonValidationTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  /** Row-API marker for a declared column whose values the row API rejects (e.g. INT96). */
  private static final Object REJECTED_VALUE = new Object();

  /**
   * Documented row-API behavior: INT96 physical values are rejected explicitly when read
   * through the row API instead of being silently surfaced as null (raw bytes remain
   * available via {@code ColumnValues.decodeAsInt96()}), while the golden export shows the
   * pyarrow timestamp strings.
   */
  private static final String INT96_UNSUPPORTED =
      "INT96 physical values are rejected explicitly by the row API (ParquetException on "
          + "read) while the golden export shows the pyarrow timestamp strings";

  /**
   * Fixtures the strict reader must REJECT, with the exact expected rejection.
   * Any fixture read failure must be listed here or the test fails.
   */
  static final Map<String, ExpectedRejection> EXPECTED_READ_FAILURES = Map.of(
      "nation.dict-malformed.parquet", new ExpectedRejection(
          "deliberately malformed dictionary fixture; the strict reader must reject it",
          ParquetException.class, "Page body exceeds column chunk boundary", null, null),
      "non_hadoop_lz4_compressed.parquet", new ExpectedRejection(
          "legacy private little-endian-framed Hadoop LZ4, non-conforming; the "
              + "codec-separated strict reader rejects it by design",
          ParquetException.class, "Failed to read row group 0",
          IOException.class, "Hadoop LZ4 uncompressed block size exceeds declared output"));

  /**
   * Per-fixture, per-column capability omissions. Every column NOT listed here is
   * compared with exact values. A listed column that is missing from the fixture or
   * from the reader output fails the test (stale declaration).
   */
  static final Map<String, Map<String, String>> UNSUPPORTED_COLUMNS = Map.ofEntries(
      Map.entry("alltypes_dictionary.parquet",
          Map.of("timestamp_col", INT96_UNSUPPORTED)),
      Map.entry("alltypes_plain.parquet",
          Map.of("timestamp_col", INT96_UNSUPPORTED)),
      Map.entry("alltypes_plain.snappy.parquet",
          Map.of("timestamp_col", INT96_UNSUPPORTED)),
      Map.entry("alltypes_tiny_pages.parquet",
          Map.of("timestamp_col", INT96_UNSUPPORTED)),
      Map.entry("alltypes_tiny_pages_plain.parquet",
          Map.of("timestamp_col", INT96_UNSUPPORTED)),
      Map.entry("int96_from_spark.parquet",
          Map.of("a", INT96_UNSUPPORTED)),
      Map.entry("userdata.parquet",
          Map.of("registration_dttm", INT96_UNSUPPORTED)));
  // DECIMAL columns are compared exactly: the row API applies the DECIMAL annotation and
  // returns scaled BigDecimal values that must equal the golden logical decimal strings.

  /**
   * Golden-export conventions that are NOT reader behavior. Each entry is asserted
   * with an exact expectation, so a change in either the export or the reader fails
   * loudly instead of being silently skipped.
   */
  static final Map<String, Map<String, ColumnQuirk>> EXPORT_QUIRKS = Map.of(
      // pandas JSON export renders NaN as null; the wire value is genuinely NaN.
      "nan_in_stats.parquet", Map.of("x",
          new ColumnQuirk(QuirkKind.NAN_AS_JSON_NULL, "x")),
      // pandas JSON export renders an empty list as the string "[]"; the reader
      // exposes the repeated item column with no items (null per row).
      "null_list.parquet", Map.of("emptylist",
          new ColumnQuirk(QuirkKind.EMPTY_LIST_AS_BRACKET_STRING, "emptylist.list.item")),
      // Known golden export defect: the pyarrow export of this concatenated-gzip
      // fixture yields 0 for the last cell, but the file's own column statistics
      // (min=1, max=513) prove the value is 513. Exact expected value is declared
      // per cell; if the golden export is ever fixed this declaration goes stale
      // and the test fails (as it should).
      "concatenated_gzip_members.parquet", Map.of("long_col",
          new ColumnQuirk(QuirkKind.GOLDEN_CELL_KNOWN_BAD, "long_col",
              Map.of(512, 513L))));

  /** Expected rejection of a deliberately non-conforming corpus fixture. */
  record ExpectedRejection(String reason, Class<? extends Throwable> type,
                           String messageSubstring, Class<? extends Throwable> causeType,
                           String causeMessageSubstring) {
  }

  /** Golden-export quirk with the reader column it maps to. */
  record ColumnQuirk(QuirkKind kind, String readerColumn,
                     Map<Integer, Object> goldenCellOverrides) {
    ColumnQuirk(QuirkKind kind, String readerColumn) {
      this(kind, readerColumn, Map.of());
    }
  }

  enum QuirkKind {
    /** Golden {@code null} means the value is NaN (pandas JSON cannot represent NaN). */
    NAN_AS_JSON_NULL,
    /** Golden string {@code "[]"} means an empty list (no items in the reader column). */
    EMPTY_LIST_AS_BRACKET_STRING,
    /**
     * Specific golden cells are known to be wrong ({@link #goldenCellOverrides} holds
     * the exact expected value per row index); all other cells compare normally.
     */
    GOLDEN_CELL_KNOWN_BAD
  }

  /**
   * Provides test cases for all parquet files that have corresponding JSON files.
   */
  static Stream<TestCase> parquetFilesWithJson() throws IOException {
    List<TestCase> testCases = new ArrayList<>();

    try (Stream<Path> paths = Files.list(Paths.get(TEST_DATA_DIR))) {
      paths.filter(p -> p.toString().endsWith(".parquet.json"))
          .forEach(jsonPath -> {
            String jsonFileName = jsonPath.getFileName().toString();
            String parquetFileName = jsonFileName.replace(".parquet.json", ".parquet");
            Path parquetPath = Paths.get(TEST_DATA_DIR, parquetFileName);

            if (Files.exists(parquetPath)) {
              testCases.add(new TestCase(parquetFileName, parquetPath.toString(), jsonPath.toString()));
            }
          });
    }

    return testCases.stream().sorted(Comparator.comparing(tc -> tc.fileName));
  }

  @Test
  void expectedReadFailureTableMatchesCorpus() throws IOException {
    Set<String> corpus = parquetFilesWithJson().map(tc -> tc.fileName)
        .collect(java.util.stream.Collectors.toSet());
    for (String fixture : EXPECTED_READ_FAILURES.keySet()) {
      assertTrue(corpus.contains(fixture),
          "EXPECTED_READ_FAILURES entry '" + fixture + "' is not part of the corpus (stale entry)");
    }
  }

  @Test
  void binaryValidationRejectsSameLengthContentChanges() {
    JsonObject expectedRow = JsonParser.parseString("{\"blob\":[1,2]}").getAsJsonObject();
    ValidationResult result = new ValidationResult();
    validateRow("binary", 0, expectedRow, Map.of("blob", "\u0001\u0003"),
        Map.of("blob", List.of(new byte[] {1, 3})), result);
    assertEquals(1, result.columnsMismatched, "Equal lengths must not hide different bytes");
    assertEquals(0, result.columnsMatched);
  }

  @Test
  void binaryValidationAcceptsIdenticalContent() {
    JsonObject expectedRow = JsonParser.parseString("{\"blob\":[1,2]}").getAsJsonObject();
    ValidationResult result = new ValidationResult();
    validateRow("binary", 0, expectedRow, Map.of("blob", "\u0001\u0002"),
        Map.of("blob", List.of(new byte[] {1, 2})), result);
    assertEquals(0, result.columnsMismatched);
    assertEquals(1, result.columnsMatched);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("parquetFilesWithJson")
  void testParquetAgainstJson(TestCase testCase) throws IOException {
    System.out.println("\n=== Testing: " + testCase.fileName + " ===");

    if (EXPECTED_READ_FAILURES.containsKey(testCase.fileName)) {
      assertExpectedRejection(testCase);
      return;
    }

    // Read golden JSON
    JsonArray expectedData;
    try (FileReader reader = new FileReader(testCase.jsonPath)) {
      expectedData = JsonParser.parseReader(reader).getAsJsonArray();
    }

    // Read Parquet file
    List<Map<String, Object>> actualData = new ArrayList<>();
    Map<String, List<byte[]>> binaryColumns = new LinkedHashMap<>();
    SchemaDescriptor schema;

    try (ParquetFileReader parquetReader = new ParquetFileReader(testCase.parquetPath)) {
      schema = parquetReader.getSchema();
      RowColumnGroupIterator iterator = parquetReader.rowIterator();
      Map<String, String> omissions = UNSUPPORTED_COLUMNS.getOrDefault(testCase.fileName, Map.of());

      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < row.getColumnCount(); i++) {
          String name = schema.getLogicalColumn(i).getName();
          try {
            rowMap.put(name, row.getColumnValue(i));
          } catch (ParquetException rejected) {
            // Columns the row API rejects (e.g. INT96) must still be present as keys for
            // the declared-omission check; they carry a rejection marker instead of a value.
            if (omissions.containsKey(name)) {
              rowMap.put(name, REJECTED_VALUE);
            } else {
              throw rejected;
            }
          }
        }
        actualData.add(rowMap);
      }

      // Raw bytes for BYTE_ARRAY columns: the row API surfaces BYTE_ARRAY as UTF-8
      // strings, which is lossy for non-UTF-8 binary. Binary columns are compared by
      // full raw content via Arrays.equals.
      for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
        LogicalColumnDescriptor logicalCol = schema.getLogicalColumn(i);
        if (logicalCol.isPrimitive()
            && logicalCol.getPhysicalDescriptor().physicalType()
                == io.github.aloksingh.parquet.model.Type.BYTE_ARRAY) {
          assertEquals(0, logicalCol.getPhysicalDescriptor().maxRepetitionLevel(),
              "Binary column " + logicalCol.getName() + " must be flat for row-aligned "
                  + "raw byte comparison");
          binaryColumns.put(logicalCol.getName(), new ArrayList<>());
        }
      }
      for (int rg = 0; rg < parquetReader.getNumRowGroups(); rg++) {
        ParquetFileReader.RowGroupReader rowGroup = parquetReader.getRowGroup(rg);
        for (int i = 0; i < schema.getNumColumns(); i++) {
          String name = schema.getColumn(i).getPathString();
          if (binaryColumns.containsKey(name)) {
            binaryColumns.get(name).addAll(rowGroup.readColumn(i).decodeAsByteArray());
          }
        }
      }
    } catch (Exception e) {
      fail("Unexpected read failure for " + testCase.fileName
          + " (declare it in EXPECTED_READ_FAILURES only if it is a deliberate "
          + "expected rejection)", e);
      return;
    }

    // Compare row counts exactly
    assertEquals(expectedData.size(), actualData.size(),
        "Row count mismatch for " + testCase.fileName);

    // Declared omissions/quirks must describe columns that actually exist
    assertDeclaredExpectationsCurrent(testCase.fileName, expectedData, schema);

    if (expectedData.size() > 0) {
      ValidationResult result = validateAllRows(
          testCase.fileName, expectedData, actualData, binaryColumns);
      assertTrue(result.columnsMatched > 0 || result.columnsOmitted > 0,
          testCase.fileName + ": no columns were validated at all");
      if (result.columnsMismatched > 0) {
        fail(testCase.fileName + ": " + result.columnsMismatched
            + " column mismatches: " + result.issues);
      }
      System.out.printf("%s: %d cells compared exactly, %d omitted (declared), 0 mismatches%n",
          testCase.fileName, result.columnsMatched, result.columnsOmitted);
    }
  }

  /**
   * Asserts that a non-conforming fixture is rejected exactly as declared.
   */
  private void assertExpectedRejection(TestCase testCase) {
    ExpectedRejection expected = EXPECTED_READ_FAILURES.get(testCase.fileName);
    Throwable thrown = assertThrows(expected.type(), () -> {
      try (ParquetFileReader reader = new ParquetFileReader(testCase.parquetPath)) {
        RowColumnGroupIterator iterator = reader.rowIterator();
        while (iterator.hasNext()) {
          iterator.next();
        }
      }
    }, "Expected rejection (" + expected.reason() + ") did not happen");
    assertNotNull(thrown.getMessage(), "Rejection must carry a message");
    assertTrue(thrown.getMessage().contains(expected.messageSubstring()),
        "Rejection message '" + thrown.getMessage() + "' must contain '"
            + expected.messageSubstring() + "'");
    if (expected.causeType() != null) {
      Throwable cause = thrown.getCause();
      assertNotNull(cause, "Rejection must carry the declared cause "
          + expected.causeType().getName());
      assertTrue(expected.causeType().isInstance(cause),
          "Rejection cause must be " + expected.causeType().getName() + " but was "
              + cause.getClass().getName());
      assertNotNull(cause.getMessage(), "Rejection cause must carry a message");
      assertTrue(cause.getMessage().contains(expected.causeMessageSubstring()),
          "Rejection cause message '" + cause.getMessage() + "' must contain '"
              + expected.causeMessageSubstring() + "'");
    }
  }

  /**
   * Fails when a declared omission/quirk names a column that no longer exists in the
   * golden data or in the reader schema, or when a golden/reader column is not
   * claimed by any declaration (silently disappearing columns are not allowed).
   */
  private void assertDeclaredExpectationsCurrent(String fileName, JsonArray expectedData,
                                                 SchemaDescriptor schema) {
    Set<String> goldenColumns = new LinkedHashSet<>();
    for (JsonElement row : expectedData) {
      flattenColumns(row.getAsJsonObject(), "", goldenColumns);
    }

    Set<String> readerColumns = new LinkedHashSet<>();
    for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
      readerColumns.add(schema.getLogicalColumn(i).getName());
    }

    Map<String, String> omissions = UNSUPPORTED_COLUMNS.getOrDefault(fileName, Map.of());
    Map<String, ColumnQuirk> quirks = EXPORT_QUIRKS.getOrDefault(fileName, Map.of());

    for (String column : omissions.keySet()) {
      assertTrue(goldenColumns.contains(column) || readerColumns.contains(column),
          fileName + ": declared unsupported column '" + column
              + "' no longer exists (stale declaration)");
    }
    for (String column : quirks.keySet()) {
      assertTrue(goldenColumns.contains(column) || readerColumns.contains(column),
          fileName + ": declared export quirk column '" + column
              + "' no longer exists (stale declaration)");
    }
    // A golden-cell override must still be needed: the golden must still hold the
    // wrong value there, otherwise the declaration is stale.
    for (Map.Entry<String, ColumnQuirk> quirkEntry : quirks.entrySet()) {
      for (Map.Entry<Integer, Object> override
          : quirkEntry.getValue().goldenCellOverrides().entrySet()) {
        int row = override.getKey();
        assertTrue(row < expectedData.size(),
            fileName + ": golden-cell override row " + row
                + " is out of range (stale declaration)");
        JsonElement goldenCell =
            expectedData.get(row).getAsJsonObject().get(quirkEntry.getKey());
        assertNotNull(goldenCell, fileName + ": golden-cell override column '"
            + quirkEntry.getKey() + "' missing at row " + row + " (stale declaration)");
        assertTrue(goldenCell.isJsonPrimitive(),
            fileName + ": golden-cell override at row " + row
                + " must target a primitive cell");
        boolean goldenAlreadyCorrect = goldenCell.getAsJsonPrimitive().isNumber()
            ? numbersEqual(override.getValue(),
                goldenCell.getAsJsonPrimitive().getAsNumber())
            : java.util.Objects.equals(override.getValue(), goldenCell.getAsString());
        assertTrue(!goldenAlreadyCorrect,
            fileName + ": golden cell '" + quirkEntry.getKey() + "' row " + row
                + " now holds the correct value; remove the stale golden-cell override");
      }
    }

    Set<String> declared = new HashSet<>(omissions.keySet());
    declared.addAll(quirks.keySet());
    for (String column : goldenColumns) {
      assertTrue(readerColumns.contains(column) || declared.contains(column),
          fileName + ": golden column '" + column
              + "' has no matching reader column and is not declared; every column must "
              + "be compared exactly or explicitly declared");
    }

    Set<String> claimedReaderColumns = new HashSet<>(goldenColumns);
    claimedReaderColumns.addAll(omissions.keySet());
    for (ColumnQuirk quirk : quirks.values()) {
      assertTrue(readerColumns.contains(quirk.readerColumn()),
          fileName + ": export quirk maps to reader column '" + quirk.readerColumn()
              + "' which does not exist (stale declaration)");
      claimedReaderColumns.add(quirk.readerColumn());
    }
    // A zero-row golden has no columns to claim (its exact zero row count is
    // asserted instead); for goldens with rows every reader column must be claimed.
    if (expectedData.size() > 0) {
      for (String column : readerColumns) {
        assertTrue(claimedReaderColumns.contains(column),
            fileName + ": reader column '" + column
                + "' is not claimed by the golden data or any declaration "
                + "(column silently disappeared from the golden export?)");
      }
    }
  }

  /** Flattens nested JSON objects into dotted leaf column names. */
  private static void flattenColumns(JsonObject row, String prefix, Set<String> out) {
    for (Map.Entry<String, JsonElement> entry : row.entrySet()) {
      String name = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
      if (entry.getValue().isJsonObject()) {
        flattenColumns(entry.getValue().getAsJsonObject(), name, out);
      } else {
        out.add(name);
      }
    }
  }

  private ValidationResult validateAllRows(String fileName, JsonArray expectedData,
                                          List<Map<String, Object>> actualData,
                                          Map<String, List<byte[]>> binaryColumns) {
    ValidationResult result = new ValidationResult();
    for (int i = 0; i < expectedData.size(); i++) {
      validateRow(fileName, i, expectedData.get(i).getAsJsonObject(), actualData.get(i),
          binaryColumns, result);
    }
    return result;
  }

  private void validateRow(String fileName, int rowIndex, JsonObject expectedRow,
                           Map<String, Object> actualRow,
                           Map<String, List<byte[]>> binaryColumns,
                           ValidationResult result) {
    Map<String, String> omissions = UNSUPPORTED_COLUMNS.getOrDefault(fileName, Map.of());
    Map<String, ColumnQuirk> quirks = EXPORT_QUIRKS.getOrDefault(fileName, Map.of());

    Map<String, JsonElement> leaves = new LinkedHashMap<>();
    flattenValues(expectedRow, "", leaves);

    for (Map.Entry<String, JsonElement> entry : leaves.entrySet()) {
      String columnName = entry.getKey();
      JsonElement expectedValue = entry.getValue();
      result.columnsChecked++;

      if (omissions.containsKey(columnName)) {
        // Declared capability gap: not compared, but it must still be present.
        if (!actualRow.containsKey(columnName)) {
          result.columnsMismatched++;
          result.issues.add(columnName + ": declared unsupported column disappeared");
        } else {
          result.columnsOmitted++;
        }
        continue;
      }

      ColumnQuirk quirk = quirks.get(columnName);
      if (quirk != null) {
        validateQuirkyValue(fileName, rowIndex, columnName, expectedValue, quirk,
            actualRow, result);
        continue;
      }

      if (!actualRow.containsKey(columnName)) {
        result.columnsMismatched++;
        result.issues.add(columnName + ": declared column missing from reader output");
        continue;
      }

      Object actualValue = actualRow.get(columnName);
      if (expectedValue.isJsonNull()) {
        // Strict: a golden null must be a real null (NaN is only accepted where the
        // export quirk is explicitly declared).
        if (actualValue == null) {
          result.columnsMatched++;
        } else {
          result.columnsMismatched++;
          result.issues.add(columnName + ": Expected null but got " + actualValue);
        }
      } else if (expectedValue.isJsonPrimitive()) {
        if (compareValue(rowIndex, columnName, expectedValue, actualValue, result)) {
          result.columnsMatched++;
        } else {
          result.columnsMismatched++;
        }
      } else if (expectedValue.isJsonArray()) {
        // Byte arrays are exported as lists of byte values.
        if (compareBytes(rowIndex, columnName, expectedValue.getAsJsonArray(),
            binaryColumns, result)) {
          result.columnsMatched++;
        } else {
          result.columnsMismatched++;
        }
      } else {
        result.columnsMismatched++;
        result.issues.add(columnName + ": Unsupported expected value shape (nested "
            + "values must be flattened to leaf columns)");
      }
    }
  }

  /** Flattens nested JSON objects into dotted leaf column names with their values. */
  private static void flattenValues(JsonObject row, String prefix, Map<String, JsonElement> out) {
    for (Map.Entry<String, JsonElement> entry : row.entrySet()) {
      String name = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
      if (entry.getValue().isJsonObject()) {
        flattenValues(entry.getValue().getAsJsonObject(), name, out);
      } else {
        out.put(name, entry.getValue());
      }
    }
  }

  private void validateQuirkyValue(String fileName, int rowIndex, String columnName,
                                   JsonElement expectedValue, ColumnQuirk quirk,
                                   Map<String, Object> actualRow, ValidationResult result) {
    if (!actualRow.containsKey(quirk.readerColumn())) {
      result.columnsMismatched++;
      result.issues.add(columnName + ": reader column '" + quirk.readerColumn()
          + "' mapped by declared quirk is missing");
      return;
    }
    Object actualValue = actualRow.get(quirk.readerColumn());
    switch (quirk.kind()) {
      case NAN_AS_JSON_NULL -> {
        if (expectedValue.isJsonNull()) {
          // Golden null stands for NaN here; assert the wire value is exactly NaN.
          if (actualValue instanceof Double && Double.isNaN((Double) actualValue)) {
            result.columnsMatched++;
          } else {
            result.columnsMismatched++;
            result.issues.add(columnName + ": golden null stands for NaN (pandas JSON "
                + "export) but reader returned " + actualValue);
          }
        } else if (compareValue(rowIndex, columnName, expectedValue, actualValue, result)) {
          result.columnsMatched++;
        } else {
          result.columnsMismatched++;
        }
      }
      case EMPTY_LIST_AS_BRACKET_STRING -> {
        if (!expectedValue.isJsonPrimitive()
            || !"[]".equals(expectedValue.getAsString())) {
          result.columnsMismatched++;
          result.issues.add(columnName + ": declared empty-list quirk expects golden "
              + "string \"[]\" but got " + expectedValue);
        } else if (actualValue != null) {
          result.columnsMismatched++;
          result.issues.add(columnName + ": golden \"[]\" means an empty list (no "
              + "items) but reader returned " + actualValue);
        } else {
          result.columnsMatched++;
        }
      }
      case GOLDEN_CELL_KNOWN_BAD -> {
        Object override = quirk.goldenCellOverrides().get(rowIndex);
        if (override == null) {
          // Cells without an override compare normally.
          if (expectedValue.isJsonNull() ? actualValue == null
              : compareValue(rowIndex, columnName, expectedValue, actualValue, result)) {
            result.columnsMatched++;
          } else {
            result.columnsMismatched++;
          }
        } else if (numbersEqual(override, actualValue)) {
          result.columnsMatched++;
        } else {
          result.columnsMismatched++;
          result.issues.add(columnName + " row " + rowIndex + ": golden cell is known "
              + "wrong; expected the declared value " + override + " but got " + actualValue);
        }
      }
    }
  }

  /** Numeric equality across boxed integral/floating types. */
  private static boolean numbersEqual(Object expected, Object actual) {
    if (expected instanceof Number && actual instanceof Number) {
      return new BigDecimal(expected.toString())
          .compareTo(new BigDecimal(actual.toString())) == 0;
    }
    return java.util.Objects.equals(expected, actual);
  }

  private boolean compareBytes(int rowIndex, String columnName, JsonArray expectedArray,
                               Map<String, List<byte[]>> binaryColumns, ValidationResult result) {
    List<byte[]> column = binaryColumns.get(columnName);
    if (column == null || rowIndex >= column.size()) {
      result.issues.add(columnName + ": no raw byte values available for comparison");
      return false;
    }
    byte[] expectedBytes = new byte[expectedArray.size()];
    for (int i = 0; i < expectedBytes.length; i++) {
      int value = expectedArray.get(i).getAsBigDecimal().intValueExact();
      if (value < -128 || value > 255) {
        throw new IllegalArgumentException("Invalid exported byte: " + value);
      }
      expectedBytes[i] = (byte) value;
    }
    byte[] actualBytes = column.get(rowIndex);
    if (actualBytes == null) {
      result.issues.add(columnName + ": Expected byte array " + Arrays.toString(expectedBytes)
          + " but got null");
      return false;
    }
    if (Arrays.equals(expectedBytes, actualBytes)) {
      return true;
    }
    result.issues.add(columnName + ": Byte array content mismatch (expected "
        + Arrays.toString(expectedBytes) + " but got " + Arrays.toString(actualBytes) + ")");
    return false;
  }

  private boolean compareValue(int rowIndex, String columnName,
                               JsonElement expectedValue, Object actualValue,
                               ValidationResult result) {
    if (expectedValue.getAsJsonPrimitive().isBoolean()) {
      if (actualValue instanceof Boolean && expectedValue.getAsBoolean() == (Boolean) actualValue) {
        return true;
      }
      result.issues.add(columnName + ": Expected " + expectedValue.getAsBoolean()
          + " but got " + actualValue);
      return false;
    }

    if (expectedValue.getAsJsonPrimitive().isNumber()) {
      if (actualValue instanceof Integer || actualValue instanceof Long) {
        BigDecimal actual = new BigDecimal(((Number) actualValue).longValue());
        if (expectedValue.getAsBigDecimal().compareTo(actual) == 0) {
          return true;
        }
        result.issues.add(columnName + ": Expected " + expectedValue.getAsBigDecimal()
            + " but got " + actualValue);
        return false;
      }
      if (actualValue instanceof java.math.BigInteger || actualValue instanceof BigDecimal) {
        // Widened INTEGER carriers (BigInteger) and exact scaled DECIMAL values compare by
        // numeric value; the exact carrier and scale are asserted by the annotation tests.
        if (expectedValue.getAsBigDecimal()
            .compareTo(new BigDecimal(actualValue.toString())) == 0) {
          return true;
        }
        result.issues.add(columnName + ": Expected " + expectedValue.getAsBigDecimal()
            + " but got " + actualValue);
        return false;
      }
      if (actualValue instanceof Float) {
        float expected = expectedValue.getAsFloat();
        float actual = (Float) actualValue;
        if (Float.compare(expected, actual) == 0) {
          return true;
        }
        result.issues.add(columnName + ": Expected " + expected + " but got " + actual);
        return false;
      }
      if (actualValue instanceof Double) {
        double expected = expectedValue.getAsDouble();
        double actual = (Double) actualValue;
        if (Double.compare(expected, actual) == 0) {
          return true;
        }
        result.issues.add(columnName + ": Expected " + expected + " but got " + actual);
        return false;
      }
      result.issues.add(columnName + ": Expected number " + expectedValue.getAsBigDecimal()
          + " but got " + actualValue);
      return false;
    }

    if (expectedValue.getAsJsonPrimitive().isString()) {
      if (actualValue instanceof String
          && expectedValue.getAsString().equals((String) actualValue)) {
        return true;
      }
      if (actualValue instanceof BigDecimal) {
        // Golden decimal exports render the logical decimal as a string ("1.00").
        try {
          if (new BigDecimal(expectedValue.getAsString())
              .compareTo((BigDecimal) actualValue) == 0) {
            return true;
          }
        } catch (NumberFormatException ignored) {
          // Fall through to the mismatch below.
        }
      }
      result.issues.add(columnName + ": Expected '" + expectedValue.getAsString()
          + "' but got '" + actualValue + "'");
      return false;
    }

    result.issues.add(columnName + ": Unsupported value type for comparison");
    return false;
  }

  /**
   * Tracks validation results
   */
  static class ValidationResult {
    int columnsChecked = 0;
    int columnsMatched = 0;
    int columnsOmitted = 0;
    int columnsMismatched = 0;
    List<String> issues = new ArrayList<>();
  }

  /**
   * Test case holder
   */
  static class TestCase {
    final String fileName;
    final String parquetPath;
    final String jsonPath;

    TestCase(String fileName, String parquetPath, String jsonPath) {
      this.fileName = fileName;
      this.parquetPath = parquetPath;
      this.jsonPath = jsonPath;
    }

    @Override
    public String toString() {
      return fileName;
    }
  }
}
