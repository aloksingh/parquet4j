package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.Type;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Characterization pins for the two public MAP assembly paths (improvement 16).
 * The paths have deliberate, documented divergences that must survive the
 * shared-engine extraction unchanged:
 * <ul>
 *   <li>{@link NestedStructureReader#readMap} flattens nested MAP entries per row and
 *       REJECTS duplicate keys (a duplicate would drop flattened data silently);</li>
 *   <li>{@link ColumnValues#decodeMapFromKeyValueColumns} handles scalar MAP shapes only,
 *       rejects nested repetition, and lets the LAST duplicate key win (legacy behavior).</li>
 * </ul>
 */
class MapAssemblyContractTest {

  private static final class SyntheticRowGroup extends ParquetFileReader.RowGroupReader {
    private final Map<Integer, ColumnValues> columns;

    SyntheticRowGroup(Map<Integer, ColumnValues> columns) {
      super(null, null, null);
      this.columns = columns;
    }

    @Override
    public ColumnValues readColumn(int columnIndex) {
      return columns.get(columnIndex);
    }
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String string(Object value) {
    return value == null ? null : new String((byte[]) value, StandardCharsets.UTF_8);
  }

  private static ColumnValues bytesColumn(ColumnDescriptor descriptor, int[] definitions,
                                          int[] repetitions, String... values) {
    byte[][] raw = new byte[values.length][];
    for (int i = 0; i < values.length; i++) raw[i] = bytes(values[i]);
    Page page = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
        definitions, repetitions, DecodingTestSupport.plain(Type.BYTE_ARRAY, (Object[]) raw));
    return new ColumnValues(Type.BYTE_ARRAY, List.of(page), descriptor, null);
  }

  @Test
  void readMapRejectsDuplicateKeysThatFlatteningWouldDrop() throws IOException {
    // MAP of MAPs flattened per row: row 0 contributes keys "a" twice.
    ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 2);
    ColumnDescriptor valueDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 3, 2);
    ColumnValues keys = bytesColumn(keyDescriptor, new int[]{2, 2, 2}, new int[]{0, 1, 1}, "a", "a", "a");
    ColumnValues values = bytesColumn(valueDescriptor, new int[]{3, 3, 3}, new int[]{0, 1, 1}, "1", "2", "3");
    NestedStructureReader reader = new NestedStructureReader(
        new SyntheticRowGroup(Map.of(0, keys, 1, values)), null);
    ParquetException failure = assertThrows(ParquetException.class,
        () -> reader.readMap(0, 1, MapAssemblyContractTest::string, MapAssemblyContractTest::string));
    assertEquals("MAP row 0 repeats key a; flattening nested MAP entries would drop data",
        failure.getMessage());
  }

  @Test
  void readMapRejectsMixedNullKeyAndValueRows() throws IOException {
    // Row 0: key leaf has a null container (definition 0) while the value leaf has an
    // empty container (definition 1): one side null, the other non-null.
    ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 1);
    ColumnDescriptor valueDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 3, 1);
    ColumnValues keys = bytesColumn(keyDescriptor, new int[]{0}, new int[]{0});
    ColumnValues values = bytesColumn(valueDescriptor, new int[]{1}, new int[]{0});
    NestedStructureReader reader = new NestedStructureReader(
        new SyntheticRowGroup(Map.of(0, keys, 1, values)), null);
    ParquetException failure = assertThrows(ParquetException.class,
        () -> reader.readMap(0, 1, MapAssemblyContractTest::string, MapAssemblyContractTest::string));
    assertEquals("Key and value lists should both be null or both be non-null", failure.getMessage());
  }

  @Test
  void readMapRejectsEntryCountMismatchInsideOneRow() throws IOException {
    // Row 0: two key entries, one value entry under the same shared thresholds.
    ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 1);
    ColumnDescriptor valueDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 3, 1);
    ColumnValues keys = bytesColumn(keyDescriptor, new int[]{2, 2}, new int[]{0, 1}, "a", "b");
    ColumnValues values = bytesColumn(valueDescriptor, new int[]{3}, new int[]{0}, "x");
    NestedStructureReader reader = new NestedStructureReader(
        new SyntheticRowGroup(Map.of(0, keys, 1, values)), null);
    ParquetException failure = assertThrows(ParquetException.class,
        () -> reader.readMap(0, 1, MapAssemblyContractTest::string, MapAssemblyContractTest::string));
    assertEquals("Key and value lists have different sizes at index 0: 2 vs 1",
        failure.getMessage());
  }

  @Test
  void scalarMapAssemblyKeepsTheLastDuplicateKey() {
    // Scalar MAP (maximum repetition 1) with key "a" twice: legacy last-wins behavior.
    ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 1);
    ColumnDescriptor valueDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 3, 1);
    ColumnValues keys = bytesColumn(keyDescriptor, new int[]{2, 2}, new int[]{0, 1}, "a", "a");
    ColumnValues values = bytesColumn(valueDescriptor, new int[]{3, 3}, new int[]{0, 1}, "1", "2");
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("a", "2");
    assertEquals(Arrays.asList(expected),
        ColumnValues.decodeMapFromKeyValueColumns(keys, values,
            MapAssemblyContractTest::string, MapAssemblyContractTest::string));
  }

  @Test
  void scalarMapAssemblyRejectsNestedRepetitionShapes() {
    ColumnDescriptor keyDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 2);
    ColumnDescriptor valueDescriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 3, 2);
    ColumnValues keys = bytesColumn(keyDescriptor, new int[]{2}, new int[]{0}, "a");
    ColumnValues values = bytesColumn(valueDescriptor, new int[]{3}, new int[]{0}, "1");
    ParquetException failure = assertThrows(ParquetException.class,
        () -> ColumnValues.decodeMapFromKeyValueColumns(keys, values,
            MapAssemblyContractTest::string, MapAssemblyContractTest::string));
    assertEquals("Unsupported scalar MAP key/value shape", failure.getMessage());
  }
}
