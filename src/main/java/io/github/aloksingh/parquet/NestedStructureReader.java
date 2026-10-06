package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for reading nested structures (MAP, STRUCT) from Parquet files.
 * Unlike lists which are stored in a single column, maps and structs can span multiple columns.
 */
public class NestedStructureReader {

  private final ParquetFileReader.RowGroupReader rowGroupReader;
  private final SchemaDescriptor schema;

  /**
   * Constructs a new NestedStructureReader.
   *
   * @param rowGroupReader the row group reader to use for reading column data
   * @param schema the schema descriptor containing column metadata
   */
  public NestedStructureReader(ParquetFileReader.RowGroupReader rowGroupReader,
                               SchemaDescriptor schema) {
    this.rowGroupReader = rowGroupReader;
    this.schema = schema;
  }

  /**
   * Read a MAP structure from Parquet.
   * Maps in Parquet are stored as a list of key-value pairs, where keys and values
   * are in separate columns.
   * <p>
   * Schema example:
   * <pre>
   * optional group my_map (MAP) {
   *   repeated group key_value {
   *     required binary key (UTF8);
   *     optional binary value (UTF8);
   *   }
   * }
   * </pre>
   *
   * @param <K> the type of keys in the map
   * @param <V> the type of values in the map
   * @param keyColumnIndex the column index for map keys
   * @param valueColumnIndex the column index for map values
   * @param keyDecoder function to decode key values from their raw representation
   * @param valueDecoder function to decode value values from their raw representation
   * @return list of maps, where each map corresponds to one row. Null values represent
   *         rows where the map itself is null; entries with a null value keep their key.
   *         For leaves with nested repetition levels (a MAP of MAPs), each row's map
   *         contains all inner entries of that row flattened together; rows whose inner
   *         map is absent decode as null and rows with an empty inner map as empty maps.
   * @throws IOException if an I/O error occurs while reading the columns
   * @throws ParquetException if the key and value columns have mismatched structures
   */
  public <K, V> List<Map<K, V>> readMap(int keyColumnIndex, int valueColumnIndex,
                                        java.util.function.Function<Object, K> keyDecoder,
                                        java.util.function.Function<Object, V> valueDecoder)
      throws IOException {

    // Read both columns
    ColumnValues keyColumn = rowGroupReader.readColumn(keyColumnIndex);
    ColumnValues valueColumn = rowGroupReader.readColumn(valueColumnIndex);

    // Both leaves share one repeated key_value layer. MAP keys are required by the
    // Parquet specification, so entries sit at the key leaf's maximum definition level
    // and the map container one level below. Decoding both leaves with those shared
    // structural thresholds aligns their per-row containers by row/level events, never
    // by page numbers, so the two leaves may split their V1 pages at different points.
    ColumnDescriptor keyDescriptor =
        keyColumn.getColumnDescriptor();
    ColumnDescriptor valueDescriptor =
        valueColumn.getColumnDescriptor();
    int entryDefinition = keyDescriptor.maxDefinitionLevel();
    int valueDefinition = valueDescriptor.maxDefinitionLevel();
    boolean sharedEntryLayer = keyDescriptor.maxRepetitionLevel() >= 1
        && keyDescriptor.maxRepetitionLevel() == valueDescriptor.maxRepetitionLevel()
        && entryDefinition >= 1
        && valueDefinition >= entryDefinition && valueDefinition <= entryDefinition + 1;

    List<List<K>> keyLists = sharedEntryLayer
        ? keyColumn.decodeAsList(entryDefinition - 1, entryDefinition, keyDecoder)
        : keyColumn.decodeAsList(keyDecoder);
    List<List<V>> valueLists = sharedEntryLayer
        ? valueColumn.decodeAsList(entryDefinition - 1, entryDefinition, valueDecoder)
        : valueColumn.decodeAsList(valueDecoder);

    return io.github.aloksingh.parquet.model.NestedAssembler.zipEntryLists(
        keyLists, valueLists,
        io.github.aloksingh.parquet.model.NestedAssembler.DuplicateKeyPolicy.REJECT);
  }

  /**
   * Read a STRUCT structure from Parquet.
   * Structs are represented as multiple columns that need to be combined.
   * Each column represents one field of the struct, and all columns must have
   * the same number of rows.
   *
   * @param columnIndices array of column indices that form the struct
   * @param fieldNames names of the fields in the struct, must match the length
   *                   of columnIndices
   * @return list of structs represented as maps (field name to value), where each
   *         map corresponds to one row
   * @throws IOException if an I/O error occurs while reading the columns
   * @throws IllegalArgumentException if columnIndices and fieldNames have different lengths
   * @throws ParquetException if columns have mismatched row counts or unsupported types
   */
  public List<Map<String, Object>> readStruct(int[] columnIndices, String[] fieldNames)
      throws IOException {
    if (columnIndices.length != fieldNames.length) {
      throw new IllegalArgumentException("columnIndices and fieldNames must have the same length");
    }

    // Read all columns
    List<List<Object>> columnData = new ArrayList<>();
    int numRows = -1;

    for (int columnIndex : columnIndices) {
      ColumnValues column = rowGroupReader.readColumn(columnIndex);

      // Decode based on type
      List<Object> values;
      Type type = column.getType();
      switch (type) {
        case INT32:
          values = new ArrayList<>(column.decodeAsInt32());
          break;
        case INT64:
          values = new ArrayList<>(column.decodeAsInt64());
          break;
        case FLOAT:
          values = new ArrayList<>(column.decodeAsFloat());
          break;
        case DOUBLE:
          values = new ArrayList<>(column.decodeAsDouble());
          break;
        case BYTE_ARRAY:
          values = new ArrayList<>(column.decodeAsString());
          break;
        case BOOLEAN:
          values = new ArrayList<>(column.decodeAsBoolean());
          break;
        default:
          throw new ParquetException("Unsupported type for struct field: " + type);
      }

      if (numRows == -1) {
        numRows = values.size();
      } else if (values.size() != numRows) {
        throw new ParquetException("All columns in struct must have the same number of rows");
      }

      columnData.add(values);
    }

    // Combine into structs
    List<Map<String, Object>> result = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      Map<String, Object> struct = new LinkedHashMap<>();
      for (int j = 0; j < fieldNames.length; j++) {
        struct.put(fieldNames[j], columnData.get(j).get(i));
      }
      result.add(struct);
    }

    return result;
  }

  /**
   * Find column indexes for a given path prefix via the schema's central leaf resolver.
   * This is useful for finding all columns that belong to a struct or map.
   * <p>
   * For example, if the schema has columns with paths:
   * <ul>
   *   <li>["my_map", "key_value", "key"]</li>
   *   <li>["my_map", "key_value", "value"]</li>
   *   <li>["other_field"]</li>
   * </ul>
   * Then calling this method with pathPrefix ["my_map", "key_value"] would return
   * the indices of the first two columns.
   *
   * @param pathPrefix the prefix to match (e.g., ["my_map", "key_value"])
   * @return list of column indexes that match the prefix, in schema order
   */
  public List<Integer> findColumnsByPathPrefix(String[] pathPrefix) {
    return schema.leafIndexesByPathPrefix(pathPrefix);
  }
}
