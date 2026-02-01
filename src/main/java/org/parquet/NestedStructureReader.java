package org.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ColumnValues;
import org.parquet.model.ParquetException;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.Type;

/**
 * Utility class for reading nested structures (MAP, STRUCT) from Parquet files.
 * Unlike lists which are stored in a single column, maps and structs can span multiple columns.
 */
public class NestedStructureReader {

  private final SerializedFileReader.RowGroupReader rowGroupReader;
  private final SchemaDescriptor schema;

  public NestedStructureReader(SerializedFileReader.RowGroupReader rowGroupReader,
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
   * optional group my_map (MAP) {
   * repeated group key_value {
   * required binary key (UTF8);
   * optional binary value (UTF8);
   * }
   * }
   *
   * @param keyColumnIndex   The column index for map keys
   * @param valueColumnIndex The column index for map values
   * @param keyDecoder       Function to decode key values
   * @param valueDecoder     Function to decode value values
   * @return List of maps
   */
  public <K, V> List<Map<K, V>> readMap(int keyColumnIndex, int valueColumnIndex,
                                        java.util.function.Function<Object, K> keyDecoder,
                                        java.util.function.Function<Object, V> valueDecoder)
      throws IOException {

    // Read both columns
    ColumnValues keyColumn = rowGroupReader.readColumn(keyColumnIndex);
    ColumnValues valueColumn = rowGroupReader.readColumn(valueColumnIndex);

    // Decode both as lists (they should have the same structure)
    List<List<K>> keyLists = keyColumn.decodeAsList(keyDecoder);
    List<List<V>> valueLists = valueColumn.decodeAsList(valueDecoder);

    if (keyLists.size() != valueLists.size()) {
      throw new ParquetException("Key and value lists have different sizes: " +
          keyLists.size() + " vs " + valueLists.size());
    }

    // Combine into maps
    List<Map<K, V>> result = new ArrayList<>();
    for (int i = 0; i < keyLists.size(); i++) {
      List<K> keys = keyLists.get(i);
      List<V> values = valueLists.get(i);

      if (keys == null && values == null) {
        result.add(null);
      } else if (keys == null || values == null) {
        throw new ParquetException("Key and value lists should both be null or both be non-null");
      } else if (keys.size() != values.size()) {
        throw new ParquetException("Key and value lists have different sizes at index " + i +
            ": " + keys.size() + " vs " + values.size());
      } else {
        Map<K, V> map = new LinkedHashMap<>();
        for (int j = 0; j < keys.size(); j++) {
          map.put(keys.get(j), values.get(j));
        }
        result.add(map);
      }
    }

    return result;
  }

  /**
   * Read a STRUCT structure from Parquet.
   * Structs are represented as multiple columns that need to be combined.
   *
   * @param columnIndices Array of column indices that form the struct
   * @param fieldNames    Names of the fields in the struct
   * @return List of structs represented as maps (field name -> value)
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
   * Find column indices for a given path prefix.
   * This is useful for finding all columns that belong to a struct or map.
   *
   * @param pathPrefix The prefix to match (e.g., ["my_map", "key_value"])
   * @return List of column indices that match the prefix
   */
  public List<Integer> findColumnsByPathPrefix(String[] pathPrefix) {
    List<Integer> result = new ArrayList<>();

    for (int i = 0; i < schema.getNumColumns(); i++) {
      ColumnDescriptor column = schema.getColumn(i);
      String[] columnPath = column.path();

      if (matchesPrefix(columnPath, pathPrefix)) {
        result.add(i);
      }
    }

    return result;
  }

  private boolean matchesPrefix(String[] path, String[] prefix) {
    if (path.length < prefix.length) {
      return false;
    }

    for (int i = 0; i < prefix.length; i++) {
      if (!path[i].equals(prefix[i])) {
        return false;
      }
    }

    return true;
  }
}
