package org.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.parquet.model.ColumnDescriptor;
import org.parquet.model.ColumnValues;
import org.parquet.model.LogicalColumnDescriptor;
import org.parquet.model.MapMetadata;
import org.parquet.model.Page;
import org.parquet.model.ParquetException;
import org.parquet.model.RowColumnGroup;
import org.parquet.model.SchemaDescriptor;
import org.parquet.model.SimpleRowColumnGroup;
import org.parquet.model.Type;

/**
 * Iterator that reads Parquet files row by row across all row groups.
 *
 * <p>This iterator reads entire row groups into memory at once for efficient access,
 * automatically loading the next row group when the current one is exhausted.
 * It handles both primitive columns and map columns (key-value pairs).
 *
 * <p>Usage example:
 * <pre>{@code
 * try (ParquetRowIterator iterator = new ParquetRowIterator(fileReader)) {
 *   while (iterator.hasNext()) {
 *     RowColumnGroup row = iterator.next();
 *     // Process row...
 *   }
 * }
 * }</pre>
 *
 * @see RowColumnGroupIterator
 * @see SerializedFileReader
 */
public class ParquetRowIterator implements RowColumnGroupIterator {
  private final SerializedFileReader fileReader;
  private final SchemaDescriptor schema;
  private final boolean closeFileReader;

  private int currentRowGroupIndex;
  private int currentRowIndex;
  private List<List<Object>> currentRowGroupData;
  private long currentRowGroupRowCount;

  /**
   * Create an iterator for a Parquet file
   *
   * @param fileReader      The file reader to iterate over
   * @param closeFileReader Whether to close the file reader when done
   */
  public ParquetRowIterator(SerializedFileReader fileReader, boolean closeFileReader) {
    this.fileReader = fileReader;
    this.schema = fileReader.getSchema();
    this.closeFileReader = closeFileReader;
    this.currentRowGroupIndex = 0;
    this.currentRowIndex = 0;
    this.currentRowGroupData = null;
    this.currentRowGroupRowCount = 0;

    // Load the first row group if available
    if (fileReader.getNumRowGroups() > 0) {
      loadRowGroup(0);
    }
  }

  /**
   * Create an iterator for a Parquet file.
   * The file reader will be closed automatically when {@link #close()} is called.
   *
   * @param fileReader The file reader to iterate over
   */
  public ParquetRowIterator(SerializedFileReader fileReader) {
    this(fileReader, true);
  }

  /**
   * Load a row group into memory.
   * Reads LOGICAL columns (primitives and maps) and stores them for iteration.
   *
   * @param rowGroupIndex The index of the row group to load
   * @throws ParquetException If reading the row group fails
   */
  private void loadRowGroup(int rowGroupIndex) {
    try {
      SerializedFileReader.RowGroupReader rowGroupReader =
          fileReader.getRowGroup(rowGroupIndex);

      currentRowGroupRowCount = rowGroupReader.getNumRows();
      currentRowGroupData = new ArrayList<>(schema.getNumLogicalColumns());

      // Read all LOGICAL columns for this row group
      for (int logicalColIdx = 0; logicalColIdx < schema.getNumLogicalColumns(); logicalColIdx++) {
        LogicalColumnDescriptor logicalCol = schema.getLogicalColumn(logicalColIdx);

        if (logicalCol.isMap()) {
          // Decode map column
          List<Map<String, Object>> maps =
              decodeMapColumn(rowGroupReader, logicalCol.getMapMetadata());
          currentRowGroupData.add(new ArrayList<>(maps));
        } else {
          // Decode primitive column
          ColumnDescriptor physicalCol = logicalCol.getPhysicalDescriptor();
          int physicalIndex = findPhysicalColumnIndex(physicalCol);
          ColumnValues columnValues = rowGroupReader.readColumn(physicalIndex);
          List<Object> values = decodeColumn(columnValues, physicalCol);
          currentRowGroupData.add(values);
        }
      }

      currentRowIndex = 0;
    } catch (IOException e) {
      throw new ParquetException("Failed to read row group " + rowGroupIndex, e);
    }
  }

  /**
   * Find the physical column index for a given column descriptor.
   *
   * @param physicalCol The column descriptor to search for
   * @return The physical index of the column in the schema
   * @throws ParquetException If the physical column is not found
   */
  private int findPhysicalColumnIndex(ColumnDescriptor physicalCol) {
    for (int i = 0; i < schema.getNumColumns(); i++) {
      if (schema.getColumn(i).getPathString().equals(physicalCol.getPathString())) {
        return i;
      }
    }
    throw new ParquetException("Physical column not found: " + physicalCol.getPathString());
  }

  /**
   * Decode a map column from its key and value physical columns.
   * Keys are always decoded as strings, and values are decoded based on their physical type.
   *
   * @param rowGroupReader The row group reader to read columns from
   * @param mapMetadata Metadata describing the map structure (key/value column indices and types)
   * @return A list of maps, one per row, with string keys and typed values
   * @throws IOException If reading the columns fails
   * @throws ParquetException If the map structure is invalid or contains unsupported types
   */
  private List<Map<String, Object>> decodeMapColumn(
      SerializedFileReader.RowGroupReader rowGroupReader,
      MapMetadata mapMetadata) throws IOException {

    // Read key and value columns
    ColumnValues keyColumn = rowGroupReader.readColumn(mapMetadata.keyColumnIndex());
    ColumnValues valueColumn = rowGroupReader.readColumn(mapMetadata.valueColumnIndex());

    // Decode keys (always strings)
    List<List<String>> keyLists = keyColumn.decodeAsList(obj -> {
      if (obj instanceof byte[]) {
        return new String((byte[]) obj, java.nio.charset.StandardCharsets.UTF_8);
      } else if (obj == null) {
        return null;
      }
      return obj.toString();
    });

    // Decode values based on their type
    List<List<Object>> valueLists;
    Type valueType = mapMetadata.valueType();

    switch (valueType) {
      case BYTE_ARRAY:
        // BYTE_ARRAY typically represents strings
        valueLists = valueColumn.decodeAsList(obj -> {
          if (obj instanceof byte[]) {
            return new String((byte[]) obj, java.nio.charset.StandardCharsets.UTF_8);
          } else if (obj == null) {
            return null;
          }
          return obj.toString();
        });
        break;
      case INT32:
        // INT32 for integer values
        valueLists = valueColumn.decodeAsList(obj -> {
          return obj; // Already Integer
        });
        break;
      case INT64:
        // INT64 for long values
        valueLists = valueColumn.decodeAsList(obj -> {
          return obj; // Already Long
        });
        break;
      case FLOAT:
        // FLOAT for float values
        valueLists = valueColumn.decodeAsList(obj -> {
          return obj; // Already Float
        });
        break;
      case DOUBLE:
        // DOUBLE for double values
        valueLists = valueColumn.decodeAsList(obj -> {
          return obj; // Already Double
        });
        break;
      default:
        throw new ParquetException("Unsupported map value type: " + valueType);
    }

    // Combine into maps
    if (keyLists.size() != valueLists.size()) {
      throw new ParquetException("Key and value lists have different sizes: " +
          keyLists.size() + " vs " + valueLists.size());
    }

    List<Map<String, Object>> result = new ArrayList<>();
    for (int i = 0; i < keyLists.size(); i++) {
      List<String> keys = keyLists.get(i);
      List<Object> values = valueLists.get(i);

      if (keys == null && values == null) {
        result.add(null);
      } else if (keys == null || values == null) {
        throw new ParquetException("Keys and values should both be null or both non-null");
      } else if (keys.size() != values.size()) {
        throw new ParquetException("Keys and values have different sizes at index " + i +
            ": " + keys.size() + " vs " + values.size());
      } else {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int j = 0; j < keys.size(); j++) {
          map.put(keys.get(j), values.get(j));
        }
        result.add(map);
      }
    }

    return result;
  }


  /**
   * Decode a column based on its physical type.
   * Byte arrays are decoded as UTF-8 strings by default.
   * Unsupported types return a list of nulls.
   *
   * @param columnValues The column values to decode
   * @param descriptor The column descriptor containing type information
   * @return A list of decoded values, one per row
   * @throws ParquetException If decoding fails
   */
  private List<Object> decodeColumn(ColumnValues columnValues,
                                    ColumnDescriptor descriptor) {
    Type type = descriptor.physicalType();

    // Cast to List<Object> to have a uniform return type
    try {
      switch (type) {
        case INT32:
          return new ArrayList<>(columnValues.decodeAsInt32());
        case INT64:
          return new ArrayList<>(columnValues.decodeAsInt64());
        case FLOAT:
          return new ArrayList<>(columnValues.decodeAsFloat());
        case DOUBLE:
          return new ArrayList<>(columnValues.decodeAsDouble());
        case BYTE_ARRAY:
          // Return as strings by default for byte arrays
          return new ArrayList<>(columnValues.decodeAsString());
        case BOOLEAN:
          return new ArrayList<>(columnValues.decodeAsBoolean());
        default:
          // For unsupported types (like INT96), return a list of nulls
          List<Object> nullList = new ArrayList<>();
          // We need to know the row count - get it from the first page
          int rowCount = columnValues.getPages().stream()
              .filter(p -> p instanceof Page.DataPage ||
                  p instanceof Page.DataPageV2)
              .mapToInt(p -> {
                if (p instanceof Page.DataPage) {
                  return ((Page.DataPage) p).numValues();
                } else if (p instanceof Page.DataPageV2) {
                  return ((Page.DataPageV2) p).numValues();
                }
                return 0;
              })
              .sum();
          for (int i = 0; i < rowCount; i++) {
            nullList.add(null);
          }
          return nullList;
      }
    } catch (Exception e) {
      throw new ParquetException("Failed to decode column " +
          descriptor.getPathString() + " of type " + type, e);
    }
  }

  /**
   * Check if there are more rows to iterate.
   *
   * @return true if there are more rows in the current row group or more row groups to read
   */
  @Override
  public boolean hasNext() {
    if (currentRowGroupData == null) {
      return false;
    }

    // Check if we have more rows in the current row group
    if (currentRowIndex < currentRowGroupRowCount) {
      return true;
    }

    // Check if there are more row groups
    return currentRowGroupIndex + 1 < fileReader.getNumRowGroups();
  }

  /**
   * Get the next row from the Parquet file.
   * Automatically loads the next row group when the current one is exhausted.
   *
   * @return A RowColumnGroup containing the values for all logical columns in the row
   * @throws NoSuchElementException If there are no more rows to read
   */
  @Override
  public RowColumnGroup next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more rows");
    }

    // If we've exhausted the current row group, load the next one
    if (currentRowIndex >= currentRowGroupRowCount) {
      currentRowGroupIndex++;
      loadRowGroup(currentRowGroupIndex);
    }

    // Build the row from all LOGICAL column values at the current index
    Object[] rowValues = new Object[schema.getNumLogicalColumns()];
    for (int logicalColIdx = 0; logicalColIdx < schema.getNumLogicalColumns(); logicalColIdx++) {
      List<Object> columnData = currentRowGroupData.get(logicalColIdx);
      rowValues[logicalColIdx] = columnData.get(currentRowIndex);
    }

    currentRowIndex++;

    return new SimpleRowColumnGroup(schema, rowValues);
  }

  /**
   * Close the underlying file reader if this iterator owns it.
   * Only closes the file reader if closeFileReader was set to true during construction.
   *
   * @throws IOException If closing the file reader fails
   */
  public void close() throws IOException {
    if (closeFileReader) {
      fileReader.close();
    }
  }

  /**
   * Get the total number of rows that will be iterated across all row groups.
   *
   * @return The total row count from the file metadata
   */
  public long getTotalRowCount() {
    return fileReader.getTotalRowCount();
  }

  /**
   * Get the schema for the rows being iterated.
   *
   * @return The schema descriptor containing all logical column definitions
   */
  public SchemaDescriptor getSchema() {
    return schema;
  }
}
