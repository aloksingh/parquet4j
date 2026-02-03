package io.github.aloksingh.parquet.model;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import io.github.aloksingh.parquet.BitPackedReader;
import io.github.aloksingh.parquet.ByteStreamSplitDecoder;
import io.github.aloksingh.parquet.DeltaBinaryPackedDecoder;
import io.github.aloksingh.parquet.DeltaByteArrayDecoder;
import io.github.aloksingh.parquet.DeltaLengthByteArrayDecoder;
import io.github.aloksingh.parquet.RleDecoder;

/**
 * Represents a Parquet column with methods to decode values from various encodings.
 * <p>
 * This class handles the decoding of column data from Parquet files, supporting multiple
 * physical types (INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, BOOLEAN) and various encodings
 * including PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY, DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY,
 * DELTA_LENGTH_BYTE_ARRAY, and BYTE_STREAM_SPLIT.
 * <p>
 * It supports both Data Page V1 and Data Page V2 formats, and handles nullability through
 * definition levels and nested structures (lists, maps) through repetition levels.
 */
public class ColumnValues {
  /** The physical type of the column data */
  private final Type type;

  /** List of pages containing the column data (may include dictionary and data pages) */
  private final List<Page> pages;

  /** Column descriptor containing metadata like max definition/repetition levels */
  private final ColumnDescriptor columnDescriptor;

  /** Logical column descriptor for semantic type information */
  private final LogicalColumnDescriptor logicalColumnDescriptor;

  /**
   * Constructs a ColumnValues instance.
   *
   * @param type the physical type of the column
   * @param pages list of pages containing the column data
   * @param columnDescriptor metadata descriptor for the column
   * @param logicalColumnDescriptor logical type descriptor for the column
   */
  public ColumnValues(Type type, List<Page> pages,
                      ColumnDescriptor columnDescriptor,
                      LogicalColumnDescriptor logicalColumnDescriptor) {
    this.type = type;
    this.pages = pages;
    this.columnDescriptor = columnDescriptor;
    this.logicalColumnDescriptor = logicalColumnDescriptor;
  }

  /**
   * Returns the physical type of this column.
   *
   * @return the Type enum value
   */
  public Type getType() {
    return type;
  }

  /**
   * Returns the list of pages for this column.
   *
   * @return list of Page objects (dictionary and data pages)
   */
  public List<Page> getPages() {
    return pages;
  }

  /**
   * Returns the logical column descriptor.
   *
   * @return the LogicalColumnDescriptor instance
   */
  public LogicalColumnDescriptor getLogicalColumnDescriptor() {
    return logicalColumnDescriptor;
  }

  /**
   * Decodes all column values as 32-bit integers.
   * <p>
   * Supports INT32 physical type with the following encodings:
   * <ul>
   *   <li>PLAIN - values stored directly as 4-byte little-endian integers</li>
   *   <li>PLAIN_DICTIONARY / RLE_DICTIONARY - values referenced via dictionary indices</li>
   *   <li>DELTA_BINARY_PACKED - delta-encoded integers for better compression</li>
   * </ul>
   * Handles both Data Page V1 and V2 formats, including null values via definition levels.
   *
   * @return list of Integer values, with null entries for null values
   * @throws ParquetException if column type is not INT32 or encoding is unsupported
   */
  public List<Integer> decodeAsInt32() {
    if (type != Type.INT32) {
      throw new ParquetException("Column type is not INT32: " + type);
    }

    List<Integer> values = new ArrayList<>();
    int[] dictionary = null;

    for (Page page : pages) {
      if (page instanceof Page.DictionaryPage dictPage) {
        // Build dictionary
        ByteBuffer buffer = dictPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        dictionary = new int[dictPage.numValues()];
        for (int i = 0; i < dictPage.numValues(); i++) {
          dictionary[i] = buffer.getInt();
        }
      } else if (page instanceof Page.DataPage dataPage) {
        ByteBuffer buffer = dataPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int[] defLevels = null;
        int maxDefLevel = columnDescriptor.maxDefinitionLevel();
        if (dataPage.definitionLevelByteLen() > 0) {
          defLevels = readLevels(buffer, dataPage.definitionLevelByteLen(), dataPage.numValues(),
              maxDefLevel);
        } else {
          // No definition levels - all values are non-null
          defLevels = new int[dataPage.numValues()];
          java.util.Arrays.fill(defLevels, maxDefLevel);
        }

        // Skip repetition levels
        if (dataPage.repetitionLevelByteLen() > 0) {
          buffer.position(buffer.position() + dataPage.repetitionLevelByteLen());
        }

        if (dataPage.encoding() == Encoding.PLAIN) {
          // Read values directly
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              // Null value
              values.add(null);
            } else {
              values.add(buffer.getInt());
            }
          }
        } else if (dataPage.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPage.encoding() == Encoding.RLE_DICTIONARY) {
          // Read dictionary indices
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }

          // Count non-null values - dictionary indices only encode non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          int[] indices = readDictionaryIndices(buffer, dictionary.length, nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(dictionary[valueIndex < indices.length ? indices[valueIndex++] : 0]);
            }
          }
        } else if (dataPage.encoding() == Encoding.DELTA_BINARY_PACKED) {
          // DELTA_BINARY_PACKED encoding
          // DELTA_BINARY_PACKED encodes ONLY non-null values
          // Definition levels indicate which positions are null

          // Count non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(buffer, false);
          int[] decodedValues = decoder.decodeInt32(nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPage.encoding());
        }
      } else if (page instanceof Page.DataPageV2 dataPageV2) {
        // Data Page V2 - levels are stored separately
        ByteBuffer buffer = dataPageV2.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int maxDefLevel = columnDescriptor.maxDefinitionLevel();
        ByteBuffer defLevelBuffer = dataPageV2.definitionLevels();
        int[] defLevels = readLevelsV2(defLevelBuffer, dataPageV2.numValues(), maxDefLevel);

        if (dataPageV2.encoding() == Encoding.PLAIN) {
          // Read values directly
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              // Null value
              values.add(null);
            } else {
              values.add(buffer.getInt());
            }
          }
        } else if (dataPageV2.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPageV2.encoding() == Encoding.RLE_DICTIONARY) {
          // Read dictionary indices
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }

          // Count non-null values - dictionary indices only encode non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          int[] indices =
              readDictionaryIndicesV2(buffer, dictionary.length, dataPageV2, nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(dictionary[valueIndex < indices.length ? indices[valueIndex++] : 0]);
            }
          }
        } else if (dataPageV2.encoding() == Encoding.DELTA_BINARY_PACKED) {
          // DELTA_BINARY_PACKED encoding
          // DELTA_BINARY_PACKED encodes ONLY non-null values
          // Definition levels indicate which positions are null

          // Count non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(buffer, false);
          int[] decodedValues = decoder.decodeInt32(nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPageV2.encoding());
        }
      }
    }

    return values;
  }

  /**
   * Reads definition levels from a Data Page V2.
   * <p>
   * Definition levels indicate nullability in the column data:
   * <ul>
   *   <li>0 = null value</li>
   *   <li>1 = non-null value (for optional columns with max_def_level = 1)</li>
   *   <li>Higher values indicate nested structure presence</li>
   * </ul>
   *
   * @param dataPageV2 the Data Page V2 to read from
   * @param columnDescriptor descriptor containing max definition level
   * @return array of definition levels, one per value
   */
  private int[] readDefinitionLevelsV2(Page.DataPageV2 dataPageV2,
                                       ColumnDescriptor columnDescriptor) {
    if (columnDescriptor.maxDefinitionLevel() == 0) {
      // Required column - all values are non-null
      int[] levels = new int[dataPageV2.numValues()];
      java.util.Arrays.fill(levels, 1);
      return levels;
    }

    ByteBuffer defLevels = dataPageV2.definitionLevels().duplicate();

    if (defLevels.remaining() == 0) {
      // No definition level data - assume all non-null
      int[] levels = new int[dataPageV2.numValues()];
      java.util.Arrays.fill(levels, 1);
      return levels;
    }

    // For Data Page V2, the definition levels buffer is already extracted with the correct size
    // from the page header (definition_levels_byte_length). The buffer contains RLE-encoded data
    // directly without any additional length prefix.
    defLevels.order(java.nio.ByteOrder.LITTLE_ENDIAN);

    // Decode RLE data with bit-width for max definition level
    int bitWidth = getBitWidth(columnDescriptor.maxDefinitionLevel());
    RleDecoder decoder = new RleDecoder(defLevels, bitWidth, dataPageV2.numValues());
    return decoder.readAll();
  }

  /**
   * Calculates the minimum bit width needed to represent values from 0 to maxValue.
   *
   * @param maxValue the maximum value to represent
   * @return the number of bits required (0 if maxValue is 0)
   */
  private int getBitWidth(int maxValue) {
    if (maxValue == 0) {
      return 0;
    }
    return 32 - Integer.numberOfLeadingZeros(maxValue);
  }

  /**
   * Decodes all column values as 64-bit long integers.
   * <p>
   * Supports INT64 physical type with PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY,
   * and DELTA_BINARY_PACKED encodings. Handles both Data Page V1 and V2 formats,
   * including null values via definition levels.
   *
   * @return list of Long values, with null entries for null values
   * @throws ParquetException if column type is not INT64 or encoding is unsupported
   */
  public List<Long> decodeAsInt64() {
    if (type != Type.INT64) {
      throw new ParquetException("Column type is not INT64: " + type);
    }

    List<Long> values = new ArrayList<>();
    long[] dictionary = null;

    for (Page page : pages) {
      if (page instanceof Page.DictionaryPage dictPage) {
        // Build dictionary
        ByteBuffer buffer = dictPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        dictionary = new long[dictPage.numValues()];
        for (int i = 0; i < dictPage.numValues(); i++) {
          dictionary[i] = buffer.getLong();
        }
      } else if (page instanceof Page.DataPage dataPage) {
        ByteBuffer buffer = dataPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int[] defLevels = null;
        int maxDefLevel = columnDescriptor.maxDefinitionLevel();
        if (dataPage.definitionLevelByteLen() > 0) {
          defLevels = readLevels(buffer, dataPage.definitionLevelByteLen(), dataPage.numValues(),
              maxDefLevel);
        } else {
          // No definition levels - all values are non-null
          defLevels = new int[dataPage.numValues()];
          java.util.Arrays.fill(defLevels, maxDefLevel);
        }

        // Skip repetition levels
        if (dataPage.repetitionLevelByteLen() > 0) {
          buffer.position(buffer.position() + dataPage.repetitionLevelByteLen());
        }

        if (dataPage.encoding() == Encoding.PLAIN) {
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(buffer.getLong());
            }
          }
        } else if (dataPage.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPage.encoding() == Encoding.RLE_DICTIONARY) {
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }

          // Count non-null values - dictionary indices only encode non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          int[] indices = readDictionaryIndices(buffer, dictionary.length, nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(dictionary[valueIndex < indices.length ? indices[valueIndex++] : 0]);
            }
          }
        } else if (dataPage.encoding() == Encoding.DELTA_BINARY_PACKED) {
          // DELTA_BINARY_PACKED encoding
          // DELTA_BINARY_PACKED encodes ONLY non-null values
          // Definition levels indicate which positions are null

          // Count non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(buffer, true);
          long[] decodedValues = decoder.decodeInt64(nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPage.encoding());
        }
      } else if (page instanceof Page.DataPageV2 dataPageV2) {
        ByteBuffer buffer = dataPageV2.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int maxDefLevel = columnDescriptor.maxDefinitionLevel();
        ByteBuffer defLevelBuffer = dataPageV2.definitionLevels();
        int[] defLevels = readLevelsV2(defLevelBuffer, dataPageV2.numValues(), maxDefLevel);

        if (dataPageV2.encoding() == Encoding.PLAIN) {
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              // Null value
              values.add(null);
            } else {
              values.add(buffer.getLong());
            }
          }
        } else if (dataPageV2.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPageV2.encoding() == Encoding.RLE_DICTIONARY) {
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }

          // Count non-null values - dictionary indices only encode non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          int[] indices =
              readDictionaryIndicesV2(buffer, dictionary.length, dataPageV2, nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(dictionary[valueIndex < indices.length ? indices[valueIndex++] : 0]);
            }
          }
        } else if (dataPageV2.encoding() == Encoding.DELTA_BINARY_PACKED) {
          // DELTA_BINARY_PACKED encoding
          // DELTA_BINARY_PACKED encodes ONLY non-null values
          // Definition levels indicate which positions are null

          // Count non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(buffer, true);
          long[] decodedValues = decoder.decodeInt64(nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPageV2.encoding());
        }
      }
    }

    return values;
  }

  /**
   * Decodes all column values as single-precision floating point numbers.
   * <p>
   * Supports FLOAT physical type with PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY,
   * and BYTE_STREAM_SPLIT encodings. BYTE_STREAM_SPLIT provides better compression
   * for floating point data by separating bytes of each float value.
   *
   * @return list of Float values, with null entries for null values
   * @throws ParquetException if column type is not FLOAT or encoding is unsupported
   */
  public List<Float> decodeAsFloat() {
    if (type != Type.FLOAT) {
      throw new ParquetException("Column type is not FLOAT: " + type);
    }

    List<Float> values = new ArrayList<>();
    float[] dictionary = null;

    for (Page page : pages) {
      if (page instanceof Page.DictionaryPage dictPage) {
        ByteBuffer buffer = dictPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        dictionary = new float[dictPage.numValues()];
        for (int i = 0; i < dictPage.numValues(); i++) {
          dictionary[i] = buffer.getFloat();
        }
      } else if (page instanceof Page.DataPage dataPage) {
        ByteBuffer buffer = dataPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int[] defLevels = null;
        int maxDefLevel = columnDescriptor.maxDefinitionLevel();
        if (dataPage.definitionLevelByteLen() > 0) {
          defLevels = readLevels(buffer, dataPage.definitionLevelByteLen(), dataPage.numValues(),
              maxDefLevel);
        } else {
          // No definition levels - all values are non-null
          defLevels = new int[dataPage.numValues()];
          java.util.Arrays.fill(defLevels, maxDefLevel);
        }

        // Skip repetition levels
        if (dataPage.repetitionLevelByteLen() > 0) {
          buffer.position(buffer.position() + dataPage.repetitionLevelByteLen());
        }

        if (dataPage.encoding() == Encoding.PLAIN) {
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(buffer.getFloat());
            }
          }
        } else if (dataPage.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPage.encoding() == Encoding.RLE_DICTIONARY) {
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }

          // Count non-null values - dictionary indices only encode non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          int[] indices = readDictionaryIndices(buffer, dictionary.length, nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(dictionary[valueIndex < indices.length ? indices[valueIndex++] : 0]);
            }
          }
        } else if (dataPage.encoding() == Encoding.BYTE_STREAM_SPLIT) {
          // BYTE_STREAM_SPLIT encoding for floats
          // Count non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          ByteStreamSplitDecoder decoder = new ByteStreamSplitDecoder(buffer, nonNullCount, 4);
          float[] decodedValues = decoder.decodeFloat();

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPage.encoding());
        }
      } else if (page instanceof Page.DataPageV2 dataPageV2) {
        ByteBuffer buffer = dataPageV2.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        if (dataPageV2.encoding() == Encoding.PLAIN) {
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            values.add(buffer.getFloat());
          }
        } else if (dataPageV2.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPageV2.encoding() == Encoding.RLE_DICTIONARY) {
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }
          int[] indices = readDictionaryIndicesV2(buffer, dictionary.length, dataPageV2);
          for (int index : indices) {
            values.add(dictionary[index]);
          }
        } else if (dataPageV2.encoding() == Encoding.BYTE_STREAM_SPLIT) {
          // BYTE_STREAM_SPLIT encoding for floats in Data Page V2
          ByteStreamSplitDecoder decoder =
              new ByteStreamSplitDecoder(buffer, dataPageV2.numValues(), 4);
          float[] decodedValues = decoder.decodeFloat();
          for (float value : decodedValues) {
            values.add(value);
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPageV2.encoding());
        }
      }
    }

    return values;
  }

  /**
   * Decodes all column values as double-precision floating point numbers.
   * <p>
   * Supports DOUBLE physical type with PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY,
   * and BYTE_STREAM_SPLIT encodings. BYTE_STREAM_SPLIT provides better compression
   * for floating point data by separating bytes of each double value.
   *
   * @return list of Double values, with null entries for null values
   * @throws ParquetException if column type is not DOUBLE or encoding is unsupported
   */
  public List<Double> decodeAsDouble() {
    if (type != Type.DOUBLE) {
      throw new ParquetException("Column type is not DOUBLE: " + type);
    }

    List<Double> values = new ArrayList<>();
    double[] dictionary = null;

    for (Page page : pages) {
      if (page instanceof Page.DictionaryPage dictPage) {
        ByteBuffer buffer = dictPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        dictionary = new double[dictPage.numValues()];
        for (int i = 0; i < dictPage.numValues(); i++) {
          dictionary[i] = buffer.getDouble();
        }
      } else if (page instanceof Page.DataPage dataPage) {
        ByteBuffer buffer = dataPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int[] defLevels = null;
        int maxDefLevel = columnDescriptor.maxDefinitionLevel();
        if (dataPage.definitionLevelByteLen() > 0) {
          defLevels = readLevels(buffer, dataPage.definitionLevelByteLen(), dataPage.numValues(),
              maxDefLevel);
        } else {
          // No definition levels - all values are non-null
          defLevels = new int[dataPage.numValues()];
          java.util.Arrays.fill(defLevels, maxDefLevel);
        }

        // Skip repetition levels
        if (dataPage.repetitionLevelByteLen() > 0) {
          buffer.position(buffer.position() + dataPage.repetitionLevelByteLen());
        }

        if (dataPage.encoding() == Encoding.PLAIN) {
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(buffer.getDouble());
            }
          }
        } else if (dataPage.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPage.encoding() == Encoding.RLE_DICTIONARY) {
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }

          // Count non-null values - dictionary indices only encode non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          int[] indices = readDictionaryIndices(buffer, dictionary.length, nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(dictionary[valueIndex < indices.length ? indices[valueIndex++] : 0]);
            }
          }
        } else if (dataPage.encoding() == Encoding.BYTE_STREAM_SPLIT) {
          // BYTE_STREAM_SPLIT encoding for doubles
          // Count non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          ByteStreamSplitDecoder decoder = new ByteStreamSplitDecoder(buffer, nonNullCount, 8);
          double[] decodedValues = decoder.decodeDouble();

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPage.encoding());
        }
      } else if (page instanceof Page.DataPageV2 dataPageV2) {
        ByteBuffer buffer = dataPageV2.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        if (dataPageV2.encoding() == Encoding.PLAIN) {
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            values.add(buffer.getDouble());
          }
        } else if (dataPageV2.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPageV2.encoding() == Encoding.RLE_DICTIONARY) {
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }
          int[] indices = readDictionaryIndicesV2(buffer, dictionary.length, dataPageV2);
          for (int index : indices) {
            values.add(dictionary[index]);
          }
        } else if (dataPageV2.encoding() == Encoding.BYTE_STREAM_SPLIT) {
          // BYTE_STREAM_SPLIT encoding for doubles in Data Page V2
          ByteStreamSplitDecoder decoder =
              new ByteStreamSplitDecoder(buffer, dataPageV2.numValues(), 8);
          double[] decodedValues = decoder.decodeDouble();
          for (double value : decodedValues) {
            values.add(value);
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPageV2.encoding());
        }
      }
    }

    return values;
  }

  /**
   * Decodes all column values as byte arrays.
   * <p>
   * Supports BYTE_ARRAY physical type with the following encodings:
   * <ul>
   *   <li>PLAIN - length-prefixed byte arrays (4-byte little-endian length + data)</li>
   *   <li>PLAIN_DICTIONARY / RLE_DICTIONARY - dictionary-encoded references</li>
   *   <li>DELTA_BYTE_ARRAY - delta encoding for byte arrays with common prefixes</li>
   *   <li>DELTA_LENGTH_BYTE_ARRAY - delta encoding of lengths followed by data</li>
   * </ul>
   *
   * @return list of byte arrays, with null entries for null values
   * @throws ParquetException if column type is not BYTE_ARRAY or encoding is unsupported
   */
  public List<byte[]> decodeAsByteArray() {
    if (type != Type.BYTE_ARRAY) {
      throw new ParquetException("Column type is not BYTE_ARRAY: " + type);
    }

    List<byte[]> values = new ArrayList<>();
    byte[][] dictionary = null;

    for (Page page : pages) {
      if (page instanceof Page.DictionaryPage dictPage) {
        ByteBuffer buffer = dictPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        dictionary = new byte[dictPage.numValues()][];
        for (int i = 0; i < dictPage.numValues(); i++) {
          int length = buffer.getInt();
          byte[] value = new byte[length];
          buffer.get(value);
          dictionary[i] = value;
        }
      } else if (page instanceof Page.DataPage dataPage) {
        ByteBuffer buffer = dataPage.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int[] defLevels = null;
        int maxDefLevel = columnDescriptor.maxDefinitionLevel();
        if (dataPage.definitionLevelByteLen() > 0) {
          defLevels = readLevels(buffer, dataPage.definitionLevelByteLen(), dataPage.numValues(),
              maxDefLevel);
        } else {
          // No definition levels - all values are non-null
          defLevels = new int[dataPage.numValues()];
          java.util.Arrays.fill(defLevels, maxDefLevel);
        }

        // Skip repetition levels
        if (dataPage.repetitionLevelByteLen() > 0) {
          buffer.position(buffer.position() + dataPage.repetitionLevelByteLen());
        }

        if (dataPage.encoding() == Encoding.PLAIN) {
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              // Null value (definition level less than max means null)
              values.add(null);
            } else {
              // Non-null value - read from buffer
              int length = buffer.getInt();
              byte[] value = new byte[length];
              buffer.get(value);
              values.add(value);
            }
          }
        } else if (dataPage.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPage.encoding() == Encoding.RLE_DICTIONARY) {
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }

          // Count non-null values - dictionary indices only encode non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          int[] indices = readDictionaryIndices(buffer, dictionary.length, nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(dictionary[valueIndex < indices.length ? indices[valueIndex++] : 0]);
            }
          }
        } else if (dataPage.encoding() == Encoding.DELTA_BYTE_ARRAY) {
          // DELTA_BYTE_ARRAY encoding
          // DELTA_BYTE_ARRAY encodes ONLY non-null values
          // Definition levels indicate which positions are null

          // Count non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          System.err.println("DEBUG V1 DELTA_BYTE_ARRAY: col=" + columnDescriptor.getPathString() +
              ", numValues=" + dataPage.numValues() +
              ", maxDefLevel=" + maxDefLevel +
              ", nonNullCount=" + nonNullCount +
              ", buffer.remaining()=" + buffer.remaining());

          DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder(buffer);
          byte[][] decodedValues = decoder.decode(nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else if (dataPage.encoding() == Encoding.DELTA_LENGTH_BYTE_ARRAY) {
          // DELTA_LENGTH_BYTE_ARRAY encoding
          // Similar to DELTA_BYTE_ARRAY, this encodes ONLY non-null values
          // Definition levels indicate which positions are null

          // Count non-null values
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          System.err.println(
              "DEBUG V1 DELTA_LENGTH_BYTE_ARRAY: col=" + columnDescriptor.getPathString() +
                  ", numValues=" + dataPage.numValues() +
                  ", maxDefLevel=" + maxDefLevel +
                  ", nonNullCount=" + nonNullCount +
                  ", buffer.remaining()=" + buffer.remaining());

          DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder(buffer);
          decoder.init(nonNullCount);
          byte[][] decodedValues = decoder.decode(nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPage.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPage.encoding());
        }
      } else if (page instanceof Page.DataPageV2 dataPageV2) {
        ByteBuffer buffer = dataPageV2.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int[] defLevels = readDefinitionLevelsV2(dataPageV2, columnDescriptor);

        if (dataPageV2.encoding() == Encoding.PLAIN) {
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] == 0) {
              // Null value
              values.add(null);
            } else {
              // Non-null value - read from buffer
              int length = buffer.getInt();
              byte[] value = new byte[length];
              buffer.get(value);
              values.add(value);
            }
          }
        } else if (dataPageV2.encoding() == Encoding.PLAIN_DICTIONARY ||
            dataPageV2.encoding() == Encoding.RLE_DICTIONARY) {
          if (dictionary == null) {
            throw new ParquetException("Dictionary page not found for dictionary-encoded data");
          }

          // Read dictionary indices (only for non-null values)
          int nonNullCount = (int) java.util.Arrays.stream(defLevels).filter(l -> l > 0).count();
          int[] indices =
              readDictionaryIndicesV2(buffer, dictionary.length, dataPageV2, nonNullCount);

          // Map indices back to values, inserting nulls where needed
          int indexPos = 0;
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] == 0) {
              values.add(null);
            } else {
              values.add(dictionary[indices[indexPos++]]);
            }
          }
        } else if (dataPageV2.encoding() == Encoding.DELTA_BYTE_ARRAY) {
          // DELTA_BYTE_ARRAY encoding
          // DELTA_BYTE_ARRAY encodes ONLY non-null values
          // Definition levels indicate which positions are null

          int maxDefLevel = columnDescriptor.maxDefinitionLevel();
          // Count non-null values (definition level >= maxDefinitionLevel means non-null)
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          System.err.println("DEBUG V2 DELTA_BYTE_ARRAY: col=" + columnDescriptor.getPathString() +
              ", numValues=" + dataPageV2.numValues() +
              ", maxDefLevel=" + maxDefLevel +
              ", nonNullCount=" + nonNullCount +
              ", buffer.remaining()=" + buffer.remaining() +
              ", first 10 defLevels=" + java.util.Arrays.toString(java.util.Arrays.copyOf(defLevels, Math.min(10, defLevels.length))));

          DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder(buffer);
          byte[][] decodedValues = decoder.decode(nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else if (dataPageV2.encoding() == Encoding.DELTA_LENGTH_BYTE_ARRAY) {
          // DELTA_LENGTH_BYTE_ARRAY encoding
          // Similar to DELTA_BYTE_ARRAY, this encodes ONLY non-null values
          // Definition levels indicate which positions are null

          int maxDefLevel = columnDescriptor.maxDefinitionLevel();
          // Count non-null values (definition level >= maxDefinitionLevel means non-null)
          int nonNullCount = 0;
          for (int defLevel : defLevels) {
            if (defLevel >= maxDefLevel) {
              nonNullCount++;
            }
          }

          System.err.println(
              "DEBUG V2 DELTA_LENGTH_BYTE_ARRAY: col=" + columnDescriptor.getPathString() +
                  ", numValues=" + dataPageV2.numValues() +
                  ", maxDefLevel=" + maxDefLevel +
                  ", nonNullCount=" + nonNullCount +
                  ", buffer.remaining()=" + buffer.remaining() +
                  ", first 10 defLevels=" + java.util.Arrays.toString(
                  java.util.Arrays.copyOf(defLevels, Math.min(10, defLevels.length))));

          DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder(buffer);
          decoder.init(nonNullCount);
          byte[][] decodedValues = decoder.decode(nonNullCount);

          // Map non-null values to positions based on definition levels
          int valueIndex = 0;
          for (int i = 0; i < dataPageV2.numValues(); i++) {
            if (defLevels[i] < maxDefLevel) {
              values.add(null);
            } else {
              values.add(decodedValues[valueIndex++]);
            }
          }
        } else {
          throw new ParquetException("Unsupported encoding: " + dataPageV2.encoding());
        }
      }
    }

    return values;
  }

  /**
   * Decodes byte array column values as UTF-8 strings.
   * <p>
   * This is a convenience method that decodes BYTE_ARRAY data and converts each
   * byte array to a String using UTF-8 encoding. Null byte arrays result in null strings.
   *
   * @return list of String values, with null entries for null values
   * @throws ParquetException if column type is not BYTE_ARRAY or encoding is unsupported
   */
  public List<String> decodeAsString() {
    List<byte[]> byteArrays = decodeAsByteArray();
    List<String> strings = new ArrayList<>(byteArrays.size());
    for (byte[] bytes : byteArrays) {
      if (bytes == null) {
        strings.add(null);
      } else {
        strings.add(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
      }
    }
    return strings;
  }

  /**
   * Decodes all column values as booleans.
   * <p>
   * Supports BOOLEAN physical type with PLAIN (bit-packed) and RLE encodings.
   * Booleans are stored efficiently using bit-packing (8 booleans per byte) or
   * RLE encoding for runs of repeated values.
   *
   * @return list of Boolean values, with null entries for null values
   * @throws ParquetException if column type is not BOOLEAN or encoding is unsupported
   */
  public List<Boolean> decodeAsBoolean() {
    if (type != Type.BOOLEAN) {
      throw new ParquetException("Column type is not BOOLEAN: " + type);
    }

    List<Boolean> values = new ArrayList<>();

    for (Page page : pages) {
      if (page instanceof Page.DataPage(
          ByteBuffer data, int numValues, Encoding encoding, int definitionLevelByteLen,
          int repetitionLevelByteLen
      )) {
        if (encoding != Encoding.PLAIN &&
            encoding != Encoding.RLE) {
          throw new ParquetException("Unsupported encoding for BOOLEAN: " +
              encoding);
        }

        ByteBuffer buffer = data.duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int[] defLevels = null;
        int maxDefLevel = columnDescriptor.maxDefinitionLevel();
        if (definitionLevelByteLen > 0) {
          defLevels = readLevels(buffer, definitionLevelByteLen, numValues, maxDefLevel);
        } else {
          // No definition levels - all values are non-null
          defLevels = new int[numValues];
          java.util.Arrays.fill(defLevels, maxDefLevel);
        }

        // Skip repetition levels
        if (repetitionLevelByteLen > 0) {
          buffer.position(buffer.position() + repetitionLevelByteLen);
        }

        // Count non-null values to determine how many booleans to read
        int nonNullCount = 0;
        for (int defLevel : defLevels) {
          if (defLevel >= maxDefLevel) {
            nonNullCount++;
          }
        }

        // Read boolean values (only non-null values are encoded)
        boolean[] boolArray;
        if (encoding == Encoding.PLAIN) {
          // Read bit-packed booleans
          boolArray = BitPackedReader.readBooleans(buffer, nonNullCount);
        } else { // Encoding.RLE
          // RLE encoding for booleans
          // First byte is the bit width (should be 1 for booleans)
          int bitWidth = buffer.get() & 0xFF;
          if (bitWidth != 1) {
            throw new ParquetException("Expected bit width 1 for BOOLEAN RLE, got: " + bitWidth);
          }

          // Read RLE/bit-packed hybrid data
          boolArray = readRleBooleans(buffer, nonNullCount);
        }

        // Map boolean values to positions based on definition levels
        int boolIndex = 0;
        for (int i = 0; i < numValues; i++) {
          if (defLevels[i] < maxDefLevel) {
            // Null value
            values.add(null);
          } else {
            // Non-null value
            values.add(boolArray[boolIndex++]);
          }
        }
      } else if (page instanceof Page.DataPageV2 dataPageV2) {
        if (dataPageV2.encoding() != Encoding.PLAIN &&
            dataPageV2.encoding() != Encoding.RLE) {
          throw new ParquetException("Unsupported encoding for BOOLEAN: " +
              dataPageV2.encoding());
        }

        ByteBuffer buffer = dataPageV2.data().duplicate();
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read definition levels to determine which values are null
        int maxDefLevel = columnDescriptor.maxDefinitionLevel();
        ByteBuffer defLevelBuffer = dataPageV2.definitionLevels();
        int[] defLevels = readLevelsV2(defLevelBuffer, dataPageV2.numValues(), maxDefLevel);

        // Count non-null values
        int nonNullCount = 0;
        for (int defLevel : defLevels) {
          if (defLevel >= maxDefLevel) {
            nonNullCount++;
          }
        }

        // Read boolean values (only non-null values are encoded)
        boolean[] boolArray;
        if (dataPageV2.encoding() == Encoding.PLAIN) {
          // Read bit-packed booleans
          boolArray = BitPackedReader.readBooleans(buffer, nonNullCount);
        } else { // Encoding.RLE
          // RLE encoding for booleans in Data Page V2
          // In Data Page V2, the RLE data has a 4-byte length prefix (little-endian)
          // followed by the RLE-encoded data with bit-width 1 for booleans

          if (buffer.remaining() >= 4) {
            // Read 4-byte length (little-endian)
            int length = buffer.getInt();

            if (length > 0 && length <= buffer.remaining()) {
              // Create a slice for the RLE data
              ByteBuffer rleData = buffer.slice();
              rleData.limit(length);

              // For booleans, bit-width is always 1
              boolArray = readRleBooleansWithBitWidth(rleData, nonNullCount, 1);
              buffer.position(buffer.position() + length);
            } else {
              throw new ParquetException("Invalid RLE length for BOOLEAN Data Page V2: " + length);
            }
          } else {
            throw new ParquetException("Not enough data for RLE BOOLEAN Data Page V2");
          }
        }

        // Map boolean values to positions based on definition levels
        int boolIndex = 0;
        for (int i = 0; i < dataPageV2.numValues(); i++) {
          if (defLevels[i] < maxDefLevel) {
            // Null value
            values.add(null);
          } else {
            // Non-null value
            values.add(boolArray[boolIndex++]);
          }
        }
      }
    }

    return values;
  }

  /**
   * Reads RLE-encoded boolean values from a buffer.
   * <p>
   * RLE/bit-packed hybrid encoding uses run headers to indicate either:
   * <ul>
   *   <li>RLE run (header & 1 == 0): repeated value, length = header >> 1</li>
   *   <li>Bit-packed run (header & 1 == 1): bit-packed values, count = (header >> 1) * 8</li>
   * </ul>
   *
   * @param buffer the buffer containing RLE-encoded data
   * @param numValues the number of boolean values to read
   * @return array of decoded boolean values
   */
  private boolean[] readRleBooleans(ByteBuffer buffer, int numValues) {
    boolean[] result = new boolean[numValues];
    int valuesRead = 0;

    while (valuesRead < numValues && buffer.hasRemaining()) {
      // Read the run header
      int header = readUnsignedVarInt(buffer);

      if ((header & 1) == 0) {
        // RLE run: header >> 1 = run length
        int runLength = header >> 1;
        boolean value = (buffer.get() & 1) != 0;

        for (int i = 0; i < runLength && valuesRead < numValues; i++) {
          result[valuesRead++] = value;
        }
      } else {
        // Bit-packed run: (header >> 1) * 8 = number of values
        int numGroups = header >> 1;
        int numBits = numGroups * 8;
        int valuesToRead = Math.min(numBits, numValues - valuesRead);

        boolean[] bitPackedValues = BitPackedReader.readBooleans(buffer, valuesToRead);
        System.arraycopy(bitPackedValues, 0, result, valuesRead, valuesToRead);
        valuesRead += valuesToRead;
      }
    }

    return result;
  }

  /**
   * Reads RLE-encoded boolean values with a specified bit-width.
   * <p>
   * Data Page V2 may use bit-width greater than 1 for booleans. Non-zero values
   * are treated as true, zero values as false.
   *
   * @param buffer the buffer containing RLE-encoded data
   * @param numValues the number of boolean values to read
   * @param bitWidth the bit-width used for encoding (typically 1 for booleans)
   * @return array of decoded boolean values
   */
  private boolean[] readRleBooleansWithBitWidth(ByteBuffer buffer, int numValues, int bitWidth) {
    if (bitWidth == 1) {
      // Standard bit-width for booleans
      return readRleBooleans(buffer, numValues);
    }

    // For bit-width > 1, use RleDecoder to read values, then convert to booleans
    // Non-zero values are treated as true
    RleDecoder decoder = new RleDecoder(buffer, bitWidth, numValues);
    int[] intValues = decoder.readAll();

    boolean[] result = new boolean[intValues.length];
    for (int i = 0; i < intValues.length; i++) {
      result[i] = intValues[i] != 0;
    }

    return result;
  }

  /**
   * Reads an unsigned variable-length integer from the buffer.
   * <p>
   * Uses little-endian base-128 encoding where each byte contains 7 bits of data
   * and the MSB indicates whether more bytes follow (1) or this is the last byte (0).
   *
   * @param buffer the buffer to read from
   * @return the decoded unsigned integer value
   */
  private int readUnsignedVarInt(ByteBuffer buffer) {
    int value = 0;
    int shift = 0;

    while (buffer.hasRemaining()) {
      int b = buffer.get() & 0xFF;
      value |= (b & 0x7F) << shift;

      if ((b & 0x80) == 0) {
        break;
      }

      shift += 7;
    }

    return value;
  }

  /**
   * Calculates the minimum bit width needed to represent values from 0 to maxValue-1.
   * <p>
   * This is used for determining how many bits are needed for RLE encoding and
   * dictionary indices.
   *
   * @param maxValue the exclusive upper bound (values range from 0 to maxValue-1)
   * @return the minimum number of bits required
   */
  private static int bitWidth(int maxValue) {
    if (maxValue <= 1) {
      return 0;
    }
    if (maxValue <= 2) {
      return 1;
    }
    if (maxValue <= 4) {
      return 2;
    }
    if (maxValue <= 8) {
      return 3;
    }
    if (maxValue <= 16) {
      return 4;
    }
    if (maxValue <= 32) {
      return 5;
    }
    if (maxValue <= 64) {
      return 6;
    }
    if (maxValue <= 128) {
      return 7;
    }
    if (maxValue <= 256) {
      return 8;
    }

    // For larger values, calculate using bit operations
    return 32 - Integer.numberOfLeadingZeros(maxValue - 1);
  }

  /**
   * Reads dictionary indices from a Data Page V1.
   * <p>
   * Format: [1-byte bit-width] [RLE/bit-packed hybrid data]
   * <p>
   * The bit-width indicates how many bits are needed to represent dictionary indices.
   * The RLE data follows using the RLE/bit-packed hybrid encoding scheme.
   *
   * @param buffer the buffer to read from
   * @param dictionarySize the size of the dictionary (for validation)
   * @param numValues the number of indices to decode
   * @return array of dictionary indices
   * @throws ParquetException if an index is out of bounds
   */
  private int[] readDictionaryIndices(ByteBuffer buffer, int dictionarySize, int numValues) {
    // Dictionary indices in Data Page V1 use RLE/bit-packed hybrid encoding:
    // [1-byte bit-width] [RLE data]
    // There is NO 4-byte length prefix for dictionary indices

    // Special case: if buffer is completely empty, this means all values are null
    // (definition levels indicated all nulls, no actual data to decode)
    if (buffer.remaining() == 0) {
      // Return empty array to signal all nulls
      // TODO: Properly implement definition level handling for Data Page V1
      return new int[0];
    }

    // Read the bit-width byte
    int bitWidth = buffer.get() & 0xFF;

    // The rest of the buffer contains RLE-encoded dictionary indices
    int rleDataLength = buffer.remaining();

    // If there's no RLE data after the bit-width, all values are null
    if (rleDataLength == 0) {
      return new int[0];
    }

    // Create a limited view of the buffer for the RLE decoder
    ByteBuffer rleData = buffer.slice();
    rleData.limit(rleDataLength);
    rleData.order(ByteOrder.LITTLE_ENDIAN);

    RleDecoder decoder = new RleDecoder(rleData, bitWidth, numValues);
    int[] indices = decoder.readAll();

    // Advance the original buffer past the RLE data
    buffer.position(buffer.position() + rleDataLength);

    // Validate indices
    for (int index : indices) {
      if (index >= dictionarySize) {
        throw new ParquetException(
            "Dictionary index out of bounds: " + index + " >= " + dictionarySize);
      }
    }
    return indices;
  }

  /**
   * Reads dictionary indices from a data page (legacy version for flat columns).
   *
   * @param buffer the buffer to read from
   * @param dictionarySize the size of the dictionary (for validation)
   * @param dataPage the data page containing the number of values
   * @return array of dictionary indices
   */
  private int[] readDictionaryIndices(ByteBuffer buffer, int dictionarySize,
                                      Page.DataPage dataPage) {
    return readDictionaryIndices(buffer, dictionarySize, dataPage.numValues());
  }

  /**
   * Reads dictionary indices from a Data Page V2 (wrapper using page's value count).
   *
   * @param buffer the buffer to read from
   * @param dictionarySize the size of the dictionary (for validation)
   * @param dataPageV2 the Data Page V2 containing the number of values
   * @return array of dictionary indices
   */
  private int[] readDictionaryIndicesV2(ByteBuffer buffer, int dictionarySize,
                                        Page.DataPageV2 dataPageV2) {
    return readDictionaryIndicesV2(buffer, dictionarySize, dataPageV2, dataPageV2.numValues());
  }

  /**
   * Reads dictionary indices from a Data Page V2.
   * <p>
   * Format: [1-byte bit-width] [RLE/bit-packed hybrid data]
   * <p>
   * Similar to V1 but without a 4-byte length prefix.
   *
   * @param buffer the buffer to read from
   * @param dictionarySize the size of the dictionary (for validation)
   * @param dataPageV2 the Data Page V2 (for encoding verification)
   * @param count the number of indices to decode
   * @return array of dictionary indices
   * @throws ParquetException if encoding is unsupported or indices are out of bounds
   */
  private int[] readDictionaryIndicesV2(ByteBuffer buffer, int dictionarySize,
                                        Page.DataPageV2 dataPageV2, int count) {
    // Data Page V2 uses RLE_DICTIONARY encoding for dictionary indices
    if (dataPageV2.encoding() == Encoding.PLAIN_DICTIONARY ||
        dataPageV2.encoding() == Encoding.RLE_DICTIONARY) {
      // Data Page V2 format for dictionary indices:
      // The entire buffer contains RLE/bit-packed hybrid data
      // Format: [1-byte bit-width] [RLE data]

      // Read bit-width byte
      int bitWidth = buffer.get() & 0xFF;

      // The remaining buffer contains RLE-encoded indices
      RleDecoder decoder = new RleDecoder(buffer, bitWidth, count);
      int[] indices = decoder.readAll();

      // Validate indices (skip if dictionary is empty - all values are NULL)
      if (dictionarySize > 0) {
        for (int index : indices) {
          if (index >= dictionarySize) {
            throw new ParquetException(
                "Dictionary index out of bounds: " + index + " >= " + dictionarySize);
          }
        }
      }
      return indices;
    } else {
      throw new ParquetException(
          "Unsupported dictionary encoding for V2: " + dataPageV2.encoding());
    }
  }

  /**
   * Decode column as a list of lists.
   * This handles the LIST logical type in Parquet, which uses repetition and definition levels
   * to encode the list structure.
   *
   * @param <T>            The type of list elements
   * @param elementDecoder A function that decodes primitive values to the element type
   * @return A list of lists, where null represents a null list
   */
  public <T> List<List<T>> decodeAsList(java.util.function.Function<Object, T> elementDecoder) {
    List<List<T>> result = new ArrayList<>();

    for (Page page : pages) {
      if (page instanceof Page.DataPage dataPage) {
        decodeListFromDataPage(dataPage, elementDecoder, result);
      } else if (page instanceof Page.DataPageV2 dataPageV2) {
        decodeListFromDataPageV2(dataPageV2, elementDecoder, result);
      }
    }

    return result;
  }

  /**
   * Decode lists from a Data Page V1
   */
  private <T> void decodeListFromDataPage(Page.DataPage dataPage,
                                          java.util.function.Function<Object, T> elementDecoder,
                                          List<List<T>> result) {
    ByteBuffer buffer = dataPage.data().duplicate();
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // Read repetition levels
    int[] repLevels = null;
    if (dataPage.repetitionLevelByteLen() > 0) {
      int maxRepLevel = getMaxRepetitionLevel();
      repLevels =
          readLevels(buffer, dataPage.repetitionLevelByteLen(), dataPage.numValues(), maxRepLevel);
    } else {
      // No repetition levels - all values at rep level 0
      repLevels = new int[dataPage.numValues()];
    }

    // Read definition levels
    int[] defLevels = null;
    int maxDefLevel = getMaxDefinitionLevel();
    if (dataPage.definitionLevelByteLen() > 0) {
      defLevels =
          readLevels(buffer, dataPage.definitionLevelByteLen(), dataPage.numValues(), maxDefLevel);
    } else {
      // No definition levels - all values are defined
      defLevels = new int[dataPage.numValues()];
      java.util.Arrays.fill(defLevels, maxDefLevel);
    }

    // Find the dictionary page (if any) for this column
    Object[] dictionary = null;
    for (Page page : pages) {
      if (page instanceof Page.DictionaryPage dictPage) {
        dictionary = buildDictionary(dictPage);
        break;
      }
    }

    // Read the actual primitive values
    List<Object> primitiveValues = readPrimitiveValues(buffer, dataPage, dictionary, defLevels);

    // Reconstruct lists from levels and values
    reconstructLists(repLevels, defLevels, primitiveValues, elementDecoder, result);
  }

  /**
   * Decode lists from a Data Page V2
   */
  private <T> void decodeListFromDataPageV2(Page.DataPageV2 dataPageV2,
                                            java.util.function.Function<Object, T> elementDecoder,
                                            List<List<T>> result) {
    // TODO: Implement Data Page V2 list decoding
    throw new ParquetException("Data Page V2 list decoding not yet implemented");
  }

  /**
   * Reads repetition or definition levels from a Data Page V1 buffer.
   * <p>
   * Format: [4-byte little-endian length] [RLE/bit-packed hybrid data]
   * <p>
   * Levels are RLE-encoded with a bit-width calculated from maxLevel.
   *
   * @param buffer the buffer to read from (position will be advanced)
   * @param levelByteLen the total byte length of the level data (including 4-byte prefix)
   * @param numValues the number of level values to decode
   * @param maxLevel the maximum level value (determines bit-width)
   * @return array of level values
   */
  private int[] readLevels(ByteBuffer buffer, int levelByteLen, int numValues, int maxLevel) {
    // Levels are RLE-encoded
    // First 4 bytes are the length
    int savedPos = buffer.position();
    int length = buffer.getInt();

    // Calculate bit width based on max level value
    // We need enough bits to represent values from 0 to maxLevel
    int bitWidth = bitWidth(maxLevel + 1);  // +1 because we need to represent maxLevel inclusively

    // The length tells us exactly how many bytes the RLE data is
    // Create a slice of the buffer limited to those bytes
    int dataStart = buffer.position();
    ByteBuffer levelData = buffer.slice();
    levelData.limit(length);
    levelData.order(ByteOrder.LITTLE_ENDIAN);

    RleDecoder decoder = new RleDecoder(levelData, bitWidth, numValues);
    int[] levels = decoder.readAll();

    // Move buffer position past the level data (4 byte length + actual RLE data length)
    buffer.position(savedPos + 4 + length);

    return levels;
  }

  /**
   * Reads levels from a Data Page V2 format buffer.
   * <p>
   * Format: RLE data directly (no bit-width prefix, no 4-byte length prefix)
   * <p>
   * Bit-width is calculated from maxLevel. If buffer is empty/null, all values
   * are assumed to be at max level (i.e., all non-null).
   *
   * @param buffer the buffer to read from (may be null or empty)
   * @param numValues the number of level values expected
   * @param maxLevel the maximum level value (determines bit-width)
   * @return array of level values
   */
  private int[] readLevelsV2(ByteBuffer buffer, int numValues, int maxLevel) {
    if (buffer == null || !buffer.hasRemaining()) {
      // No levels means all values are at max level (non-null)
      int[] levels = new int[numValues];
      java.util.Arrays.fill(levels, maxLevel);
      return levels;
    }

    // Duplicate the buffer to avoid modifying the original position
    ByteBuffer levelData = buffer.duplicate();
    levelData.order(ByteOrder.LITTLE_ENDIAN);

    // Calculate bit-width from max level (no bit-width byte in Data Page V2)
    int bitWidth = bitWidth(maxLevel + 1);

    // If bit-width is 0, all values are 0
    if (bitWidth == 0) {
      return new int[numValues];
    }

    // Create a decoder for the buffer
    RleDecoder decoder = new RleDecoder(levelData, bitWidth, numValues);
    return decoder.readAll();
  }

  /**
   * Reads primitive values from buffer based on page encoding and type.
   * <p>
   * This version is for nested structures where levels have already been read.
   * Only non-null values (those with definition level at max) are stored in the buffer.
   *
   * @param buffer the buffer to read from (positioned after level data)
   * @param dataPage the data page containing encoding information
   * @param dictionary optional dictionary for dictionary-encoded data (may be null)
   * @param defLevels definition levels array to determine null positions
   * @return list of decoded primitive values (only non-null values)
   */
  private List<Object> readPrimitiveValues(ByteBuffer buffer, Page.DataPage dataPage,
                                           Object[] dictionary, int[] defLevels) {
    List<Object> values = new ArrayList<>();

    // Count how many non-null values there are
    // Only non-null values are stored in the data buffer
    int numNonNullValues = dataPage.numValues();
    if (defLevels != null) {
      // Count values that have definition level at max (non-null)
      int maxDefLevel = getMaxDefinitionLevel();
      numNonNullValues = 0;
      for (int defLevel : defLevels) {
        if (defLevel >= maxDefLevel) {
          numNonNullValues++;
        }
      }
    }

    // Skip to value data (already positioned after levels)
    if (dataPage.encoding() == Encoding.PLAIN) {
      // Read PLAIN encoded values
      // Special handling for booleans which are bit-packed
      if (type == Type.BOOLEAN) {
        boolean[] boolValues = BitPackedReader.readBooleans(buffer, numNonNullValues);
        for (boolean b : boolValues) {
          values.add(b);
        }
      } else {
        for (int i = 0; i < numNonNullValues; i++) {
          Object value = readPlainValue(buffer);
          values.add(value);
        }
      }
    } else if (dataPage.encoding() == Encoding.PLAIN_DICTIONARY ||
        dataPage.encoding() == Encoding.RLE_DICTIONARY) {
      // Read dictionary indices and lookup values
      if (dictionary == null) {
        throw new ParquetException("Dictionary page not found for dictionary-encoded data");
      }

      // Dictionary indices use the same format for both flat and nested columns
      // Only decode the number of non-null values
      int[] indices = readDictionaryIndices(buffer, dictionary.length, numNonNullValues);
      for (int index : indices) {
        values.add(dictionary[index]);
      }
    } else {
      throw new ParquetException("Unsupported encoding for lists: " + dataPage.encoding());
    }

    return values;
  }

  /**
   * Reads dictionary indices without a 4-byte length prefix.
   * <p>
   * Used for nested structures where the RLE data is stored directly without
   * a length prefix. Bit-width is calculated from dictionary size.
   *
   * @param buffer the buffer to read from
   * @param dictionarySize the size of the dictionary (determines bit-width)
   * @param numValues the number of indices to decode
   * @return array of dictionary indices
   * @throws ParquetException if an index is out of bounds
   */
  private int[] readDictionaryIndicesWithoutLength(ByteBuffer buffer, int dictionarySize,
                                                   int numValues) {
    // Calculate bit-width from dictionary size
    int bitWidth = bitWidth(dictionarySize);

    System.out.println("[DEBUG DICT] Buffer position: " + buffer.position());
    System.out.println("[DEBUG DICT] Buffer remaining: " + buffer.remaining());
    System.out.println(
        "[DEBUG DICT] bitWidth: " + bitWidth + ", dictionarySize: " + dictionarySize);

    // Read the first 4 bytes to see what they are
    int savedPos = buffer.position();
    if (buffer.remaining() >= 4) {
      int firstInt = buffer.getInt();
      System.out.println("[DEBUG DICT] First 4 bytes as int: " + firstInt);
      buffer.position(savedPos);
    }

    RleDecoder decoder = new RleDecoder(buffer, bitWidth, numValues);
    int[] indices = decoder.readAll();

    System.out.println("[DEBUG] Dictionary indices: " + java.util.Arrays.toString(indices));

    // Validate indices
    for (int index : indices) {
      if (index >= dictionarySize) {
        throw new ParquetException(
            "Dictionary index out of bounds: " + index + " >= " + dictionarySize);
      }
    }

    return indices;
  }

  /**
   * Builds a dictionary array from a DictionaryPage.
   * <p>
   * Dictionary pages contain PLAIN-encoded values that can be referenced by
   * indices in dictionary-encoded data pages.
   *
   * @param dictPage the dictionary page to decode
   * @return array of decoded dictionary values
   */
  private Object[] buildDictionary(Page.DictionaryPage dictPage) {
    ByteBuffer buffer = dictPage.data().duplicate();
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    Object[] dictionary = new Object[dictPage.numValues()];

    // Dictionary is always PLAIN encoded
    for (int i = 0; i < dictPage.numValues(); i++) {
      dictionary[i] = readPlainValue(buffer);
    }

    return dictionary;
  }

  /**
   * Reads a single PLAIN-encoded value from the buffer based on column type.
   * <p>
   * PLAIN encoding stores values in their native binary format:
   * <ul>
   *   <li>INT32/INT64/FLOAT/DOUBLE: fixed-width binary values</li>
   *   <li>BYTE_ARRAY: 4-byte length prefix + data bytes</li>
   *   <li>BOOLEAN: single bit (but typically bit-packed separately)</li>
   * </ul>
   *
   * @param buffer the buffer to read from
   * @return the decoded value as an Object
   * @throws ParquetException if the type is unsupported
   */
  private Object readPlainValue(ByteBuffer buffer) {
    switch (type) {
      case INT32:
        return buffer.getInt();
      case INT64:
        return buffer.getLong();
      case FLOAT:
        return buffer.getFloat();
      case DOUBLE:
        return buffer.getDouble();
      case BYTE_ARRAY:
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
      case BOOLEAN:
        return (buffer.get() & 1) != 0;
      default:
        throw new ParquetException("Unsupported type for list elements: " + type);
    }
  }

  /**
   * Reconstructs lists from repetition levels, definition levels, and primitive values.
   * <p>
   * List structure encoding in Parquet:
   * <ul>
   *   <li>repLevel = 0: Start of a new list</li>
   *   <li>repLevel = 1: Additional element in current list</li>
   *   <li>defLevel = 0: Null list</li>
   *   <li>defLevel = 1: Empty list</li>
   *   <li>defLevel >= maxDefLevel: Non-null element</li>
   * </ul>
   *
   * @param repLevels repetition levels indicating list boundaries
   * @param defLevels definition levels indicating null values
   * @param primitiveValues decoded non-null primitive values
   * @param elementDecoder function to convert primitive values to element type
   * @param result output list to append decoded lists to
   * @param <T> the element type
   */
  private <T> void reconstructLists(int[] repLevels, int[] defLevels,
                                    List<Object> primitiveValues,
                                    java.util.function.Function<Object, T> elementDecoder,
                                    List<List<T>> result) {
    int valueIndex = 0;
    List<T> currentList = null;

    for (int i = 0; i < repLevels.length; i++) {
      int repLevel = repLevels[i];
      int defLevel = defLevels[i];

      if (repLevel == 0) {
        // Start of a new list
        if (currentList != null) {
          result.add(currentList);
        }

        if (defLevel == 0) {
          // Null list
          currentList = null;
          result.add(null);
          currentList = null; // Reset for next
        } else if (defLevel == 1) {
          // Empty list
          currentList = new ArrayList<>();
        } else {
          // List with elements
          currentList = new ArrayList<>();
          // This first element belongs to the new list
          if (defLevel >= getMaxDefinitionLevel()) {
            // Non-null element
            T element = elementDecoder.apply(primitiveValues.get(valueIndex++));
            currentList.add(element);
          } else {
            // Null element (no value in primitiveValues for nulls)
            currentList.add(null);
          }
        }
      } else {
        // Continuation of current list (repLevel == 1)
        if (currentList != null) {
          if (defLevel >= getMaxDefinitionLevel()) {
            // Non-null element
            T element = elementDecoder.apply(primitiveValues.get(valueIndex++));
            currentList.add(element);
          } else {
            // Null element (no value in primitiveValues for nulls)
            currentList.add(null);
          }
        }
      }
    }

    // Add the last list
    if (currentList != null) {
      result.add(currentList);
    }
  }

  /**
   * Decode column as a list of maps.
   * This handles the MAP logical type in Parquet, which uses repetition and definition levels
   * to encode the map structure.
   *
   * @param <K>          The type of map keys
   * @param <V>          The type of map values
   * @param keyDecoder   A function that decodes primitive values to the key type
   * @param valueDecoder A function that decodes primitive values to the value type
   * @return A list of maps, where null represents a null map
   */
  public <K, V> List<java.util.Map<K, V>> decodeAsMap(
      java.util.function.Function<Object, K> keyDecoder,
      java.util.function.Function<Object, V> valueDecoder) {

    List<java.util.Map<K, V>> result = new ArrayList<>();

    for (Page page : pages) {
      if (page instanceof Page.DataPage dataPage) {
        decodeMapFromDataPage(dataPage, keyDecoder, valueDecoder, result);
      } else if (page instanceof Page.DataPageV2 dataPageV2) {
        decodeMapFromDataPageV2(dataPageV2, keyDecoder, valueDecoder, result);
      }
    }

    return result;
  }

  /**
   * Decode maps from a Data Page V1
   */
  private <K, V> void decodeMapFromDataPage(Page.DataPage dataPage,
                                            java.util.function.Function<Object, K> keyDecoder,
                                            java.util.function.Function<Object, V> valueDecoder,
                                            List<java.util.Map<K, V>> result) {
    ByteBuffer buffer = dataPage.data().duplicate();
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // Read repetition levels
    int[] repLevels = null;
    if (dataPage.repetitionLevelByteLen() > 0) {
      int maxRepLevel = getMaxRepetitionLevel();
      repLevels =
          readLevels(buffer, dataPage.repetitionLevelByteLen(), dataPage.numValues(), maxRepLevel);
    } else {
      repLevels = new int[dataPage.numValues()];
    }

    // Read definition levels
    int[] defLevels = null;
    int maxDefLevel = getMaxDefinitionLevel();
    if (dataPage.definitionLevelByteLen() > 0) {
      defLevels =
          readLevels(buffer, dataPage.definitionLevelByteLen(), dataPage.numValues(), maxDefLevel);
    } else {
      defLevels = new int[dataPage.numValues()];
      java.util.Arrays.fill(defLevels, maxDefLevel);
    }

    // Find the dictionary page (if any) for this column
    Object[] dictionary = null;
    for (Page page : pages) {
      if (page instanceof Page.DictionaryPage dictPage) {
        dictionary = buildDictionary(dictPage);
        break;
      }
    }

    // Read the actual primitive values (these are the keys or values depending on column)
    List<Object> primitiveValues = readPrimitiveValues(buffer, dataPage, dictionary, defLevels);

    // For MAP, we need to handle key-value pairs
    // This is a simplified version - in reality, keys and values are in separate columns
    reconstructMaps(repLevels, defLevels, primitiveValues, keyDecoder, valueDecoder, result);
  }

  /**
   * Decode maps from a Data Page V2
   */
  private <K, V> void decodeMapFromDataPageV2(Page.DataPageV2 dataPageV2,
                                              java.util.function.Function<Object, K> keyDecoder,
                                              java.util.function.Function<Object, V> valueDecoder,
                                              List<java.util.Map<K, V>> result) {
    throw new ParquetException("Data Page V2 map decoding not yet implemented");
  }

  /**
   * Reconstructs maps from repetition levels, definition levels, and primitive values.
   * <p>
   * <strong>Note:</strong> This is a simplified placeholder implementation. Full MAP support
   * requires reading both key and value columns separately using
   * {@link #decodeMapFromKeyValueColumns}.
   *
   * @param repLevels repetition levels indicating map boundaries
   * @param defLevels definition levels indicating null values
   * @param primitiveValues decoded primitive values
   * @param keyDecoder function to convert primitive values to key type
   * @param valueDecoder function to convert primitive values to value type
   * @param result output list to append decoded maps to
   * @param <K> the key type
   * @param <V> the value type
   */
  private <K, V> void reconstructMaps(int[] repLevels, int[] defLevels,
                                      List<Object> primitiveValues,
                                      java.util.function.Function<Object, K> keyDecoder,
                                      java.util.function.Function<Object, V> valueDecoder,
                                      List<java.util.Map<K, V>> result) {
    int valueIndex = 0;
    java.util.Map<K, V> currentMap = null;
    boolean isKey = true; // Alternate between key and value
    int maxDefLevel = getMaxDefinitionLevel();

    for (int i = 0; i < repLevels.length; i++) {
      int repLevel = repLevels[i];
      int defLevel = defLevels[i];

      if (repLevel == 0) {
        // Start of a new map
        if (currentMap != null) {
          result.add(currentMap);
        }

        if (defLevel == 0) {
          // Null map
          result.add(null);
          currentMap = null;
        } else if (defLevel == 1) {
          // Empty map
          currentMap = new java.util.LinkedHashMap<>();
        } else {
          // Map with entries
          currentMap = new java.util.LinkedHashMap<>();
          // Note: This simplified implementation assumes alternating key-value pattern
          // Real implementation should read separate columns for keys and values
        }
      }

      // For proper MAP support, we need to read key and value from separate columns
      // This is a placeholder implementation
      if (currentMap != null && defLevel >= maxDefLevel) {
        // This is simplified - proper implementation needs column metadata
        if (valueIndex < primitiveValues.size()) {
          valueIndex++;
        }
      }
    }

    // Add the last map
    if (currentMap != null) {
      result.add(currentMap);
    }
  }

  /**
   * Reconstruct maps from separate key and value columns using definition and repetition levels.
   * <p>
   * This is the proper way to decode MAP columns in Parquet, which stores keys and values
   * in two separate physical columns with shared repetition and definition levels.
   * <p>
   * Definition level meanings:
   * - For keys (max def level = 2):
   *   0 = Map is NULL
   *   1 = Map is empty
   *   2 = Key is present (keys are always required)
   * <p>
   * - For values (max def level = 3 if values are optional, 2 if required):
   *   0 = Map is NULL
   *   1 = Map is empty
   *   2 = Entry exists but value is NULL (only if values are optional)
   *   3 = Value is present (if values are optional)
   *   OR 2 = Value is present (if values are required)
   * <p>
   * Repetition level meanings:
   * - 0 = First entry of a new map (or NULL/empty map marker)
   * - 1 = Additional entry in the same map
   *
   * @param keyColumn    The ColumnValues for keys
   * @param valueColumn  The ColumnValues for values
   * @param keyDecoder   Function to decode key values
   * @param valueDecoder Function to decode value values
   * @param <K>          Key type
   * @param <V>          Value type
   * @return List of Maps reconstructed from the columns
   */
  public static <K, V> List<java.util.Map<K, V>> decodeMapFromKeyValueColumns(
      ColumnValues keyColumn,
      ColumnValues valueColumn,
      java.util.function.Function<Object, K> keyDecoder,
      java.util.function.Function<Object, V> valueDecoder) {

    List<java.util.Map<K, V>> result = new ArrayList<>();

    // Process each page pair
    for (int pageIdx = 0; pageIdx < keyColumn.pages.size(); pageIdx++) {
      Page keyPage = keyColumn.pages.get(pageIdx);
      Page valuePage = valueColumn.pages.get(pageIdx);

      // Skip dictionary pages
      if (keyPage instanceof Page.DictionaryPage || valuePage instanceof Page.DictionaryPage) {
        continue;
      }

      if (keyPage instanceof Page.DataPage keyDataPage &&
          valuePage instanceof Page.DataPage valueDataPage) {
        decodeMapFromDataPages(keyColumn, valueColumn, keyDataPage, valueDataPage,
            keyDecoder, valueDecoder, result);
      } else if (keyPage instanceof Page.DataPageV2 keyDataPageV2 &&
          valuePage instanceof Page.DataPageV2 valueDataPageV2) {
        decodeMapFromDataPagesV2(keyColumn, valueColumn, keyDataPageV2, valueDataPageV2,
            keyDecoder, valueDecoder, result);
      }
    }

    return result;
  }

  /**
   * Decode maps from Data Page V1 key and value pages
   */
  private static <K, V> void decodeMapFromDataPages(
      ColumnValues keyColumn,
      ColumnValues valueColumn,
      Page.DataPage keyDataPage,
      Page.DataPage valueDataPage,
      java.util.function.Function<Object, K> keyDecoder,
      java.util.function.Function<Object, V> valueDecoder,
      List<java.util.Map<K, V>> result) {

    // Read key column data
    ByteBuffer keyBuffer = keyDataPage.data().duplicate();
    keyBuffer.order(ByteOrder.LITTLE_ENDIAN);

    // Read repetition levels from key column (keys and values share the same rep levels)
    int[] repLevels = null;
    int maxRepLevel = keyColumn.columnDescriptor.maxRepetitionLevel();
    if (keyDataPage.repetitionLevelByteLen() > 0) {
      repLevels = keyColumn.readLevels(keyBuffer, keyDataPage.repetitionLevelByteLen(),
          keyDataPage.numValues(), maxRepLevel);
    } else {
      repLevels = new int[keyDataPage.numValues()];
    }

    // Read definition levels from key column
    int[] keyDefLevels = null;
    int keyMaxDefLevel = keyColumn.columnDescriptor.maxDefinitionLevel();
    if (keyDataPage.definitionLevelByteLen() > 0) {
      keyDefLevels = keyColumn.readLevels(keyBuffer, keyDataPage.definitionLevelByteLen(),
          keyDataPage.numValues(), keyMaxDefLevel);
    } else {
      keyDefLevels = new int[keyDataPage.numValues()];
      java.util.Arrays.fill(keyDefLevels, keyMaxDefLevel);
    }

    // Read value column data
    ByteBuffer valueBuffer = valueDataPage.data().duplicate();
    valueBuffer.order(ByteOrder.LITTLE_ENDIAN);

    // Skip repetition levels in value column (we already have them from key column)
    if (valueDataPage.repetitionLevelByteLen() > 0) {
      valueBuffer.position(valueBuffer.position() + valueDataPage.repetitionLevelByteLen());
    }

    // Read definition levels from value column
    int[] valueDefLevels = null;
    int valueMaxDefLevel = valueColumn.columnDescriptor.maxDefinitionLevel();
    if (valueDataPage.definitionLevelByteLen() > 0) {
      valueDefLevels = valueColumn.readLevels(valueBuffer, valueDataPage.definitionLevelByteLen(),
          valueDataPage.numValues(), valueMaxDefLevel);
    } else {
      valueDefLevels = new int[valueDataPage.numValues()];
      java.util.Arrays.fill(valueDefLevels, valueMaxDefLevel);
    }

    // Find dictionaries for both columns
    Object[] keyDictionary = findDictionary(keyColumn);
    Object[] valueDictionary = findDictionary(valueColumn);

    // Read primitive key values
    List<Object> keyValues = keyColumn.readPrimitiveValues(keyBuffer, keyDataPage,
        keyDictionary, keyDefLevels);

    // Read primitive value values
    List<Object> valueValues = valueColumn.readPrimitiveValues(valueBuffer, valueDataPage,
        valueDictionary, valueDefLevels);

    // Reconstruct maps using levels and primitive values
    reconstructMapsFromColumns(repLevels, keyDefLevels, valueDefLevels, keyValues, valueValues,
        keyDecoder, valueDecoder, keyMaxDefLevel, valueMaxDefLevel, result);
  }

  /**
   * Decode maps from Data Page V2 key and value pages
   */
  private static <K, V> void decodeMapFromDataPagesV2(
      ColumnValues keyColumn,
      ColumnValues valueColumn,
      Page.DataPageV2 keyDataPageV2,
      Page.DataPageV2 valueDataPageV2,
      java.util.function.Function<Object, K> keyDecoder,
      java.util.function.Function<Object, V> valueDecoder,
      List<java.util.Map<K, V>> result) {

    // Read repetition levels from key column (shared with value column)
    int maxRepLevel = keyColumn.columnDescriptor.maxRepetitionLevel();
    ByteBuffer keyRepBuffer = keyDataPageV2.repetitionLevels();
    int[] repLevels = keyColumn.readLevelsV2(keyRepBuffer, keyDataPageV2.numValues(), maxRepLevel);

    // Read definition levels from key column
    int keyMaxDefLevel = keyColumn.columnDescriptor.maxDefinitionLevel();
    ByteBuffer keyDefBuffer = keyDataPageV2.definitionLevels();
    int[] keyDefLevels = keyColumn.readLevelsV2(keyDefBuffer, keyDataPageV2.numValues(),
        keyMaxDefLevel);

    // Read definition levels from value column
    int valueMaxDefLevel = valueColumn.columnDescriptor.maxDefinitionLevel();
    ByteBuffer valueDefBuffer = valueDataPageV2.definitionLevels();
    int[] valueDefLevels = valueColumn.readLevelsV2(valueDefBuffer, valueDataPageV2.numValues(),
        valueMaxDefLevel);

    // Find dictionaries for both columns
    Object[] keyDictionary = findDictionary(keyColumn);
    Object[] valueDictionary = findDictionary(valueColumn);

    // Read primitive key values
    ByteBuffer keyBuffer = keyDataPageV2.data().duplicate();
    keyBuffer.order(ByteOrder.LITTLE_ENDIAN);
    List<Object> keyValues = keyColumn.readPrimitiveValues(keyBuffer, null,
        keyDictionary, keyDefLevels, keyDataPageV2.encoding());

    // Read primitive value values
    ByteBuffer valueBuffer = valueDataPageV2.data().duplicate();
    valueBuffer.order(ByteOrder.LITTLE_ENDIAN);
    List<Object> valueValues = valueColumn.readPrimitiveValues(valueBuffer, null,
        valueDictionary, valueDefLevels, valueDataPageV2.encoding());

    // Reconstruct maps using levels and primitive values
    reconstructMapsFromColumns(repLevels, keyDefLevels, valueDefLevels, keyValues, valueValues,
        keyDecoder, valueDecoder, keyMaxDefLevel, valueMaxDefLevel, result);
  }

  /**
   * Finds the dictionary page for a column.
   *
   * @param column the column to search for a dictionary page
   * @return array of dictionary values, or null if no dictionary page found
   */
  private static Object[] findDictionary(ColumnValues column) {
    for (Page page : column.pages) {
      if (page instanceof Page.DictionaryPage dictPage) {
        return column.buildDictionary(dictPage);
      }
    }
    return null;
  }

  /**
   * Reconstruct maps from repetition levels, definition levels, and primitive values
   * from BOTH key and value columns.
   * <p>
   * This properly handles the two-column MAP structure in Parquet.
   */
  private static <K, V> void reconstructMapsFromColumns(
      int[] repLevels,
      int[] keyDefLevels,
      int[] valueDefLevels,
      List<Object> keyValues,
      List<Object> valueValues,
      java.util.function.Function<Object, K> keyDecoder,
      java.util.function.Function<Object, V> valueDecoder,
      int keyMaxDefLevel,
      int valueMaxDefLevel,
      List<java.util.Map<K, V>> result) {

    int keyIndex = 0;
    int valueIndex = 0;
    java.util.Map<K, V> currentMap = null;

    for (int i = 0; i < repLevels.length; i++) {
      int repLevel = repLevels[i];
      int keyDefLevel = keyDefLevels[i];
      int valueDefLevel = valueDefLevels[i];

      if (repLevel == 0) {
        // Start of a new map
        if (currentMap != null) {
          result.add(currentMap);
        }

        // Check if map is NULL or empty
        if (keyDefLevel == 0) {
          // NULL map
          result.add(null);
          currentMap = null;
        } else if (keyDefLevel == 1) {
          // Empty map
          currentMap = new java.util.LinkedHashMap<>();
        } else {
          // Map with at least one entry
          currentMap = new java.util.LinkedHashMap<>();

          // Add the first key-value pair
          if (keyDefLevel >= keyMaxDefLevel) {
            K key = keyDecoder.apply(keyValues.get(keyIndex++));

            // Check if value is NULL or present
            V value = null;
            if (valueDefLevel >= valueMaxDefLevel) {
              value = valueDecoder.apply(valueValues.get(valueIndex++));
            } else if (valueDefLevel == valueMaxDefLevel - 1) {
              // Value is NULL (entry exists but value is null)
              // Don't increment valueIndex - null values aren't stored
            }

            currentMap.put(key, value);
          }
        }
      } else if (repLevel == 1 && currentMap != null) {
        // Additional entry in current map
        if (keyDefLevel >= keyMaxDefLevel) {
          K key = keyDecoder.apply(keyValues.get(keyIndex++));

          // Check if value is NULL or present
          V value = null;
          if (valueDefLevel >= valueMaxDefLevel) {
            value = valueDecoder.apply(valueValues.get(valueIndex++));
          } else if (valueDefLevel == valueMaxDefLevel - 1) {
            // Value is NULL (entry exists but value is null)
            // Don't increment valueIndex - null values aren't stored
          }

          currentMap.put(key, value);
        }
      }
    }

    // Add the last map
    if (currentMap != null) {
      result.add(currentMap);
    }
  }

  /**
   * Reads primitive values from a Data Page V2 buffer.
   * <p>
   * This is an overloaded version that accepts an explicit encoding parameter
   * for Data Page V2 decoding. Handles PLAIN and dictionary encodings.
   *
   * @param buffer the buffer to read from
   * @param dataPage the data page (null for Data Page V2)
   * @param dictionary optional dictionary array (may be null)
   * @param defLevels definition levels to determine null positions
   * @param encoding the encoding to use for decoding
   * @return list of decoded primitive values (only non-null values)
   */
  private List<Object> readPrimitiveValues(ByteBuffer buffer, Page.DataPage dataPage,
                                           Object[] dictionary, int[] defLevels,
                                           Encoding encoding) {
    // Use existing readPrimitiveValues but with explicit encoding parameter
    // For now, wrap the DataPage V2 data in a DataPage-like structure
    if (dataPage == null) {
      // Data Page V2 - create a temporary DataPage for compatibility
      int numNonNull = 0;
      int maxDefLevel = getMaxDefinitionLevel();
      for (int defLevel : defLevels) {
        if (defLevel >= maxDefLevel) {
          numNonNull++;
        }
      }

      List<Object> values = new ArrayList<>();

      if (encoding == Encoding.PLAIN) {
        if (type == Type.BOOLEAN) {
          boolean[] boolValues = BitPackedReader.readBooleans(buffer, numNonNull);
          for (boolean b : boolValues) {
            values.add(b);
          }
        } else {
          for (int i = 0; i < numNonNull; i++) {
            values.add(readPlainValue(buffer));
          }
        }
      } else if (encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY) {
        if (dictionary == null) {
          throw new ParquetException("Dictionary page not found for dictionary-encoded data");
        }
        // Read bit-width and decode indices
        int bitWidth = buffer.get() & 0xFF;
        RleDecoder decoder = new RleDecoder(buffer, bitWidth, numNonNull);
        int[] indices = decoder.readAll();
        for (int index : indices) {
          values.add(dictionary[index]);
        }
      }

      return values;
    }

    // Data Page V1 - use existing method
    return readPrimitiveValues(buffer, dataPage, dictionary, defLevels);
  }

  /**
   * Returns the maximum definition level for this column.
   * <p>
   * The maximum definition level indicates the deepest level of nesting where
   * a value can be defined. For simple optional columns, this is 1.
   *
   * @return the maximum definition level
   */
  private int getMaxDefinitionLevel() {
    if (columnDescriptor != null) {
      return columnDescriptor.maxDefinitionLevel();
    }
    // Fallback for backward compatibility
    return 3;
  }

  /**
   * Returns the maximum repetition level for this column.
   * <p>
   * The maximum repetition level indicates the deepest level of nesting with
   * repeated elements (lists/maps). For flat columns, this is 0; for columns
   * inside one level of list, this is 1.
   *
   * @return the maximum repetition level
   */
  private int getMaxRepetitionLevel() {
    if (columnDescriptor != null) {
      return columnDescriptor.maxRepetitionLevel();
    }
    // Fallback for backward compatibility
    return 1;
  }
}
