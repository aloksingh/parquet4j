package org.parquet.model;

import java.nio.ByteBuffer;

/**
 * Represents a page in a Parquet file.
 * <p>
 * A page is the smallest unit of data stored in a column chunk. Parquet supports
 * multiple page types for efficient data storage and retrieval.
 */
public sealed interface Page permits Page.DataPage, Page.DataPageV2, Page.DictionaryPage {

  /**
   * Data page containing column values using Parquet format V1.
   * <p>
   * In V1 format, definition and repetition levels are compressed together with
   * the data and stored as a single byte stream.
   *
   * @param data                    the compressed data containing definition levels,
   *                                repetition levels, and values
   * @param numValues               the number of values in this page
   * @param encoding                the encoding used for the values
   * @param definitionLevelByteLen  the byte length of the definition levels section
   * @param repetitionLevelByteLen  the byte length of the repetition levels section
   */
  record DataPage(
      ByteBuffer data,
      int numValues,
      Encoding encoding,
      int definitionLevelByteLen,
      int repetitionLevelByteLen
  ) implements Page {
  }

  /**
   * Data page containing column values using Parquet format V2.
   * <p>
   * In V2 format, definition and repetition levels are stored separately from
   * the data values and are not compressed. This allows for more efficient
   * processing when only levels are needed.
   *
   * @param data             the compressed or uncompressed column values
   * @param numValues        the number of values in this page
   * @param numNulls         the number of null values in this page
   * @param numRows          the number of rows in this page
   * @param encoding         the encoding used for the values
   * @param definitionLevels the uncompressed definition levels
   * @param repetitionLevels the uncompressed repetition levels
   * @param isCompressed     whether the data values are compressed
   */
  record DataPageV2(
      ByteBuffer data,
      int numValues,
      int numNulls,
      int numRows,
      Encoding encoding,
      ByteBuffer definitionLevels,
      ByteBuffer repetitionLevels,
      boolean isCompressed
  ) implements Page {
  }

  /**
   * Dictionary page containing encoded dictionary values.
   * <p>
   * Dictionary encoding stores unique values in a dictionary page, and data pages
   * reference these values by index. This is efficient for columns with low cardinality.
   *
   * @param data      the compressed dictionary values
   * @param numValues the number of unique values in the dictionary
   * @param encoding  the encoding used for the dictionary values
   */
  record DictionaryPage(
      ByteBuffer data,
      int numValues,
      Encoding encoding
  ) implements Page {
  }
}
