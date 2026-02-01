package org.parquet.model;

import java.nio.ByteBuffer;

/**
 * Represents a page in a Parquet file
 */
public sealed interface Page permits Page.DataPage, Page.DataPageV2, Page.DictionaryPage {

  /**
   * Data page containing column values (V1)
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
   * Data page V2 containing column values
   * In V2, definition and repetition levels are stored separately and are not compressed
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
   * Dictionary page containing dictionary values
   */
  record DictionaryPage(
      ByteBuffer data,
      int numValues,
      Encoding encoding
  ) implements Page {
  }
}
