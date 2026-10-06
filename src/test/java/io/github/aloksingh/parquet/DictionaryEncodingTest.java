package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for dictionary encoding support.
 *
 * <p>All assertions compare against exact golden values (pyarrow exports of
 * alltypes_dictionary.parquet). Unexpected decode failures fail the test.
 */
class DictionaryEncodingTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  @Test
  void testReadDictionaryEncodedFile() throws IOException {
    String filePath = TEST_DATA_DIR + "alltypes_dictionary.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();
      assertEquals(11, schema.getNumColumns());
      assertEquals(2, reader.getMetadata().fileMetadata().numRows());

      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);
        try {
          ColumnValues values = rowGroup.readColumn(i);
          switch (col.physicalType()) {
            case BOOLEAN -> assertEquals(
                List.of(true, false), values.decodeAsBoolean(), col.getPathString());
            case INT32 -> assertEquals(
                List.of(0, 1), values.decodeAsInt32(), col.getPathString());
            case INT64 -> assertEquals(
                List.of(0L, 10L), values.decodeAsInt64(), col.getPathString());
            case FLOAT -> assertEquals(
                List.of(0.0f, 1.1f), values.decodeAsFloat(), col.getPathString());
            case DOUBLE -> assertEquals(
                List.of(0.0, 10.1), values.decodeAsDouble(), col.getPathString());
            case BYTE_ARRAY -> assertEquals(
                switch (col.getPathString()) {
                  case "date_string_col" -> List.of("01/01/09", "01/01/09");
                  case "string_col" -> List.of("0", "1");
                  default -> fail("unexpected BYTE_ARRAY column " + col.getPathString());
                }, values.decodeAsString(), col.getPathString());
            case INT96 -> {
              // timestamp_col: 2009-01-01 00:00:00 and 00:01:00. INT96 is
              // little-endian nanos-of-day (8 bytes) + little-endian Julian day
              // (4 bytes); 2009-01-01 is Julian day 2454833.
              List<byte[]> timestamps = values.decodeAsInt96();
              assertEquals(2, timestamps.size());
              byte[] julian2454833 = {49, 117, 37, 0};
              byte[] expected0 = new byte[12];
              byte[] expected1 = new byte[12];
              // 60 seconds in nanos, little-endian
              long nanosOfDay = 60L * 1_000_000_000L;
              for (int b = 0; b < 8; b++) {
                expected1[b] = (byte) (nanosOfDay >>> (8 * b));
              }
              System.arraycopy(julian2454833, 0, expected0, 8, 4);
              System.arraycopy(julian2454833, 0, expected1, 8, 4);
              assertArrayEquals(expected0, timestamps.get(0), "timestamp_col row 0");
              assertArrayEquals(expected1, timestamps.get(1), "timestamp_col row 1");
            }
            default -> fail("Unhandled physical type " + col.physicalType()
                + " for column " + col.getPathString());
          }
        } catch (Exception e) {
          fail("Unable to parse Column " + i + ": " + col.getPathString() + " ("
              + col.physicalType() + ")", e);
        }
      }
    }
  }

  @Test
  void testRleDecoderBasic() {
    // Test basic RLE decoding
    byte[] data = new byte[] {
        0x02,  // RLE run: header = 2 (LSB=0, length = 2>>1 = 1)
        0x05   // Value = 5
    };
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(data);
    RleDecoder decoder = new RleDecoder(buffer, 8, 1);

    int[] values = decoder.readAll();
    assertEquals(1, values.length);
    assertEquals(5, values[0]);
  }

  @Test
  void testRleDecoderBitPacked() {
    // Bit-packed run per the parquet RLE/bit-packed hybrid format:
    // header = 3 (LSB=1, num_groups = 3>>1 = 1 group of 8 values), then 3 bytes
    // for bit width 3. Values are packed least-significant bit first:
    // bits of 0x01,0x02,0x03 = 10000000 01000000 11000000 (LSB-first per byte)
    // -> 3-bit values [1, 0, 0, 1, 0, 6, 0, 0]
    byte[] data = new byte[] {
        0x03,  // Bit-packed run: header = 3 (LSB=1, num_groups = 3>>1 = 1)
        0x01, 0x02, 0x03  // 3 bytes for bit width 3 (8 values)
    };
    java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(data);
    RleDecoder decoder = new RleDecoder(buffer, 3, 8);

    int[] values = decoder.readAll();
    assertArrayEquals(new int[] {1, 0, 0, 1, 0, 6, 0, 0}, values);
  }

  /**
   * Regression from the retired {@code DebugRleDictionaryTest}: the corpus file
   * with an unrecognized logical-type annotation has a RLE_DICTIONARY string leaf
   * whose dictionary page is PLAIN. This fixture has no golden JSON, so these
   * exact values are its only value pin.
   */
  @Test
  void unknownLogicalTypeDictionaryColumnDecodesExactly() throws IOException {
    try (ParquetFileReader reader = new ParquetFileReader("src/test/data/unknown-logical-type.parquet")) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Page> pages = rowGroup.getColumnPageReader(0).readAllPages();
      assertEquals(2, pages.size());
      Page.DictionaryPage dictionary = (Page.DictionaryPage) pages.get(0);
      assertEquals(3, dictionary.numValues());
      assertEquals(Encoding.PLAIN, dictionary.encoding());
      Page.DataPage dataPage = (Page.DataPage) pages.get(1);
      assertEquals(3, dataPage.numValues());
      assertEquals(Encoding.RLE_DICTIONARY, dataPage.encoding());
      ColumnValues values = new ColumnValues(io.github.aloksingh.parquet.model.Type.BYTE_ARRAY,
              pages, reader.getSchema().getColumn(0), null);
      assertEquals(List.of("known string 1", "known string 2", "known string 3"),
              values.decodeAsString());
    }
  }
}
