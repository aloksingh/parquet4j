package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Debug test to understand RLE_DICTIONARY format
 */
public class DebugRleDictionaryTest {

  @Test
  void debugRleDictionaryFormat() throws IOException {
    String testFile = "src/test/data/unknown-logical-type.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(testFile)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Read column 0
      PageReader pageReader = rowGroup.getColumnPageReader(0);
      List<Page> pages = pageReader.readAllPages();

      System.out.println("\n=== Debugging RLE_DICTIONARY format ===");
      System.out.println("Column 0 pages: " + pages.size());

      for (int i = 0; i < pages.size(); i++) {
        Page page = pages.get(i);
        System.out.println("\nPage " + i + ": " + page.getClass().getSimpleName());

        if (page instanceof Page.DictionaryPage(
            ByteBuffer data1, int values, Encoding encoding1
        )) {
          System.out.println("  Num values: " + values);
          System.out.println("  Encoding: " + encoding1);

          // Decode dictionary as strings
          ByteBuffer dictBuffer = data1.duplicate();
          dictBuffer.order(ByteOrder.LITTLE_ENDIAN);

          System.out.println("  Dictionary entries:");
          for (int j = 0; j < values; j++) {
            int len = dictBuffer.getInt();
            byte[] bytes = new byte[len];
            dictBuffer.get(bytes);
            String value = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
            System.out.println("    [" + j + "]: \"" + value + "\"");
          }
        } else if (page instanceof Page.DataPage(
            ByteBuffer data, int numValues, Encoding encoding,
            int definitionLevelByteLen,
            int repetitionLevelByteLen
        )) {
          System.out.println("  Encoding: " + encoding);
          System.out.println("  Num values: " + numValues);
          System.out.println("  Definition level byte len: " + definitionLevelByteLen);
          System.out.println("  Repetition level byte len: " + repetitionLevelByteLen);

          ByteBuffer buffer = data.duplicate();
          buffer.order(ByteOrder.LITTLE_ENDIAN);

          // Skip levels
          buffer.position(definitionLevelByteLen + repetitionLevelByteLen);

          System.out.println("  Buffer remaining after skipping levels: " + buffer.remaining());

          // Read first 20 bytes as hex
          System.out.print("  First bytes (hex): ");
          int bytesToRead = Math.min(20, buffer.remaining());
          for (int j = 0; j < bytesToRead; j++) {
            System.out.printf("%02X ", buffer.get() & 0xFF);
          }
          System.out.println();

          // Reset and try reading as per format
          buffer.position(definitionLevelByteLen + repetitionLevelByteLen);

          int length = buffer.getInt();
          System.out.println("  Length field: " + length);
          System.out.println("  Buffer remaining after length: " + buffer.remaining());

          if (buffer.hasRemaining()) {
            // Try decoding with calculated bit-width
            buffer.position(definitionLevelByteLen + repetitionLevelByteLen);
            int lengthForDecode = buffer.getInt();
            int calcBitWidth = bitWidth(3);  // Dictionary size is 3
            System.out.println("  Calculated bit-width for dict size 3: " + calcBitWidth);

            System.out.println("  Attempting to decode with RleDecoder...");
            RleDecoder decoder = new RleDecoder(buffer, calcBitWidth, numValues);
            int[] indices = decoder.readAll();
            System.out.println("  Decoded indices: " + java.util.Arrays.toString(indices));

            // Also try decoding the rest of the buffer
            System.out.println("\n  Buffer position after decode: " + buffer.position());
            System.out.println("  Buffer remaining: " + buffer.remaining());
            if (buffer.remaining() > 0) {
              System.out.print("  Remaining bytes (hex): ");
              while (buffer.hasRemaining()) {
                System.out.printf("%02X ", buffer.get() & 0xFF);
              }
              System.out.println();
            }

            // Try decoding all remaining bytes as RLE
            System.out.println("\n  === Trying to decode all bytes after length as RLE ===");
            buffer.position(definitionLevelByteLen + repetitionLevelByteLen + 4);
            System.out.println("  All remaining bytes: " + buffer.remaining());
            System.out.print("  Bytes (hex): ");
            int startPos = buffer.position();
            for (int j = 0; j < buffer.remaining(); j++) {
              System.out.printf("%02X ", buffer.get() & 0xFF);
            }
            System.out.println();

            buffer.position(startPos);
            RleDecoder decoder3 = new RleDecoder(buffer, calcBitWidth, numValues);
            try {
              int[] indices3 = decoder3.readAll();
              System.out.println(
                  "  Decoded indices (all bytes): " + java.util.Arrays.toString(indices3));
            } catch (Exception e) {
              System.out.println("  Failed: " + e.getMessage());
            }

            // Try with different bit widths
            for (int testBitWidth = 1; testBitWidth <= 3; testBitWidth++) {
              System.out.println("\n  === Trying bit-width " + testBitWidth + " ===");
              buffer.position(startPos);
              RleDecoder decoder4 = new RleDecoder(buffer, testBitWidth, numValues);
              try {
                int[] indices4 = decoder4.readAll();
                System.out.println("  Decoded indices: " + java.util.Arrays.toString(indices4));
              } catch (Exception e) {
                System.out.println("  Failed: " + e.getMessage());
              }
            }

            // Try decoding value-by-value to see the progression
            System.out.println("\n  === Decoding value-by-value ===");
            buffer.position(startPos);
            RleDecoder decoder5 = new RleDecoder(buffer, calcBitWidth, numValues);
            for (int j = 0; j < numValues; j++) {
              int val = decoder5.readNext();
              System.out.println("  Value " + j + ": " + val);
              System.out.println("    Buffer position: " + buffer.position());
            }
          }
        }
      }
    }
  }

  /**
   * Calculate the minimum bit width needed to represent values 0 to maxValue-1
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
}
