package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class DebugDeltaPacked {
  public static void main(String[] args) throws IOException {
    String filePath = "src/test/data/delta_binary_packed.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Find column 'bitwidth0'
      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnDescriptor col = schema.getColumn(i);

        if (col.getPathString().equals("bitwidth0")) {
          System.out.println("=== Debugging column 'bitwidth0' (INT64) ===");

          PageReader pageReader = rowGroup.getColumnPageReader(i);
          List<Page> pages = pageReader.readAllPages();

          System.out.println("  Total pages: " + pages.size());

          for (Page page : pages) {
            System.out.println("  Page class: " + page.getClass().getName());
            if (page instanceof Page.DataPageV2 dataPageV2) {
              System.out.println("DataPageV2 found:");
              System.out.println("  Num values: " + dataPageV2.numValues());
              System.out.println("  Encoding: " + dataPageV2.encoding());

              ByteBuffer buffer = dataPageV2.data().duplicate();
              buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

              System.out.println("  Buffer size: " + buffer.remaining());

              // Read first 50 bytes
              System.out.print("  First 50 bytes: ");
              for (int j = 0; j < Math.min(50, buffer.remaining()); j++) {
                System.out.printf("0x%02x ", buffer.get(buffer.position() + j));
              }
              System.out.println();

              // Try reading header
              System.out.println("\nTrying to parse header:");
              int blockSize = readUnsignedVarInt(buffer);
              System.out.println("  Block size: " + blockSize);

              int numMiniBlocks = readUnsignedVarInt(buffer);
              System.out.println("  Num mini-blocks: " + numMiniBlocks);

              int totalValueCount = readUnsignedVarInt(buffer);
              System.out.println("  Total value count: " + totalValueCount);

              long firstValue = readZigzagVarLong(buffer);
              System.out.println("  First value: " + firstValue);

              // Skip minDelta
              System.out.println("\n  Remaining after header: " + buffer.remaining());
              long minDelta = readZigzagVarLong(buffer);
              System.out.println("  Min delta: " + minDelta);
              System.out.println("  Remaining after minDelta: " + buffer.remaining());

              // Try decoding
              buffer.rewind();
              try {
                DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(buffer, true);
                long[] values = decoder.decodeInt64(200);
                System.out.println("\n  Successfully decoded " + values.length + " values");
                System.out.println(
                    "  First few values: " + values[0] + ", " + values[1] + ", " + values[2]);
              } catch (Exception e) {
                System.out.println("\n  Failed to decode: " + e.getMessage());
                e.printStackTrace();
              }
            }
          }
          break;
        }
      }
    }
  }

  private static int readUnsignedVarInt(ByteBuffer buffer) {
    int result = 0;
    int shift = 0;
    while (true) {
      byte b = buffer.get();
      result |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return result;
  }

  private static long readZigzagVarLong(ByteBuffer buffer) {
    long encoded = 0;
    int shift = 0;
    while (true) {
      byte b = buffer.get();
      encoded |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return (encoded >>> 1) ^ -(encoded & 1);
  }
}
