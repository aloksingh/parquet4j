package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for DELTA_BINARY_PACKED encoding support.
 *
 * <p>The in-memory tests below feed hand-built delta streams to the decoder and assert
 * the exact decoded values. The file-based tests assert exact decoded VALUES and counts
 * against corpus files whose columns use DELTA_BINARY_PACKED (verified through the page
 * encoding). Wire-byte level golden checks live in StorageDeltaValidationTest and are not
 * duplicated here.
 */
class DeltaEncodingTest {

  private static final String TEST_DATA_DIR = "src/test/data/";

  @Test
  void testDeltaBinaryPackedBasic() {
    // Create a simple delta-encoded sequence: [100, 101, 102, 103, 104]
    // Deltas: [1, 1, 1, 1]
    ByteBuffer buffer = ByteBuffer.allocate(100);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // Block header:
    // - block size = 128 (must be multiple of 128)
    writeUnsignedVarInt(buffer, 128);
    // - num mini-blocks = 4 (miniblock size = 32, must be multiple of 32)
    writeUnsignedVarInt(buffer, 4);
    // - total value count = 5
    writeUnsignedVarInt(buffer, 5);
    // - first value = 100 (zigzag encoded: 200)
    writeZigzagVarLong(buffer, 100);

    // Min delta = 1 (zigzag encoded: 2)
    writeZigzagVarLong(buffer, 1);

    // Bit widths for 4 miniblocks (all 0 because deltas equal min delta)
    for (int i = 0; i < 4; i++) {
      buffer.put((byte) 0);
    }

    buffer.flip();

    DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(buffer, false);
    int[] values = decoder.decodeInt32(5);

    assertArrayEquals(new int[] {100, 101, 102, 103, 104}, values);
  }

  @Test
  void testDeltaBinaryPackedVariable() {
    // Create a simpler sequence: [10, 12, 14, 16]
    // Deltas: [2, 2, 2]
    // Min delta: 2
    // All deltas are min delta, so bit width = 0
    ByteBuffer buffer = ByteBuffer.allocate(100);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    // Block header
    writeUnsignedVarInt(buffer, 128);  // block size
    writeUnsignedVarInt(buffer, 4);  // num mini-blocks
    writeUnsignedVarInt(buffer, 4);  // total value count
    writeZigzagVarLong(buffer, 10);  // first value

    // Min delta = 2
    writeZigzagVarLong(buffer, 2);

    // Bit widths for 4 miniblocks (all 0 because deltas equal min delta)
    for (int i = 0; i < 4; i++) {
      buffer.put((byte) 0);
    }

    buffer.flip();

    DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(buffer, false);
    int[] values = decoder.decodeInt32(4);

    assertArrayEquals(new int[] {10, 12, 14, 16}, values);
  }

  @Test
  void testDeltaBinaryPackedNegativeValues() {
    // Test with negative values: [-5, -3, -1, 2, 4]
    // Deltas: [2, 2, 3, 2]
    // Min delta: 2
    // Packed deltas: [0, 0, 1, 0]
    ByteBuffer buffer = ByteBuffer.allocate(100);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    writeUnsignedVarInt(buffer, 128);
    writeUnsignedVarInt(buffer, 4);
    writeUnsignedVarInt(buffer, 5);
    writeZigzagVarLong(buffer, -5);  // first value

    writeZigzagVarLong(buffer, 2);  // min delta

    // Bit widths for 4 miniblocks (first width 1, rest 0)
    buffer.put((byte) 1);
    buffer.put((byte) 0);
    buffer.put((byte) 0);
    buffer.put((byte) 0);
    // Bit-pack [0, 0, 1, 0] with 1 bit each -> 0b00000100, then padding
    buffer.put((byte) 0b00000100);
    buffer.put((byte) 0);
    buffer.put((byte) 0);
    buffer.put((byte) 0);

    buffer.flip();

    DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(buffer, false);
    int[] values = decoder.decodeInt32(5);

    assertArrayEquals(new int[] {-5, -3, -1, 2, 4}, values);
  }

  @Test
  void testDeltaBinaryPackedInt64() {
    // Test with 64-bit values
    ByteBuffer buffer = ByteBuffer.allocate(100);
    buffer.order(ByteOrder.LITTLE_ENDIAN);

    writeUnsignedVarInt(buffer, 128);
    writeUnsignedVarInt(buffer, 4);
    writeUnsignedVarInt(buffer, 4);
    writeZigzagVarLong(buffer, 1000000000L);

    writeZigzagVarLong(buffer, 1000L);
    // Bit widths for 4 miniblocks (all 0)
    for (int i = 0; i < 4; i++) {
      buffer.put((byte) 0);
    }

    buffer.flip();

    DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(buffer, true);
    long[] values = decoder.decodeInt64(4);

    assertArrayEquals(new long[] {1000000000L, 1000001000L, 1000002000L, 1000003000L}, values);
  }

  /**
   * Exact decoded values for DELTA_BINARY_PACKED INT64 columns with definition levels
   * (delta_encoding_optional_column.parquet, pyarrow golden values).
   */
  @Test
  void testReadDeltaBinaryPackedOptionalFile() throws IOException {
    String filePath = TEST_DATA_DIR + "delta_encoding_optional_column.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();

      // Columns 0..8 are INT64 and use DELTA_BINARY_PACKED; assert every data page
      // uses it so these assertions really exercise the delta path.
      for (int i = 0; i < 9; i++) {
        ColumnDescriptor col = schema.getColumn(i);
        assertEquals(Type.INT64, col.physicalType(), col.getPathString());
        ColumnValues values = rowGroup.readColumn(i);
        assertAllDataPagesUse(values, Encoding.DELTA_BINARY_PACKED, col.getPathString());
        assertEquals(100, values.decodeAsInt64().size(),
            col.getPathString() + " must decode exactly 100 values");
      }

      // c_customer_sk: descending 100..1
      List<Long> customerSk =
          rowGroup.readColumn(0).decodeAsInt64();
      assertEquals(
          List.of(100L, 99L, 98L, 97L, 96L, 95L, 94L, 93L, 92L, 91L, 90L, 89L, 88L, 87L,
              86L, 85L, 84L, 83L, 82L, 81L, 80L, 79L, 78L, 77L, 76L, 75L, 74L, 73L, 72L,
              71L, 70L, 69L, 68L, 67L, 66L, 65L, 64L, 63L, 62L, 61L, 60L, 59L, 58L, 57L,
              56L, 55L, 54L, 53L, 52L, 51L, 50L, 49L, 48L, 47L, 46L, 45L, 44L, 43L, 42L,
              41L, 40L, 39L, 38L, 37L, 36L, 35L, 34L, 33L, 32L, 31L, 30L, 29L, 28L, 27L,
              26L, 25L, 24L, 23L, 22L, 21L, 20L, 19L, 18L, 17L, 16L, 15L, 14L, 13L, 12L,
              11L, 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L),
          customerSk, "c_customer_sk values must match the golden exactly");

      // c_current_cdemo_sk: optional values with nulls at rows 66, 77 and 85
      List<Long> demoSk = rowGroup.readColumn(1).decodeAsInt64();
      assertEquals(
          java.util.Arrays.asList(1254468L, 622676L, 574977L, 418763L, 1148074L, 796503L, 451893L,
              647375L, 953084L, 827176L, 417827L, 694848L, 495575L, 1452824L, 1428237L,
              1293499L, 1250744L, 976724L, 75627L, 728917L, 1499808L, 389494L, 1092537L,
              915180L, 526064L, 1888603L, 1434225L, 425740L, 1608738L, 1292064L, 1460929L,
              971368L, 779965L, 1118294L, 747190L, 1778884L, 1260191L, 1790374L, 821787L,
              1620078L, 1179671L, 1895444L, 528756L, 752932L, 344460L, 783093L, 380102L,
              1597348L, 534808L, 532799L, 759177L, 936800L, 8817L, 1634314L, 843672L,
              1036174L, 497758L, 385562L, 1867377L, 941420L, 1795301L, 1617182L, 766645L,
              827972L, 655414L, 339036L, null, 1680761L, 1369589L, 1275120L, 84232L,
              1634269L, 889961L, 111621L, 230278L, 476176L, 17113L, null, 490494L,
              442697L, 1185612L, 1161742L, 1361151L, 707524L, 1196373L, null, 929344L,
              1128748L, 502141L, 1114415L, 1207553L, 1168667L, 1215897L, 68377L, 213219L,
              953372L, 1703214L, 1473522L, 819667L, 980124L),
          demoSk, "c_current_cdemo_sk values must match the golden exactly");
    }
  }

  /**
   * Exact decoded values for a DELTA_BINARY_PACKED corpus covering every bit width
   * (delta_binary_packed.parquet: bitwidth0..bitwidth64 INT64 + int_value INT32).
   */
  @Test
  void testReadDeltaBinaryPackedAllBitWidths() throws IOException {
    String filePath = TEST_DATA_DIR + "delta_binary_packed.parquet";
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      SchemaDescriptor schema = reader.getSchema();
      assertEquals(66, schema.getNumColumns(), "bitwidth0..64 + int_value");

      for (int i = 0; i < schema.getNumColumns(); i++) {
        ColumnValues values = rowGroup.readColumn(i);
        assertAllDataPagesUse(values, Encoding.DELTA_BINARY_PACKED, schema.getColumn(i).getPathString());
        int expectedCount = 200;
        int actualCount = schema.getColumn(i).physicalType() == Type.INT64
            ? values.decodeAsInt64().size()
            : values.decodeAsInt32().size();
        assertEquals(expectedCount, actualCount,
            schema.getColumn(i).getPathString() + " must decode exactly 200 values");
      }

      // bitwidth0: all deltas equal the min delta (bit width 0), value is constant
      List<Long> bitwidth0 = rowGroup.readColumn(0).decodeAsInt64();
      assertEquals(200, bitwidth0.size());
      for (int i = 0; i < bitwidth0.size(); i++) {
        assertEquals(6374628540732951412L, bitwidth0.get(i),
            "bitwidth0[" + i + "]");
      }

      // int_value: full mixed-sign INT32 values decoded exactly
      List<Integer> intValue = rowGroup.readColumn(65).decodeAsInt32();
      assertEquals(
          List.of(-2070986743, -22783326, -1782018724, -795597708, -50404127,
              -1324028940, 1224303596, 1429112635, 834042975, 2046362238, -153007359,
              1051233348, 210007250, -1817882083, 220205244, 82429627, 702155563,
              1911942950, -905379917, -1030925156, 448016346, -1069926607, 1577807398,
              121762752, 1157398905, -159149608, -1086596487, -349032759, 644234840,
              -1216197075, -1937155996, -911957403, -1167656573, 912053501, -467195949,
              -391325924, 927502675, -586384928, -2061074542, -643614834, 1677819594,
              1356356082, -1352516827, -225556450, -1952200131, 1512239260, -1465878623,
              -1238759026, 506443817, -510642380, -1451880677, 1009396979, -1915496749,
              1335955453, 1112757104, -117656487, 1714747228, 964863654, -242482968,
              1850970679, 2021858393, -819473984, -859081176, 1631043917, -1868650121,
              -1733277845, 586207257, 597837078, 1387707060, -834578620, 968721222,
              -270375334, -1931846469, -1147510604, 2119942957, -1312764566, -18651112,
              -696773221, -1782824032, 218797988, -1628947536, -2020304383, 1420851349,
              -1208217736, -1063256458, -938070979, 235872980, -814454686, -1660238084,
              -636905219, -1811260799, 411888961, -1285929030, -475713454, 1732679049,
              -451372708, -1553763394, -2039789440, 695340617, 1442964907, 1555938110,
              1210151157, 423952213, 1041026686, 22197833, 622534857, -621822108,
              788493476, 79321857, -1749802096, -1534912089, -1431202288, -351702518,
              2142811258, 1257470651, 1746145889, -894802465, 1162853737, -1470789998,
              -1152005073, 297457703, 353671258, -506650075, 1555481201, -1973042833,
              1578654448, -613354219, -665463398, -1440521076, 1495913870, -1334468129,
              -221281690, 1872797913, 1925118055, 522368345, 483392733, 1412551999,
              846302975, -1505922148, 1843301998, -1034124461, 104332920, -738229721,
              -118550346, -1074429320, 1836985248, -625874078, 1785069594, -2078683524,
              -318239032, 591125801, -1552550893, -485578186, 2014100340, 162640946,
              1785915259, -359918765, 1941422918, 1837153026, -939062277, 1140306395,
              -1568357236, -1015707823, 1015247445, 767228679, 1889021218, -421048908,
              -905892578, -462180864, -1494333306, -893424967, 1225191101, 810009443,
              -2075311278, 35074056, -1515615921, -187725302, -1419344549, 1431169164,
              1727433986, -1061753866, 163947535, 1925460554, -1535105731, -1078195912,
              1003419371, -1808745234, 1081677334, 1076184091, -1844391978, -1170074517,
              -1537703209, -1077352087, 1739998497, 1620621738, 894696646, 311454177,
              -33600110, 254136712, 697406929),
          intValue, "int_value values must match the golden exactly");

      // Spot values across the bit-width range (exact golden values)
      List<Long> bitwidth63 = rowGroup.readColumn(63).decodeAsInt64();
      assertEquals(0L, bitwidth63.get(0));
      assertEquals(-4611686018427387904L, bitwidth63.get(1));
      assertEquals(5110441496851123501L, bitwidth63.get(199));
    }
  }

  /** Asserts that every data page of a column uses the expected encoding. */
  private static void assertAllDataPagesUse(ColumnValues values, Encoding expected,
                                            String column) {
    int dataPages = 0;
    for (Page page : values.getPages()) {
      if (page instanceof Page.DataPage dataPage) {
        assertEquals(expected, dataPage.encoding(), column + " data page encoding");
        dataPages++;
      } else if (page instanceof Page.DataPageV2 dataPage) {
        assertEquals(expected, dataPage.encoding(), column + " data page encoding");
        dataPages++;
      }
    }
    assertTrue(dataPages > 0, column + " must have at least one data page");
  }

  /**
   * Helper method to write unsigned varint
   */
  private void writeUnsignedVarInt(ByteBuffer buffer, int value) {
    while (value > 0x7F) {
      buffer.put((byte) ((value & 0x7F) | 0x80));
      value >>>= 7;
    }
    buffer.put((byte) value);
  }

  /**
   * Helper method to write zigzag-encoded varint
   */
  private void writeZigzagVarLong(ByteBuffer buffer, long value) {
    // Zigzag encode: (n << 1) ^ (n >> 63)
    long encoded = (value << 1) ^ (value >> 63);

    while (encoded > 0x7F) {
      buffer.put((byte) ((encoded & 0x7F) | 0x80));
      encoded >>>= 7;
    }
    buffer.put((byte) encoded);
  }
}
