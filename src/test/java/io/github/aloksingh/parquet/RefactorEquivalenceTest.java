package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Behavior tripwire for the improvement-16 refactor (splitting monolithic decoding/writing
 * into shared, testable components).
 *
 * <p>Pins three things against the pre-refactor code:
 * <ol>
 *   <li><b>Writer byte identity</b> — a pinned SHA-256 of each generated file's bytes and of a
 *       canonical rendering of the rows read back through the row API.</li>
 *   <li><b>Decode identity</b> — exact decoded values for every encoding family, including the
 *       ones the writer does not emit (DELTA_*, BYTE_STREAM_SPLIT, RLE boolean).</li>
 *   <li><b>Malformed-input behavior</b> — exact exception type and message for representative
 *       corrupt inputs per decoder family.</li>
 * </ol>
 *
 * <p><b>Digests and pinned messages change only on deliberate behavior changes, never on
 * refactors.</b> If an extraction makes this test fail, fix the extraction, not the pins.
 *
 * <p>Bootstrap: when a pin is missing the assertion prints {@code PIN <name> <value>} or
 * {@code PINMSG <name> <value>} on stdout; paste those constants into {@link #WRITER_PINS} /
 * {@link #ERROR_PINS}.
 */
class RefactorEquivalenceTest {

  // ---- pinned constants: "cell" -> "fileSha256:rowsSha256" ----
  // Digests change only on deliberate behavior changes, never on refactors.
  private static final Map<String, String> WRITER_PINS = Map.ofEntries(
      Map.entry("int32_required",
          "cde8d8961ac3f986996bf35f5656a76479f47e54d6bb99da74e2dcef72751f89"
              + ":9155279f1ab0a1561f682bd0912b5b438b28c8a148093eb47e30d206177a8503"),
      Map.entry("int32_optional_sparse",
          "28fe6053de796e69c502b9292933df39613a2a72612a3c7c1650541d65d940d2"
              + ":a1d4eebafd806a7f2baf3cb212e831e0740ce7c06da5e71021b58a69b5129750"),
      Map.entry("int64_optional_all_null",
          "37162cc96916f06959b3912200b176c4cdab46ba8fb3b043d8ef190faeb14bd0"
              + ":367212b5f745d8c4994b494420cd6e5cea043ab39780197ff39e2fdf9fc638ce"),
      Map.entry("float_required",
          "d3e9c1cf869769ee1244049ffc1a390df1dd6a7e6871b67225677fb4e42abfea"
              + ":fc59a748377592529dec06bc5137f7c4f4ccb425f33b47d19f1822600c9e1719"),
      Map.entry("double_sparse",
          "7fca8b849cc20c7e456790ae5beecaafe4a6773d431e551b3dff93c7a2031945"
              + ":9e9ce811fe68a92cebcbea249a321efebe82f64487f406e6555aba148ec8a5b4"),
      Map.entry("boolean_required",
          "3d989dca5a237f3c847e4f28a5ef545fec8bd347dcb6f6e892a7915a9b081d3e"
              + ":2ace59cc34d08e0f88b80f2549891d1cebc15547ee463986eabc8d4e176df3f3"),
      Map.entry("boolean_sparse",
          "fd822190fec243c30edef4bd08f238c453cfaf16253d3e25debb76697ca909a9"
              + ":093e768d47d4b40cf6a9d06dd987c3ec0d4c8a52f29e8fd58d5b48c06cba2d3c"),
      Map.entry("byte_array_raw",
          "a43e533a88cca8dbd833dfac57394ec620c45794dfa3121b8b3a5d9750dbc1af"
              + ":1ac4dafeea5ec3ac16aee0e079dca004fe214295f093b76fc94685352a499e46"),
      Map.entry("fixed_len_3",
          "65b2f4c536bcb7a165b6b743a16a4a791476db96d13faa21e4f13576b1d0e23e"
              + ":7306a07ef723f3ac3e3b44f0707281e05bbba5f91e88caa04194d5712afe9df0"),
      Map.entry("map_plus_primitive",
          "a5df01356fe39a9d44c196494ab29fa74d9ca961969b1ca925145cb23ecef3ac"
              + ":304117ad71247e81d611733af96e8283ac2a7295c2af4a4710d045dff00f167e"),
      Map.entry("int32_multipage",
          "cc2ee50a4da8722d649f533bdda6f02300f6d85cbfdecb66a3b4ea046c707168"
              + ":9faf176d1de1f6f37471d1f4b76c48abba3966b4209d1cda7492df60aa5deb31"),
      Map.entry("int32_sparse_gzip",
          "74a93c0fab7a945cf23bfca64b8b1a17df92709266becefd583a3c38e8142b1c"
              + ":da4921f73e43c401e5698863d00800ebfe4afe2a8afc5010cfe2de1b1152ff54"),
      Map.entry("byte_array_snappy",
          "a134773dc1eafe0a9857ad11201a9039b95b386b7c030cad502eb798c6d55af7"
              + ":4365444d2e71dc579af27999c9104cd783a9670241ef13f14dadd5844c553c94")
  );

  // ---- pinned constants: "case" -> exact exception message ----
  private static final Map<String, String> ERROR_PINS = Map.ofEntries(
      Map.entry("truncated_rle_repeated_run", "Truncated RLE repeated value"),
      Map.entry("truncated_bit_packed_run", "Truncated bit-packed run payload"),
      Map.entry("zero_length_bit_packed_run", "Zero-length RLE/bit-packed run"),
      Map.entry("truncated_delta_varint", "Truncated DELTA block size varint"),
      Map.entry("column_values_type_mismatch",
          "Physical column type does not match its descriptor: INT32")
  );

  // ------------------------------------------------------------------
  // Part A: writer byte identity + round-trip identity
  // ------------------------------------------------------------------

  record Cell(String name, SchemaDescriptor schema, Object[][] rows,
              CompressionCodec codec, int pageSize, int rowGroupSize, double minCompressionRatio) {
    Cell(String name, SchemaDescriptor schema, Object[][] rows,
         CompressionCodec codec, int pageSize, int rowGroupSize) {
      this(name, schema, rows, codec, pageSize, rowGroupSize, 0.90);
    }
  }

  private static SchemaDescriptor scalar(Type type, int definition, int length) {
    return WriterValidationTest.scalar(type, definition, length);
  }

  private static SchemaDescriptor mapPlusPrimitive() {
    ColumnDescriptor after = new ColumnDescriptor(Type.INT64, new String[]{"after"}, 0, 0, 0);
    return SchemaDescriptor.fromLogicalColumns("root", List.of(
        SchemaDescriptor.createMapColumn("map", Type.BYTE_ARRAY, Type.INT32, true, true),
        new LogicalColumnDescriptor("after", LogicalType.PRIMITIVE, Type.INT64, after)));
  }

  static Stream<Cell> writerCells() {
    Map<String, Integer> map1 = new LinkedHashMap<>();
    map1.put("x", 7);
    map1.put("y", 9);
    Map<String, Integer> map2 = new LinkedHashMap<>();
    map2.put("z", null);
    Object[][] mapRows = {
        {map1, 10001L},
        {null, 10002L},
        {map2, 10003L},
    };
    Object[][] multiPage = new Object[101][];
    for (int i = 0; i < multiPage.length; i++) multiPage[i] = new Object[]{i};
    // Compressible data so GZIP/SNAPPY are actually retained (pins the compressed path).
    Object[][] gzipRows = new Object[64][];
    for (int i = 0; i < gzipRows.length; i++) {
      gzipRows[i] = new Object[]{i % 7 == 0 ? null : 42};
    }
    Object[][] snappyRows = new Object[64][];
    byte[] repeated = "hello world hello world".getBytes(StandardCharsets.US_ASCII);
    for (int i = 0; i < snappyRows.length; i++) snappyRows[i] = new Object[]{repeated};
    return Stream.of(
        new Cell("int32_required", scalar(Type.INT32, 0, 0),
            new Object[][]{{1}, {2}, {3}, {4}, {5}},
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("int32_optional_sparse", scalar(Type.INT32, 1, 0),
            new Object[][]{{1}, {null}, {3}, {null}, {5}, {6}, {null}, {8}},
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("int64_optional_all_null", scalar(Type.INT64, 1, 0),
            new Object[][]{{null}, {null}, {null}, {null}},
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("float_required", scalar(Type.FLOAT, 0, 0),
            new Object[][]{{1.5f}, {-2.25f}, {0.0f}, {3.75f}},
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("double_sparse", scalar(Type.DOUBLE, 1, 0),
            new Object[][]{{1.5}, {null}, {-2.25}, {0.0}, {null}, {4.5}},
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("boolean_required", scalar(Type.BOOLEAN, 0, 0),
            new Object[][]{{true}, {false}, {true}, {true}, {false}, {false}, {true}, {false},
                {true}, {false}},
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("boolean_sparse", scalar(Type.BOOLEAN, 1, 0),
            new Object[][]{{true}, {null}, {false}, {null}, {true}, {null}, {null}, {false}},
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("byte_array_raw", scalar(Type.BYTE_ARRAY, 0, 0),
            new Object[][]{
                {new byte[0]},
                {new byte[]{(byte) 0xff, 0x00, (byte) 0x80}},
                {"abc".getBytes(StandardCharsets.US_ASCII)},
                {new byte[]{7}}},
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("fixed_len_3", scalar(Type.FIXED_LEN_BYTE_ARRAY, 0, 3),
            new Object[][]{
                {new byte[]{1, 2, 3}}, {new byte[]{4, 5, 6}},
                {new byte[]{7, 8, 9}}, {new byte[]{-1, 0, 1}}},
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("map_plus_primitive", mapPlusPrimitive(), mapRows,
            CompressionCodec.UNCOMPRESSED, 1024, 4096),
        new Cell("int32_multipage", scalar(Type.INT32, 0, 0), multiPage,
            CompressionCodec.UNCOMPRESSED, 64, 256),
        new Cell("int32_sparse_gzip", scalar(Type.INT32, 1, 0), gzipRows,
            CompressionCodec.GZIP, 128, 1024, 0.5),
        new Cell("byte_array_snappy", scalar(Type.BYTE_ARRAY, 0, 0), snappyRows,
            CompressionCodec.SNAPPY, 128, 1024, 0.5)
    );
  }

  @ParameterizedTest
  @MethodSource("writerCells")
  void writerOutputAndRoundTripAreByteIdentical(Cell cell, @TempDir Path directory)
      throws IOException {
    Path file = directory.resolve(cell.name() + ".parquet");
    try (ParquetFileWriter writer = new ParquetFileWriter(file, cell.schema(), cell.codec(),
        cell.pageSize(), cell.rowGroupSize(), cell.minCompressionRatio())) {
      for (Object[] row : cell.rows()) {
        writer.addRow(new SimpleRowColumnGroup(cell.schema(), row));
      }
    }
    String fileHash = sha256(Files.readAllBytes(file));
    String rowHash = sha256(readCanonicalRows(file).getBytes(StandardCharsets.UTF_8));
    String actual = fileHash + ":" + rowHash;
    System.out.println("PIN " + cell.name() + " " + actual);
    String expected = WRITER_PINS.get(cell.name());
    assertNotNull(expected, "No pinned digest for cell '" + cell.name() + "'; pin: " + actual);
    assertEquals(expected, actual, "Behavior drift in writer cell '" + cell.name() + "'");
  }

  private static String readCanonicalRows(Path file) throws IOException {
    StringBuilder rendering = new StringBuilder();
    try (ParquetFileReader reader = new ParquetFileReader(file)) {
      ParquetRowIterator iterator = (ParquetRowIterator) reader.rowIterator();
      long count = 0;
      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        rendering.append(count).append(':');
        for (int i = 0; i < row.getColumnCount(); i++) {
          if (i > 0) rendering.append('|');
          rendering.append(render(row.getColumnValue(i)));
        }
        rendering.append('\n');
        count++;
      }
      rendering.append("total=").append(count);
    }
    return rendering.toString();
  }

  private static String render(Object value) {
    if (value == null) return "~";
    if (value instanceof byte[] bytes) return HexFormat.of().formatHex(bytes);
    if (value instanceof Map<?, ?> map) {
      return map.entrySet().stream()
          .map(entry -> render(entry.getKey()) + "=>" + render(entry.getValue()))
          .sorted()
          .collect(Collectors.joining(",", "{", "}"));
    }
    if (value instanceof List<?> list) {
      return list.stream().map(RefactorEquivalenceTest::render)
          .collect(Collectors.joining(",", "[", "]"));
    }
    return String.valueOf(value);
  }

  private static String sha256(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError(impossible);
    }
  }

  // ------------------------------------------------------------------
  // Part B: decode identity for every encoding family (exact values)
  // ------------------------------------------------------------------

  private static ColumnValues column(Type type, ColumnDescriptor descriptor, List<Page> pages) {
    return new ColumnValues(type, pages, descriptor, null);
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void deltaInt32RequiredIsPinned(boolean v2) {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 0, 0);
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.DELTA_BINARY_PACKED,
        new int[5], new int[5], DecodingTestSupport.delta(3, 5, 9, -2, 0));
    assertEquals(java.util.Arrays.asList(3, 5, 9, -2, 0),
        column(Type.INT32, descriptor, List.of(page)).decodeAsInt32());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void deltaInt32SparseIsPinned(boolean v2) {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 1, 0);
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.DELTA_BINARY_PACKED,
        new int[]{1, 0, 1, 1, 0}, new int[5], DecodingTestSupport.delta(3, 9, 0));
    assertEquals(java.util.Arrays.asList(3, null, 9, 0, null),
        column(Type.INT32, descriptor, List.of(page)).decodeAsInt32());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void deltaInt64ConsecutiveIsPinned(boolean v2) {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT64, 0, 0);
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.DELTA_BINARY_PACKED,
        new int[5], new int[5], DecodingTestSupport.consecutiveDelta(1000, -7, 5));
    assertEquals(java.util.Arrays.asList(1000L, 993L, 986L, 979L, 972L),
        column(Type.INT64, descriptor, List.of(page)).decodeAsInt64());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void deltaLengthByteArrayIsPinned(boolean v2) {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 0, 0);
    ByteBuffer fixture = concat(DecodingTestSupport.delta(2, 3, 1),
        ByteBuffer.wrap("abcdef".getBytes(StandardCharsets.US_ASCII)));
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.DELTA_LENGTH_BYTE_ARRAY,
        new int[3], new int[3], fixture);
    assertEquals(java.util.Arrays.asList("ab", "cde", "f"),
        column(Type.BYTE_ARRAY, descriptor, List.of(page)).decodeAsString());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void deltaLengthByteArraySparseIsPinned(boolean v2) {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 1, 0);
    ByteBuffer fixture = concat(DecodingTestSupport.delta(1, 2),
        ByteBuffer.wrap("xyz".getBytes(StandardCharsets.US_ASCII)));
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.DELTA_LENGTH_BYTE_ARRAY,
        new int[]{1, 0, 1}, new int[3], fixture);
    assertEquals(java.util.Arrays.asList("x", null, "yz"),
        column(Type.BYTE_ARRAY, descriptor, List.of(page)).decodeAsString());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void deltaByteArrayIsPinned(boolean v2) {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 0, 0);
    ByteBuffer fixture = concat(DecodingTestSupport.delta(0, 3, 2),
        DecodingTestSupport.delta(5, 2, 0),
        ByteBuffer.wrap("helloix".getBytes(StandardCharsets.US_ASCII)));
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.DELTA_BYTE_ARRAY,
        new int[3], new int[3], fixture);
    assertEquals(java.util.Arrays.asList("hello", "helix", "he"),
        column(Type.BYTE_ARRAY, descriptor, List.of(page)).decodeAsString());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void byteStreamSplitFloatIsPinned(boolean v2) {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.FLOAT, 0, 0);
    // Planes of 1.0f (3F800000), -2.5f (C0200000), 3.25f (40500000), little-endian lanes.
    ByteBuffer planes = ByteBuffer.wrap(new byte[]{
        0, 0, 0,
        0, 0, 0,
        (byte) 0x80, 0x20, 0x50,
        0x3f, (byte) 0xc0, 0x40});
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.BYTE_STREAM_SPLIT,
        new int[3], new int[3], planes);
    assertEquals(java.util.Arrays.asList(1.0f, -2.5f, 3.25f),
        column(Type.FLOAT, descriptor, List.of(page)).decodeAsFloat());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void byteStreamSplitDoubleSparseIsPinned(boolean v2) {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.DOUBLE, 1, 0);
    // Planes of 1.5 (3FF8000000000000), -2.25 (C002000000000000), little-endian lanes.
    ByteBuffer planes = ByteBuffer.wrap(new byte[]{
        0, 0,
        0, 0,
        0, 0,
        0, 0,
        0, 0,
        0, 0,
        (byte) 0xf8, 0x02,
        0x3f, (byte) 0xc0});
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.BYTE_STREAM_SPLIT,
        new int[]{1, 0, 1}, new int[3], planes);
    assertEquals(java.util.Arrays.asList(1.5, null, -2.25),
        column(Type.DOUBLE, descriptor, List.of(page)).decodeAsDouble());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void rleBooleanIsPinned(boolean v2) {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BOOLEAN, 0, 0);
    // Hybrid-RLE repeated runs (LSB 0) of two values each: true,true,false,false,true,true,false,false.
    ByteBuffer fixture = ByteBuffer.wrap(new byte[]{
        8, 0, 0, 0,
        4, 1, 4, 0, 4, 1, 4, 0}).order(java.nio.ByteOrder.LITTLE_ENDIAN);
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.RLE,
        new int[8], new int[8], fixture);
    assertEquals(java.util.Arrays.asList(true, true, false, false, true, true, false, false),
        column(Type.BOOLEAN, descriptor, List.of(page)).decodeAsBoolean());
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void nestedDictionaryIsPinned(boolean v2) {
    // Fixture copied verbatim from DecodingNullablePageTest (proven behavior).
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 2, 0);
    Page.DictionaryPage dictionary = new Page.DictionaryPage(DecodingTestSupport.plain(
        Type.BYTE_ARRAY, new byte[]{'a'}, new byte[]{'b'}), 2, Encoding.PLAIN);
    Page page = DecodingTestSupport.page(v2, descriptor, Encoding.RLE_DICTIONARY,
        new int[]{2, 1, 2}, new int[3], ByteBuffer.wrap(new byte[]{1, 3, 2}));
    assertEquals(java.util.Arrays.asList("a", null, "b"),
        column(Type.BYTE_ARRAY, descriptor, List.of(dictionary, page)).decodeAsString());
  }

  private static ByteBuffer concat(ByteBuffer... parts) {
    int size = 0;
    for (ByteBuffer part : parts) size += part.remaining();
    ByteBuffer joined = ByteBuffer.allocate(size);
    for (ByteBuffer part : parts) joined.put(part.duplicate());
    return (ByteBuffer) joined.flip();
  }

  // ------------------------------------------------------------------
  // Part C: malformed-input behavior (exact type + message)
  // ------------------------------------------------------------------

  @Test
  void truncatedRleRepeatedRunIsPinned() {
    // Header 4 = repeated run of 2 values whose value byte is missing.
    ParquetException failure = assertThrows(ParquetException.class,
        () -> new RleDecoder(ByteBuffer.wrap(new byte[]{0x04}), 1, 2).readAll());
    pinError("truncated_rle_repeated_run", failure);
  }

  @Test
  void truncatedBitPackedRunIsPinned() {
    // Header 5 = bit-packed run of 2 groups (16 values) whose payload is missing.
    ParquetException failure = assertThrows(ParquetException.class,
        () -> new RleDecoder(ByteBuffer.wrap(new byte[]{0x05}), 1, 16).readAll());
    pinError("truncated_bit_packed_run", failure);
  }

  @Test
  void zeroLengthBitPackedRunIsPinned() {
    ParquetException failure = assertThrows(ParquetException.class,
        () -> new RleDecoder(ByteBuffer.wrap(new byte[]{0x00}), 4, 8).readAll());
    pinError("zero_length_bit_packed_run", failure);
  }

  @Test
  void truncatedDeltaVarintIsPinned() {
    ParquetException failure = assertThrows(ParquetException.class,
        () -> new DeltaBinaryPackedDecoder(ByteBuffer.wrap(new byte[]{(byte) 0x80}), false));
    pinError("truncated_delta_varint", failure);
  }

  @Test
  void columnValuesTypeMismatchIsPinned() {
    ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT64, 0, 0);
    Page page = DecodingTestSupport.page(false, descriptor, Encoding.PLAIN,
        new int[]{0}, new int[]{0}, DecodingTestSupport.plain(Type.INT64, 17L));
    ParquetException failure = assertThrows(ParquetException.class,
        () -> new ColumnValues(Type.INT32, List.of(page), descriptor, null));
    pinError("column_values_type_mismatch", failure);
  }

  private static void pinError(String name, ParquetException failure) {
    System.out.println("PINMSG " + name + " " + failure.getMessage());
    String expected = ERROR_PINS.get(name);
    assertNotNull(expected,
        "No pinned message for error case '" + name + "'; pin: " + failure.getMessage());
    assertEquals(expected, failure.getMessage(), "Behavior drift in error case '" + name + "'");
  }
}
