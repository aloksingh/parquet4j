package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for reading INT64-encoded DECIMAL data.
 * <p>
 * Test file: int64_decimal.parquet
 * - Created by: parquet-mr version 1.8.2
 * - Rows: 24
 * - Column: 'value' - DECIMAL(10, 2) stored as INT64
 * - Physical encoding: INT64 with PLAIN encoding
 * - Logical type: DECIMAL with precision=10, scale=2
 * <p>
 * Expected data (verified with pyarrow):
 * Values 1.00 through 24.00
 * - Stored as INT64: 100, 200, 300, ..., 2400
 * - To get decimal: unscaled_value / (10^scale) = unscaled_value / 100
 */
class Int64DecimalTest {

  @Test
  void testInt64DecimalFileStructure() throws IOException {
    String filePath = "src/test/data/int64_decimal.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetMetadata metadata = reader.getMetadata();
      SchemaDescriptor schema = reader.getSchema();

      // Verify file structure
      assertEquals(24, metadata.fileMetadata().numRows());
      assertEquals(1, schema.getNumColumns());

      // Verify column type
      ColumnDescriptor col = schema.getColumn(0);
      assertEquals("value", col.getPathString());
      assertEquals(Type.INT64, col.physicalType());
    }
  }

  @Test
  void testInt64DecimalValues() throws IOException {
    String filePath = "src/test/data/int64_decimal.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read INT64 values (unscaled)
      List<Long> unscaledValues = rowGroup.readColumn(0).decodeAsInt64();

      assertEquals(24, unscaledValues.size());

      // Expected unscaled values for 1.00 through 24.00 with scale=2
      // 1.00 = 100, 2.00 = 200, ..., 24.00 = 2400
      long[] expectedUnscaled = new long[24];
      for (int i = 0; i < 24; i++) {
        expectedUnscaled[i] = (i + 1) * 100L;
      }

      // Verify unscaled values
      for (int i = 0; i < 24; i++) {
        assertEquals(expectedUnscaled[i], unscaledValues.get(i),
            "Unscaled value at row " + i);
      }
    }
  }

  @Test
  void testInt64DecimalConversion() throws IOException {
    String filePath = "src/test/data/int64_decimal.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

      // Read INT64 values
      List<Long> unscaledValues = rowGroup.readColumn(0).decodeAsInt64();

      // Scale for DECIMAL(10, 2)
      int scale = 2;

      // Convert to BigDecimal
      BigDecimal[] actualDecimals = new BigDecimal[unscaledValues.size()];
      for (int i = 0; i < unscaledValues.size(); i++) {
        actualDecimals[i] = BigDecimal.valueOf(unscaledValues.get(i), scale);
      }

      // Expected decimal values: 1.00, 2.00, 3.00, ..., 24.00
      for (int i = 0; i < 24; i++) {
        BigDecimal expected = new BigDecimal(String.format("%d.00", i + 1));
        assertEquals(expected, actualDecimals[i],
            "Decimal value at row " + i);
      }
    }
  }

  @Test
  void testInt64DecimalFirstTenValues() throws IOException {
    String filePath = "src/test/data/int64_decimal.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> unscaledValues = rowGroup.readColumn(0).decodeAsInt64();

      int scale = 2;

      // Verify first 10 values in detail
      String[] expectedStrings = {
          "1.00", "2.00", "3.00", "4.00", "5.00",
          "6.00", "7.00", "8.00", "9.00", "10.00"
      };

      for (int i = 0; i < expectedStrings.length; i++) {
        BigDecimal actual = BigDecimal.valueOf(unscaledValues.get(i), scale);
        BigDecimal expected = new BigDecimal(expectedStrings[i]);

        assertEquals(expected, actual,
            "Row " + i + " should be " + expectedStrings[i]);
        assertEquals(expectedStrings[i], actual.toString(),
            "String representation at row " + i);
      }
    }
  }

  @Test
  void testInt64DecimalLastFiveValues() throws IOException {
    String filePath = "src/test/data/int64_decimal.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> unscaledValues = rowGroup.readColumn(0).decodeAsInt64();

      int scale = 2;

      // Verify last 5 values: 20.00, 21.00, 22.00, 23.00, 24.00
      long[] expectedUnscaled = {2000L, 2100L, 2200L, 2300L, 2400L};
      String[] expectedStrings = {"20.00", "21.00", "22.00", "23.00", "24.00"};

      for (int i = 0; i < 5; i++) {
        int rowIndex = 19 + i; // Start at row 19 (20.00)

        // Check unscaled value
        assertEquals(expectedUnscaled[i], unscaledValues.get(rowIndex),
            "Unscaled value at row " + rowIndex);

        // Check decimal value
        BigDecimal actual = BigDecimal.valueOf(unscaledValues.get(rowIndex), scale);
        BigDecimal expected = new BigDecimal(expectedStrings[i]);
        assertEquals(expected, actual,
            "Decimal value at row " + rowIndex);
      }
    }
  }

  @Test
  void testInt64DecimalNoNulls() throws IOException {
    String filePath = "src/test/data/int64_decimal.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> values = rowGroup.readColumn(0).decodeAsInt64();

      // Verify no null values
      for (int i = 0; i < values.size(); i++) {
        assertNotNull(values.get(i), "Row " + i + " should not be null");
      }
    }
  }

  @Test
  void testInt64DecimalScaleCalculation() throws IOException {
    String filePath = "src/test/data/int64_decimal.parquet";

    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
      List<Long> unscaledValues = rowGroup.readColumn(0).decodeAsInt64();

      // DECIMAL(10, 2) means scale = 2
      int scale = 2;
      int precision = 10;

      // Test various scale conversions
      // Row 0: 100 / 100 = 1.00
      assertEquals(new BigDecimal("1.00"),
          BigDecimal.valueOf(unscaledValues.get(0), scale));

      // Row 9: 1000 / 100 = 10.00
      assertEquals(new BigDecimal("10.00"),
          BigDecimal.valueOf(unscaledValues.get(9), scale));

      // Row 23: 2400 / 100 = 24.00
      assertEquals(new BigDecimal("24.00"),
          BigDecimal.valueOf(unscaledValues.get(23), scale));

      // Verify scale is consistent
      for (int i = 0; i < unscaledValues.size(); i++) {
        BigDecimal decimal = BigDecimal.valueOf(unscaledValues.get(i), scale);
        assertEquals(scale, decimal.scale(),
            "Scale should be " + scale + " at row " + i);
      }
    }
  }
}
