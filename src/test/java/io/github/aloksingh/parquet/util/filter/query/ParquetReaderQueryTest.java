package io.github.aloksingh.parquet.util.filter.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.aloksingh.parquet.FilteringParquetRowIterator;
import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ParquetFileWriter;
import io.github.aloksingh.parquet.ParquetRowIterator;
import io.github.aloksingh.parquet.RowColumnGroupIterator;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;
import io.github.aloksingh.parquet.util.filter.ColumnFilter;
import io.github.aloksingh.parquet.util.filter.ColumnFilterDescriptor;
import io.github.aloksingh.parquet.util.filter.ColumnFilters;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ParquetReaderQueryTest {

  @TempDir
  Path tempDir;

  @Test
  void testFilters() throws Exception {
    LogicalColumnDescriptor messageCol = SchemaDescriptor.createMapColumn(
        "message",
        Type.BYTE_ARRAY,  // String key
        Type.BYTE_ARRAY,  // String value
        true,   // map itself is optional
        true// values can be NULL
    );

    List<LogicalColumnDescriptor> logicalColumns = Arrays.asList(
        toLogicalColumn("id", Type.BYTE_ARRAY, 0),
        toLogicalColumn("application", Type.BYTE_ARRAY, 1),
        toLogicalColumn("timestamp", Type.INT64, 1),
        toLogicalColumn("level", Type.BYTE_ARRAY, 1),
        toLogicalColumn("hostname", Type.BYTE_ARRAY, 1),
        toLogicalColumn("process", Type.BYTE_ARRAY, 1),
        toLogicalColumn("message_text", Type.BYTE_ARRAY, 1),
        messageCol
    );

    var schema = SchemaDescriptor.fromLogicalColumns(
        "log_schema",
        logicalColumns
    );
    Path outputFile = tempDir.resolve("complex_row.parquet");

    // Write test data
    List<Map<String, String>> expectedMaps = new ArrayList<>();
    Map<String, String> map1 = new LinkedHashMap<>();
    map1.put("message.queryHash", "5a6a230001");
    map1.put("message.method", "");
    map1.put("message.level", "INFO");
    map1.put("message.executionState", "start");
    map1.put("message.class", "org.foo.bar." + UUID.randomUUID());
    map1.put("message.timestamp", "2024-01-18T00:00:00Z");
    map1.put("message.tableName", "foo");
    map1.put("message.sql", "insert into table_foo" + UUID.randomUUID());
    expectedMaps.add(map1);
    Map<String, String> map2 = new LinkedHashMap<>();
    map1.put("message.queryHash", "5a6a230002");
    map1.put("message.method", "");
    map1.put("message.level", "ERROR");
    map1.put("message.executionState", "start");
    map1.put("message.class", "org.foo.bar." + UUID.randomUUID());
    map1.put("message.timestamp", "2024-01-18T00:00:01Z");
    map1.put("message.tableName", "bar");
    map1.put("message.sql", "insert into table_bar" + UUID.randomUUID());
    expectedMaps.add(map2);
    List rows = new ArrayList();
    long ts = System.currentTimeMillis();
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      for (int i = 0; i < expectedMaps.size(); i++) {
        Object[] row = new Object[schema.getNumLogicalColumns()];
        int idx = 0;
        row[idx++] = UUID.randomUUID().toString();
        row[idx++] = "app1";
        row[idx++] = ts + i;
        row[idx++] = "DEBUG";
        row[idx++] = "HOST" + i;
        row[idx++] = "PROCESS" + (i + 1);
        row[idx++] = String.valueOf(expectedMaps.get(i));
        row[idx++] = expectedMaps.get(i);
        rows.add(row);
        writer.addRow(new SimpleRowColumnGroup(schema, row));
      }
    }

    // Read data back
    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ParquetMetadata metadata = reader.getMetadata();

      // Verify metadata
      assertEquals(expectedMaps.size(), metadata.fileMetadata().numRows());

      // Verify schema - we expect 3 physical columns (id, key, value)
      SchemaDescriptor readSchema = reader.getSchema();
      assertEquals(9, readSchema.getNumColumns(), "Should have 3 physical columns");

      // Verify we have 2 logical columns (id, item map)
      assertEquals(8, readSchema.getNumLogicalColumns(), "Should have 8 logical columns");

      // Read rows
      RowColumnGroupIterator iterator = reader.rowIterator();
      List<Map<String, String>> actualMaps = new ArrayList<>();

      while (iterator.hasNext()) {
        RowColumnGroup row = iterator.next();
        Object mapValue = row.getColumnValue("message");

        if (mapValue == null) {
          actualMaps.add(null);
        } else if (mapValue instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, String> map = (Map<String, String>) mapValue;
          actualMaps.add(map);
        } else {
          fail("Expected Map or null, got: " + mapValue.getClass());
        }
      }
    }
    QueryParser parser = new BaseQueryParser();
    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ColumnFilterDescriptor filterDescriptor = parser.parse("application=app1");
      ColumnFilter filter = new ColumnFilters().createFilter(reader.getSchema(), filterDescriptor);
      FilteringParquetRowIterator filteringParquetRowIterator =
          new FilteringParquetRowIterator(new ParquetRowIterator(reader), filter);
      long matchingRowCount = filteringParquetRowIterator.getMatchingRowCount();
      assertEquals(2, matchingRowCount);
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ColumnFilterDescriptor filterDescriptor = parser.parse("message[\"message.level\"]=ERROR");
      ColumnFilter filter = new ColumnFilters().createFilter(reader.getSchema(), filterDescriptor);
      FilteringParquetRowIterator filteringParquetRowIterator =
          new FilteringParquetRowIterator(new ParquetRowIterator(reader), filter);
      RowColumnGroup rowColumnGroup = filteringParquetRowIterator.next();
      assertNotNull(rowColumnGroup);
      Map messageMap = (Map) rowColumnGroup.getColumnValue("message");
      System.out.println("Found matched row:" + messageMap);
      assertEquals("ERROR", messageMap.get("message.level"));
      assertFalse(filteringParquetRowIterator.hasNext());
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ColumnFilterDescriptor filterDescriptor = parser.parse("timestamp=gt(" + ts + ")");
      System.out.println(filterDescriptor);
      ColumnFilter filter = new ColumnFilters().createFilter(reader.getSchema(), filterDescriptor);
      FilteringParquetRowIterator filteringParquetRowIterator =
          new FilteringParquetRowIterator(new ParquetRowIterator(reader), filter);
      RowColumnGroup rowColumnGroup = filteringParquetRowIterator.next();
      assertNotNull(rowColumnGroup);
      assertEquals(ts + 1, rowColumnGroup.getColumnValue("timestamp"));
      assertFalse(filteringParquetRowIterator.hasNext());
    }

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ColumnFilterDescriptor filterDescriptor = parser.parse("timestamp=gte(" + ts + ")");
      ColumnFilter filter = new ColumnFilters().createFilter(reader.getSchema(), filterDescriptor);
      FilteringParquetRowIterator filteringParquetRowIterator =
          new FilteringParquetRowIterator(new ParquetRowIterator(reader), filter);
      RowColumnGroup rowColumnGroup = filteringParquetRowIterator.next();
      assertNotNull(rowColumnGroup);
      assertEquals(ts, rowColumnGroup.getColumnValue("timestamp"));
      assertTrue(filteringParquetRowIterator.hasNext());
      rowColumnGroup = filteringParquetRowIterator.next();
      assertNotNull(rowColumnGroup);
      assertEquals(ts + 1, rowColumnGroup.getColumnValue("timestamp"));
      assertFalse(filteringParquetRowIterator.hasNext());
    }


  }

  private LogicalColumnDescriptor toLogicalColumn(String id, Type type, int maxDefinitionLevel) {
    return new LogicalColumnDescriptor(
        id,
        LogicalType.PRIMITIVE,
        type,
        new ColumnDescriptor(type, new String[] {id}, maxDefinitionLevel, 0, 0)
    );
  }

}
