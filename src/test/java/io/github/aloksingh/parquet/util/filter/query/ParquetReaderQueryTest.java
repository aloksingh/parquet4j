package io.github.aloksingh.parquet.util.filter.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilterSet;
import java.io.IOException;
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
  public void testSingleFilter() throws Exception {
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
    assertMatches(outputFile, "application=app1", 2);
    assertMatches(outputFile, "timestamp=gt(" + ts + ")", 1);
    assertMatches(outputFile, "timestamp=gte(" + ts + ")", 2);
    assertMatches(outputFile, "message[\"message.level\"]=ERROR", 1);
  }

  @Test
  public void testFilterSets() throws Exception {
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
    map2.put("message.queryHash", "5a6a230002");
    map2.put("message.method", "");
    map2.put("message.level", "ERROR");
    map2.put("message.executionState", "start");
    map2.put("message.class", "org.foo.bar." + UUID.randomUUID());
    map2.put("message.timestamp", "2024-01-18T00:00:01Z");
    map2.put("message.tableName", "bar");
    map2.put("message.sql", "insert into table_bar" + UUID.randomUUID());
    expectedMaps.add(map2);
    Map<String, String> map3 = new LinkedHashMap<>();
    map3.put("message.queryHash", "5a6a230003");
    map3.put("message.method", "");
    map3.put("message.level", "DEBUG");
    map3.put("message.executionState", "end");
    map3.put("message.class", "org.foo.bar." + UUID.randomUUID());
    map3.put("message.timestamp", "2024-01-18T00:00:02Z");
    map3.put("message.tableName", "foo_bar");
    map3.put("message.sql", "insert into table_foo_bar" + UUID.randomUUID());
    expectedMaps.add(map3);
    List rows = new ArrayList();
    long ts = System.currentTimeMillis();
    try (ParquetFileWriter writer = new ParquetFileWriter(outputFile, schema)) {
      for (int i = 0; i < expectedMaps.size(); i++) {
        Object[] row = new Object[schema.getNumLogicalColumns()];
        int idx = 0;
        row[idx++] = UUID.randomUUID().toString();
        row[idx++] = "app1";
        row[idx++] = ts + i;
        Map<String, String> map = expectedMaps.get(i);
        row[idx++] = map.get("message.level");
        row[idx++] = "HOST" + i;
        row[idx++] = "PROCESS" + (i + 1);
        row[idx++] = String.valueOf(map);
        row[idx++] = map;
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
    assertMatches(outputFile, List.of("application=app1", "level=WARN"), 0);
    assertMatches(outputFile, List.of("application=app1", "level=ERROR"), 1);
    assertMatches(outputFile, List.of("application=app1", "level=DEBUG"), 1);
    assertMatches(outputFile, List.of("application=app1", "level=INFO"), 1);
    assertMatches(outputFile, List.of("application=app1", "level=INFO", "level=DEBUG"), 0);
  }

  private static void assertMatches(Path outputFile, List<String> filterExpressions, int matchCount)
      throws IOException {
    QueryParser parser = new BaseQueryParser();
    List<ColumnFilterDescriptor> filterDescriptors =
        filterExpressions.stream()
            .map(parser::parse)
            .toList();

    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      List<ColumnFilter> filters = filterDescriptors.stream()
          .map(d -> new ColumnFilters().createFilter(reader.getSchema(), d))
          .toList();
      RowColumnGroupFilterSet rowFilter = new RowColumnGroupFilterSet(FilterJoinType.All, filters);
      FilteringParquetRowIterator filteringParquetRowIterator =
          new FilteringParquetRowIterator(new ParquetRowIterator(reader), rowFilter);
      int foundMatches = 0;
      while (filteringParquetRowIterator.hasNext()) {
        RowColumnGroup rowColumnGroup = filteringParquetRowIterator.next();
        assertNotNull(rowColumnGroup);
        foundMatches = foundMatches + 1;
      }
      assertEquals(matchCount, foundMatches);
      assertFalse(filteringParquetRowIterator.hasNext());
    }
  }


  private static void assertMatches(Path outputFile, String filterExpression, int matchCount)
      throws IOException {
    QueryParser parser = new BaseQueryParser();
    try (ParquetFileReader reader = new ParquetFileReader(outputFile)) {
      ColumnFilterDescriptor filterDescriptor = parser.parse(filterExpression);
      ColumnFilter filter = new ColumnFilters().createFilter(reader.getSchema(), filterDescriptor);
      FilteringParquetRowIterator filteringParquetRowIterator =
          new FilteringParquetRowIterator(new ParquetRowIterator(reader), filter);
      for (int i = 0; i < matchCount; i++) {
        RowColumnGroup rowColumnGroup = filteringParquetRowIterator.next();
        assertNotNull(rowColumnGroup);
      }
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
