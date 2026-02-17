package io.github.aloksingh.parquet.util.filter.query;

import io.github.aloksingh.parquet.FilteringParquetRowIterator;
import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ParquetRowIterator;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.util.filter.ColumnFilter;
import io.github.aloksingh.parquet.util.filter.ColumnFilterDescriptor;
import io.github.aloksingh.parquet.util.filter.ColumnFilters;
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilterSet;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class provides a basic CLI to query a parquet file.
 * Usage
 *  java io.github.aloksingh.parquet.util.filter.query.ParquetFileQuery parquet-file-path filter_expression_1 filter_expression_2 ...
 *
 * Filter expressions must be formatted as "column_name=value". See BaseQueryParser for full range of expressions
 */
public class ParquetFileQuery {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println(
          "Usage: java io.github.aloksingh.parquet.util.filter.query.ParquetFileQuery <parquet-file-path> [filter_expression_1] [filter_expression_2] ...");
      System.err.println();
      System.err.println("Filter expression examples:");
      System.err.println("  column_name=value          - Exact match");
      System.err.println("  column_name=\"value\"        - Exact match with quoted value");
      System.err.println("  column_name=value*         - Prefix match");
      System.err.println("  column_name=*value         - Suffix match");
      System.err.println("  column_name=*value*        - Contains match");
      System.err.println("  column_name=lt(value)      - Less than");
      System.err.println("  column_name=lte(value)     - Less than or equal");
      System.err.println("  column_name=gt(value)      - Greater than");
      System.err.println("  column_name=gte(value)     - Greater than or equal");
      System.err.println("  column_name=isNull()       - Is null");
      System.err.println("  column_name=isNotNull()    - Is not null");
      System.err.println("  map_column[\"key\"]=value    - Map key/value match");
      System.exit(1);
    }

    String filePath = args[0];
    List<Path> parquetFiles = new ArrayList<>();
    File path = Paths.get(filePath).toFile();
    if (path.isDirectory()) {
      File[] files = path.listFiles((dir, name) -> name.toLowerCase().endsWith(".parquet"));
      if (files != null) {
        for (File file : files) {
          parquetFiles.add(file.toPath());
        }
        System.out.println("Files:" + parquetFiles.toString());
      } else {
        System.err.println("No files found at path:" + path);
      }
    } else {
      parquetFiles.add(path.toPath());
    }

    // Parse filter expressions (if any)
    List<String> filterExpressions = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      filterExpressions.add(args[i]);
    }
    final AtomicInteger rowCount = new AtomicInteger(0);
    RowColumnGroupVisitor visitor = new RowColumnGroupVisitor() {
      @Override
      public void visit(List<LogicalColumnDescriptor> columns, RowColumnGroup row) {
        if (rowCount.get() == 0) {
          printHeader(columns);
        }
        printRow(row, columns);
        rowCount.incrementAndGet();
      }
    };
    long startTime = System.currentTimeMillis();
    for (Path parquetFile : parquetFiles) {
      try {
        ParquetFileQueryResult fileResult =
            queryParquetFile(parquetFile, filterExpressions, visitor);
        System.out.println(fileResult);
      } catch (Exception e) {
        System.err.println("Error querying parquet file: " + e.getMessage());
        e.printStackTrace();
        System.exit(1);
      }
    }
    long elapsedTime = System.currentTimeMillis() - startTime;
    System.out.println(String.format("Total Rows: %,d in %,d ms", rowCount.get(), elapsedTime));
  }

  private static ParquetFileQueryResult queryParquetFile(Path parquetFile,
                                                         List<String> filterExpressions,
                                                         RowColumnGroupVisitor visitor)
      throws Exception {
    QueryParser parser = new BaseQueryParser();
    try (ParquetFileReader reader = new ParquetFileReader(parquetFile)) {
      // Create filters from expressions
      List<ColumnFilter> filters = new ArrayList<>();
      for (String expression : filterExpressions) {
        ColumnFilterDescriptor descriptor = parser.parse(expression);
        ColumnFilter filter = new ColumnFilters().createFilter(reader.getSchema(), descriptor);
        filters.add(filter);
      }

      // Get column names from schema
      List<LogicalColumnDescriptor> logicalColumns = reader.getSchema().logicalColumns();
      long startMillis = System.currentTimeMillis();
      // Print header
//      printHeader(logicalColumns);
      // Iterate through matching rows
      int rowCount = 0;
      if (filters.isEmpty()) {
        // No filters - return all rows
        ParquetRowIterator iterator = new ParquetRowIterator(reader);
        while (iterator.hasNext()) {
          visitor.visit(logicalColumns, iterator.next());
          rowCount++;
        }
      } else {
        // Create filtering iterator
        FilteringParquetRowIterator iterator;
        if (filters.size() == 1) {
          // Single filter
          iterator =
              new FilteringParquetRowIterator(new ParquetRowIterator(reader), filters.get(0));
        } else {
          // Multiple filters - combine with AND
          RowColumnGroupFilterSet filterSet =
              new RowColumnGroupFilterSet(FilterJoinType.All, filters);
          iterator = new FilteringParquetRowIterator(new ParquetRowIterator(reader), filterSet);
        }

        while (iterator.hasNext()) {
          RowColumnGroup row = iterator.next();
          visitor.visit(logicalColumns, row);
          rowCount++;
        }
      }
      System.out.println("\nTotal rows: " + rowCount);
      long elapsedTimeMillis = System.currentTimeMillis() - startMillis;
      System.out.println(String.format("Total time (ms): %,d", elapsedTimeMillis));
      return new ParquetFileQueryResult(parquetFile.toFile().getAbsolutePath(), rowCount,
          Duration.ofMillis(elapsedTimeMillis));
    }
  }

  private static void printHeader(List<LogicalColumnDescriptor> logicalColumns) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < logicalColumns.size(); i++) {
      if (i > 0) {
        sb.append("\t");
      }
      sb.append(logicalColumns.get(i).getName());
    }
    System.out.println(sb.toString());
  }

  private static void printRow(RowColumnGroup row, List<LogicalColumnDescriptor> logicalColumns) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < logicalColumns.size(); i++) {
      if (i > 0) {
        sb.append("\t");
      }
      Object value = row.getColumnValue(logicalColumns.get(i).getName());
      sb.append(formatValue(value));
    }
    System.out.println(sb.toString());
  }

  private static String formatValue(Object value) {
    if (value == null) {
      return "NULL";
    }
    return value.toString();
  }

  public record ParquetFileQueryResult(String file, long rowCount, Duration duration) {

  }

  public interface RowColumnGroupVisitor {
    void visit(List<LogicalColumnDescriptor> columns, RowColumnGroup row);
  }
}
