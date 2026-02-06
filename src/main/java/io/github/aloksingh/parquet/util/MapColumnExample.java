package io.github.aloksingh.parquet.util;

import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.RowColumnGroupIterator;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Example application demonstrating how to read Parquet files containing MAP columns.
 * <p>
 * This utility reads a Parquet file with Map&lt;String, String&gt; columns and displays:
 * <ul>
 *   <li>Schema information including column names and types</li>
 *   <li>The first 10 rows of data with MAP values expanded</li>
 *   <li>Summary statistics including total row count</li>
 * </ul>
 * <p>
 * Usage: {@code java MapColumnExample <parquet-file-path>}
 * <p>
 * Example:
 * <pre>
 * mvn exec:java -Dexec.mainClass="util.org.wazokazi.parquet.MapColumnExample" \
 *   -Dexec.args="src/test/data/data_with_map_column.parquet"
 * </pre>
 */
public class MapColumnExample {

  /**
   * Private constructor to prevent instantiation of this utility class.
   */
  private MapColumnExample() {
    // Utility class
  }

  /**
   * Main entry point for the MapColumnExample utility.
   * <p>
   * Reads and displays the contents of a Parquet file containing MAP columns.
   * The program will print the schema, display up to 10 rows with expanded MAP values,
   * and provide a summary of the total row count.
   *
   * @param args command-line arguments; expects a single argument: the path to a Parquet file
   * @throws IOException if an error occurs while reading the Parquet file
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: java MapColumnExample <parquet-file-with-maps>");
      System.err.println("\nExample:");
      System.err.println(
          "  mvn exec:java -Dexec.mainClass=\"util.org.wazokazi.parquet.MapColumnExample\" \\");
      System.err.println("    -Dexec.args=\"src/test/data/data_with_map_column.parquet\"");
      System.exit(1);
    }

    String filePath = args[0];
    System.out.println("=== Reading Parquet File with Maps ===");
    System.out.println("File: " + filePath);
    System.out.println();
    List<LogicalColumnDescriptor> mapColumns = new ArrayList<>();
    try (ParquetFileReader reader = new ParquetFileReader(filePath)) {
      // Print schema info
      System.out.println("Schema:");
      System.out.println("  Total columns: " + reader.getSchema().getNumLogicalColumns());

      for (int i = 0; i < reader.getSchema().getNumLogicalColumns(); i++) {
        var col = reader.getSchema().getLogicalColumn(i);
        String typeInfo;
        if (col.isMap()) {
          mapColumns.add(col);
          var mapMeta = col.getMapMetadata();
          typeInfo = "MAP<" + mapMeta.keyType() + ", " + mapMeta.valueType() + ">";
        } else {
          typeInfo = col.getPhysicalType().toString();
        }
        System.out.println("  Column " + i + ": " + col.getName() + " (" + typeInfo + ")");
      }
      System.out.println();

      // Iterate and display rows
      System.out.println("Data (showing first 10 rows):");
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext() && rowCount < 10) {
        RowColumnGroup row = iterator.next();

        System.out.println("\nRow " + rowCount + ":");

        // Iterate over all columns
        for (int i = 0; i < row.getColumnCount(); i++) {
          LogicalColumnDescriptor column = reader.getSchema().getLogicalColumn(i);
          String colName = column.getName();
          Object value = row.getColumnValue(i);

          System.out.print("  " + colName + ": ");

          if (column.isMap()) {
            Map<String, String> map = (Map<String, String>) value;
            System.out.println("{");
            for (Map.Entry<String, String> entry : map.entrySet()) {
              System.out.println("    " + entry.getKey() + " -> " + entry.getValue());
            }
            System.out.println("  }");
          } else {
            // Display primitive value
            System.out.println(value);
          }
        }

        rowCount++;
      }

      System.out.println("\n=== Summary ===");
      System.out.println("Total rows in file: " + reader.getTotalRowCount());
      System.out.println("Rows displayed: " + rowCount);
    }
  }
}
