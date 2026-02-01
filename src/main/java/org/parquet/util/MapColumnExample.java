package org.parquet.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.parquet.SerializedFileReader;
import org.parquet.RowColumnGroupIterator;
import org.parquet.model.LogicalColumnDescriptor;
import org.parquet.model.RowColumnGroup;

/**
 * Example demonstrating how to read Parquet files with Map<String, String> columns
 */
public class MapColumnExample {

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: java MapColumnExample <parquet-file-with-maps>");
      System.err.println("\nExample:");
      System.err.println(
          "  mvn exec:java -Dexec.mainClass=\"org.parquet.util.MapColumnExample\" \\");
      System.err.println("    -Dexec.args=\"src/test/data/data_with_map_column.parquet\"");
      System.exit(1);
    }

    String filePath = args[0];
    System.out.println("=== Reading Parquet File with Maps ===");
    System.out.println("File: " + filePath);
    System.out.println();
    List<LogicalColumnDescriptor> mapColumns = new ArrayList<>();
    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
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
