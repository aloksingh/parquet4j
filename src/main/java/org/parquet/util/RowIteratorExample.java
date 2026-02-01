package org.parquet.util;

import java.io.IOException;
import org.parquet.SerializedFileReader;
import org.parquet.RowColumnGroupIterator;
import org.parquet.model.RowColumnGroup;

/**
 * Example demonstrating how to use the row-by-row iterator for Parquet files
 */
public class RowIteratorExample {

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("Usage: java RowIteratorExample <parquet-file>");
      System.exit(1);
    }

    String filePath = args[0];

    // Example 1: Basic iteration using try-with-resources
    System.out.println("=== Example 1: Basic Row Iteration ===");
    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext() && rowCount < 10) {
        RowColumnGroup row = iterator.next();
        System.out.println("Row " + rowCount + ": " + row);
        rowCount++;
      }

      System.out.println("Total rows in file: " + reader.getTotalRowCount());
    }

    // Example 2: Accessing specific columns
    System.out.println("\n=== Example 2: Accessing Specific Columns ===");
    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      while (iterator.hasNext() && rowCount < 5) {
        RowColumnGroup row = iterator.next();

        System.out.print("Row " + rowCount + ": ");
        for (int i = 0; i < row.getColumnCount(); i++) {
          Object value = row.getColumnValue(i);
          System.out.print(row.getColumns().get(i).getPathString() + "=" + value);
          if (i < row.getColumnCount() - 1) {
            System.out.print(", ");
          }
        }
        System.out.println();
        rowCount++;
      }
    }

    // Example 3: Enhanced for-each style iteration
    System.out.println("\n=== Example 3: Enhanced For-Each Iteration ===");
    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      int rowCount = 0;
      for (RowColumnGroup row : (Iterable<RowColumnGroup>) () -> iterator) {
        if (rowCount >= 5) {
          break;
        }
        System.out.println("Row " + rowCount + ": " + row);
        rowCount++;
      }
    }

    // Example 4: Accessing columns by name
    System.out.println("\n=== Example 4: Accessing Columns by Name ===");
    try (SerializedFileReader reader = new SerializedFileReader(filePath)) {
      RowColumnGroupIterator iterator = reader.rowIterator();

      if (iterator.hasNext()) {
        RowColumnGroup firstRow = iterator.next();
        System.out.println("Available columns:");
        for (int i = 0; i < firstRow.getColumnCount(); i++) {
          System.out.println("  " + i + ": " +
              firstRow.getColumns().get(i).getPathString() +
              " (" + firstRow.getColumns().get(i).physicalType() + ")");
        }

        // Access first column by name
        if (firstRow.getColumnCount() > 0) {
          String firstColumnName = firstRow.getColumns().get(0).getPathString();
          Object value = firstRow.getColumnValue(firstColumnName);
          System.out.println("\nFirst row, first column (" + firstColumnName + "): " + value);
        }
      }
    }
  }
}
