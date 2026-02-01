package org.parquet;

import org.parquet.model.RowColumnGroup;

public interface ParquetWriter extends AutoCloseable {

  void addRow(RowColumnGroup row);

}
