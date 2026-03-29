package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.RowColumnGroup;

public interface RowColumnGroupFilter {
  boolean apply(RowColumnGroup row);
}
