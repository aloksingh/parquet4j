package io.github.aloksingh.parquet.util.filter.query;

import io.github.aloksingh.parquet.util.filter.ColumnFilterDescriptor;

public interface QueryParser {

  ColumnFilterDescriptor parse(String expression);

}
