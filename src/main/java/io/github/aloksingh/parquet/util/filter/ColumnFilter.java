package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public interface ColumnFilter {

  boolean apply(Object colValue);

  boolean isApplicable(LogicalColumnDescriptor columnDescriptor);

}
