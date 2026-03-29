package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;

public interface ColumnFilter {

  boolean apply(Object colValue);

  boolean isApplicable(LogicalColumnDescriptor columnDescriptor);

  /**
   * return false if the colValue is in the range defined by the statistics object.
   *
   * @param statistics
   * @param colValue
   * @return
   */
  boolean skip(ColumnStatistics statistics, Object colValue);

}
