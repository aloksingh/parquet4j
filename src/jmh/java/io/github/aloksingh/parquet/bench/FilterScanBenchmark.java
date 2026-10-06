package io.github.aloksingh.parquet.bench;

import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ParquetRowIterator;
import io.github.aloksingh.parquet.ReadOptions;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.util.filter.ColumnFilter;
import io.github.aloksingh.parquet.util.filter.ColumnFilters;
import io.github.aloksingh.parquet.util.filter.FilterJoinType;
import io.github.aloksingh.parquet.util.filter.FilterOperator;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilterSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Selective filter scans with row-group pruning ON vs OFF over a multi-row-group file.
 *
 * <p>Dataset: FILTERABLE (100k rows, ~1k rows per row group). The predicate is
 * {@code pick < selectivity AND block <= 49}: {@code pick} is uniform 0..99 so the selectivity
 * parameter drives the row-level pass rate (0/10/50/100% within surviving groups), while
 * {@code block} (row / 1000) has tight per-group statistics so the {@code block <= 49} branch
 * can drop roughly the second half of the row groups outright when pruning is enabled.
 *
 * <p>Guards: matched rows and their checksums are precomputed from the generated values; the
 * number of prunable groups is calibrated from the written file's column statistics
 * (independent of the pruning code path) and asserted per operation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class FilterScanBenchmark {

  @State(Scope.Benchmark)
  public static class FilterState {
    @Param({"0", "10", "50", "100"})
    public int selectivity;

    @Param({"true", "false"})
    public boolean pruning;

    Datasets.Generated data;
    Path file;
    long fileBytes;
    RowColumnGroupFilterSet filter;
    int selectivityIndex;

    @Setup(Level.Trial)
    public void setUp() {
      data = Datasets.generate(Datasets.DatasetKind.FILTERABLE);
      file = Datasets.materialize(data, CompressionCodec.SNAPPY, Datasets.Layout.FILTER);
      fileBytes = file.toFile().length();
      selectivityIndex = 0;
      for (int i = 0; i < Datasets.SELECTIVITIES.length; i++) {
        if (Datasets.SELECTIVITIES[i] == selectivity) {
          selectivityIndex = i;
        }
      }
      if (data.expectation.expectedDroppedGroups[selectivityIndex] <= 0) {
        throw new IllegalStateException(
            "FILTERABLE dataset must expose prunable row groups for selectivity=" + selectivity
                + "; got expectedDroppedGroups="
                + data.expectation.expectedDroppedGroups[selectivityIndex]);
      }
      ColumnFilters factory = new ColumnFilters();
      ColumnFilter pick = factory.createFilter(data.schema.getLogicalColumn("pick"),
          FilterOperator.lt, String.valueOf(selectivity));
      ColumnFilter block = factory.createFilter(data.schema.getLogicalColumn("block"),
          FilterOperator.lte, String.valueOf(Datasets.PRUNE_BLOCK_MAX));
      filter = new RowColumnGroupFilterSet(FilterJoinType.All, pick, block);
    }
  }

  @Benchmark
  public long filteredScan(FilterState state, ScanCounters counters, GuardState guardState,
                           Verify verify) {
    Datasets.Expectation expected = state.data.expectation;
    int s = state.selectivityIndex;
    String what = "FilterScan selectivity=" + state.selectivity + " pruning=" + state.pruning;
    guardState.beginOp();
    int logical = state.data.logicalColumns();
    ReadOptions options = ReadOptions.builder()
        .filter(state.filter)
        .pruning(state.pruning)
        .build();
    try (ParquetFileReader reader = new ParquetFileReader(state.file);
         ParquetRowIterator rows = reader.rowIterator(options)) {
      while (rows.hasNext()) {
        RowColumnGroup row = rows.next();
        for (int j = 0; j < logical; j++) {
          state.data.feedLogical(guardState.guard, j, row.getColumnValue(j));
        }
        guardState.opRows++;
      }
      long dropped = rows.getDroppedRowGroupCount();
      long expectedDropped = state.pruning ? expected.expectedDroppedGroups[s] : 0;
      if (dropped != expectedDropped) {
        throw new AssertionError(what + ": dropped " + dropped + " row groups (expected "
            + expectedDropped + ")");
      }
      counters.groupsPruned += dropped;
      guardState.guardGroups += dropped;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    counters.rowsProcessed += guardState.opRows;
    counters.bytesRead += state.fileBytes;
    guardState.endOp(expected.filterRows[s], expected.filterChecksums[s],
        guardState.guard.fold(expected.leaves), what);
    return guardState.opRows;
  }

  /** Iteration-level guard accounting on matched rows plus pruning effectiveness. */
  @State(Scope.Benchmark)
  public static class Verify {
    @TearDown(Level.Iteration)
    public void check(FilterState state, GuardState guardState) {
      Datasets.Expectation expected = state.data.expectation;
      int s = state.selectivityIndex;
      String what = "FilterScan selectivity=" + state.selectivity + " pruning=" + state.pruning;
      guardState.assertIteration(expected.filterRows[s], expected.filterChecksums[s], what);
      long expectedDropped = state.pruning ? expected.expectedDroppedGroups[s] : 0;
      if (guardState.guardGroups != guardState.ops * expectedDropped) {
        throw new AssertionError(what + ": iteration pruning accounting failed: dropped groups="
            + guardState.guardGroups + " (expected " + guardState.ops + " x " + expectedDropped
            + ")");
      }
    }
  }
}
