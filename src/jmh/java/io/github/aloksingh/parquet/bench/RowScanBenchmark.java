package io.github.aloksingh.parquet.bench;

import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ParquetRowIterator;
import io.github.aloksingh.parquet.ReadOptions;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Full row scan through the row API ({@code ParquetFileReader.rowIterator()}): opens the file and
 * materializes every value of every column, one {@link RowColumnGroup} row at a time.
 *
 * <p>Compare against {@link ColumnBatchScanBenchmark} (columnar batches) and
 * {@link ColumnValuesScanBenchmark} (boxed column lists) on the same parameter values.
 *
 * <p>Guard: every operation must produce the exact row count and value checksum precomputed from
 * the generated values (see {@link GuardState}).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class RowScanBenchmark {

  @Benchmark
  public long rowIteration(DatasetState state, ScanCounters counters, GuardState guardState,
                           Verify verify) {
    Datasets.Expectation expected = state.data.expectation;
    String what = "RowScan " + state.kind + "/" + state.codec + "/" + state.layout;
    guardState.beginOp();
    int logical = state.data.logicalColumns();
    try (ParquetFileReader reader = new ParquetFileReader(state.file);
         ParquetRowIterator rows = reader.rowIterator(ReadOptions.DEFAULT)) {
      while (rows.hasNext()) {
        RowColumnGroup row = rows.next();
        for (int j = 0; j < logical; j++) {
          state.data.feedLogical(guardState.guard, j, row.getColumnValue(j));
        }
        guardState.opRows++;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    counters.rowsProcessed += guardState.opRows;
    counters.bytesRead += state.fileBytes;
    guardState.endOp(expected.rows, expected.checksum, guardState.guard.fold(expected.leaves), what);
    return guardState.opRows;
  }

  /** Iteration-level guard accounting. */
  @State(Scope.Benchmark)
  public static class Verify {
    @TearDown(Level.Iteration)
    public void check(DatasetState state, GuardState guardState) {
      Datasets.Expectation expected = state.data.expectation;
      guardState.assertIteration(expected.rows, expected.checksum,
          "RowScan " + state.kind + "/" + state.codec + "/" + state.layout);
    }
  }
}
