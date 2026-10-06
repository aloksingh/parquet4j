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
 * Time to first row: open the reader, construct the row iterator and materialize exactly one row.
 * Each operation includes file open, footer/metadata parse and the first page decode - this is
 * the latency an interactive query pays before it sees any data. The guard checks the first
 * row's checksum so a benchmark that "fast-forwards" to a fabricated first row fails.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class FirstRowBenchmark {

  @Benchmark
  public long timeToFirstRow(DatasetState state, ScanCounters counters, GuardState guardState,
                             Verify verify) {
    Datasets.Expectation expected = state.data.expectation;
    String what = "FirstRow " + state.kind + "/" + state.codec + "/" + state.layout;
    guardState.beginOp();
    int logical = state.data.logicalColumns();
    try (ParquetFileReader reader = new ParquetFileReader(state.file);
         ParquetRowIterator rows = reader.rowIterator(ReadOptions.DEFAULT)) {
      if (!rows.hasNext()) {
        throw new AssertionError(what + ": dataset is empty");
      }
      RowColumnGroup row = rows.next();
      for (int j = 0; j < logical; j++) {
        state.data.feedLogical(guardState.guard, j, row.getColumnValue(j));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    counters.rowsProcessed += 1;
    counters.bytesRead += state.fileBytes;
    guardState.opRows = 1;
    guardState.endOp(1, expected.firstRowHash, guardState.guard.fold(expected.leaves), what);
    return 1;
  }

  /** Iteration-level guard accounting on the first row's values. */
  @State(Scope.Benchmark)
  public static class Verify {
    @TearDown(Level.Iteration)
    public void check(DatasetState state, GuardState guardState) {
      guardState.assertIteration(1, state.data.expectation.firstRowHash,
          "FirstRow " + state.kind + "/" + state.codec + "/" + state.layout);
    }
  }
}
