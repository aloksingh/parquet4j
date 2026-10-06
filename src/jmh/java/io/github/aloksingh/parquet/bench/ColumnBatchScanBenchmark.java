package io.github.aloksingh.parquet.bench;

import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.model.ColumnBatch;
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
 * Columnar batch access: per row group and physical leaf, materializes the whole chunk as one
 * {@link ColumnBatch} ({@code RowGroupReader.readColumnBatch}) and walks its slots. This is the
 * primitive/binary batch path - the fastest way to consume a single column at scale.
 *
 * <p>Guard: every operation must produce the exact row count and value checksum precomputed from
 * the generated values (see {@link GuardState}).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ColumnBatchScanBenchmark {

  @Benchmark
  public long columnBatchAccess(DatasetState state, ScanCounters counters, GuardState guardState,
                                Verify verify) {
    Datasets.Expectation expected = state.data.expectation;
    String what = "ColumnBatchScan " + state.kind + "/" + state.codec + "/" + state.layout;
    guardState.beginOp();
    int leaves = expected.leaves;
    try (ParquetFileReader reader = new ParquetFileReader(state.file)) {
      for (int g = 0; g < reader.getNumRowGroups(); g++) {
        ParquetFileReader.RowGroupReader group = reader.getRowGroup(g);
        long groupRows = group.getNumRows();
        for (int leaf = 0; leaf < leaves; leaf++) {
          ColumnBatch batch = group.readColumnBatch(leaf);
          Events.feedBatch(guardState.guard, leaf, batch);
        }
        guardState.opRows += groupRows;
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
          "ColumnBatchScan " + state.kind + "/" + state.codec + "/" + state.layout);
    }
  }
}
