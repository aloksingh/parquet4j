package io.github.aloksingh.parquet.bench;

import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ParquetRowIterator;
import io.github.aloksingh.parquet.ReadOptions;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
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
 * Projection: row scan over a subset of logical columns via {@code ReadOptions.project(...)}.
 * "single" reads the first logical column only; "multi" reads the first three (or all of them
 * when the schema has fewer). The guard checksum folds exactly the projected leaves, so a
 * projection that silently reads more (or less) than requested fails the run.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ProjectionBenchmark {

  @State(Scope.Benchmark)
  public static class ProjectionParams {
    @Param({"single", "multi"})
    public String projection;

    String[] names;
    int[] logicalIndexes;
    int[] projectedLeaves;
    long expectedChecksum;

    @Setup(Level.Trial)
    public void setUp(DatasetState state) {
      int logical = state.data.logicalColumns();
      int count = projection.equals("single") ? 1 : Math.min(3, logical);
      names = new String[count];
      logicalIndexes = new int[count];
      List<Integer> leaves = new ArrayList<>();
      for (int j = 0; j < count; j++) {
        names[j] = state.data.schema.getLogicalColumn(j).getName();
        logicalIndexes[j] = j;
        int start = state.data.logicalLeafStart[j];
        int width = state.data.logicalIsMap[j] ? 2 : 1;
        for (int l = 0; l < width; l++) {
          leaves.add(start + l);
        }
      }
      projectedLeaves = leaves.stream().mapToInt(Integer::intValue).toArray();
      expectedChecksum = state.data.expectation.checksumFor(projectedLeaves);
    }
  }

  @Benchmark
  public long projectedRowScan(DatasetState state, ScanCounters counters, GuardState guardState,
                               ProjectionParams projection, Verify verify) {
    Datasets.Expectation expected = state.data.expectation;
    String what = "Projection " + state.kind + "/" + state.codec + "/" + state.layout + "/"
        + projection.projection;
    guardState.beginOp();
    ReadOptions options = ReadOptions.builder().project(projection.names).build();
    try (ParquetFileReader reader = new ParquetFileReader(state.file);
         ParquetRowIterator rows = reader.rowIterator(options)) {
      while (rows.hasNext()) {
        RowColumnGroup row = rows.next();
        for (int j = 0; j < projection.logicalIndexes.length; j++) {
          state.data.feedLogical(guardState.guard, projection.logicalIndexes[j],
              row.getColumnValue(projection.names[j]));
        }
        guardState.opRows++;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    counters.rowsProcessed += guardState.opRows;
    counters.bytesRead += state.fileBytes;
    guardState.endOp(expected.rows, projection.expectedChecksum,
        guardState.guard.fold(projection.projectedLeaves), what);
    return guardState.opRows;
  }

  /** Iteration-level guard accounting over exactly the projected leaves. */
  @State(Scope.Benchmark)
  public static class Verify {
    @TearDown(Level.Iteration)
    public void check(DatasetState state, GuardState guardState, ProjectionParams projection) {
      Datasets.Expectation expected = state.data.expectation;
      guardState.assertIteration(expected.rows, projection.expectedChecksum,
          "Projection " + state.kind + "/" + state.codec + "/" + state.layout + "/"
              + projection.projection);
    }
  }
}
