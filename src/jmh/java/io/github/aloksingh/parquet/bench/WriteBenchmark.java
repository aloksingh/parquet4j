package io.github.aloksingh.parquet.bench;

import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ParquetFileWriter;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * Full-file write: encodes every row through {@link ParquetFileWriter} (addRow + close/flush to
 * the footer) to a real file. Covers all codecs, both page/row-group targets, narrow/wide
 * schemas, nullable and repeated (MAP) columns and compressible/incompressible payloads via the
 * shared dataset matrix. The {@code outputBytes} counter reports bytes per operation, i.e. the
 * produced file size for these settings.
 *
 * <p>Guards: the produced file must be non-empty after every iteration (asserted in
 * {@code @TearDown(Level.Iteration)} together with row accounting) and, outside the measured
 * operation at trial teardown, the produced file must re-read with exactly the input row count.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class WriteBenchmark {

  @Benchmark
  public long writeFullFile(DatasetState state, ScanCounters counters, GuardState guardState,
                            Verify verify) {
    String what = "Write " + state.kind + "/" + state.codec + "/" + state.layout;
    Path out = output(state);
    try (ParquetFileWriter writer = new ParquetFileWriter(out, state.data.schema,
        state.compressionCodec, state.pageLayout.pageBytes, state.pageLayout.groupBytes)) {
      for (RowColumnGroup row : state.data.rows) {
        writer.addRow(row);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    long bytes = out.toFile().length();
    if (bytes <= 0) {
      throw new AssertionError(what + ": produced file is empty: " + out);
    }
    counters.outputBytes += bytes;
    counters.rowsProcessed += state.data.rows.length;
    guardState.endOpRows(state.data.rows.length, what);
    return bytes;
  }

  static Path output(DatasetState state) {
    try {
      Files.createDirectories(Datasets.OUTPUT_DIR);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return Datasets.OUTPUT_DIR.resolve(
        "write_" + state.kind + "_" + state.codec + "_" + state.layout + ".parquet");
  }

  /** Write guard: non-empty output per iteration; row count re-read at trial teardown. */
  @State(Scope.Benchmark)
  public static class Verify {
    @TearDown(Level.Iteration)
    public void check(DatasetState state, GuardState guardState) {
      Path out = output(state);
      long size = out.toFile().length();
      String what = "Write " + state.kind + "/" + state.codec + "/" + state.layout;
      if (size <= 0) {
        throw new AssertionError(what + ": produced file is empty: " + out);
      }
      guardState.assertIterationRows(state.data.expectation.rows, what);
    }

    @TearDown(Level.Trial)
    public void reRead(DatasetState state) {
      Path out = output(state);
      long expected = state.data.expectation.rows;
      String what = "Write " + state.kind + "/" + state.codec + "/" + state.layout;
      try (ParquetFileReader reader = new ParquetFileReader(out)) {
        long rows = reader.getTotalRowCount();
        if (rows != expected) {
          throw new AssertionError(what + ": re-read guard failed for file " + out + ": has "
              + rows + " rows (expected " + expected + ")");
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
