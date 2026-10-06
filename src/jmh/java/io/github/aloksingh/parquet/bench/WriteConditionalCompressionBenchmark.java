package io.github.aloksingh.parquet.bench;

import io.github.aloksingh.parquet.ParquetFileReader;
import io.github.aloksingh.parquet.ParquetFileWriter;
import io.github.aloksingh.parquet.model.CompressionCodec;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Conditional-compression write paths: the writer keeps a compressed page only when it beats
 * {@code minCompressionRatio}; {@code 0.0} disables compression entirely (library semantics),
 * {@code 0.5}/{@code 0.9} demand increasingly strong compression before the compressed form is
 * kept. Crossed with compressible vs incompressible payloads, the {@code outputBytes} counter
 * shows the resulting file size and the score shows the CPU cost of trying (and possibly
 * discarding) compression. Deeper semantics are pinned by ConditionalCompressionTest.
 *
 * <p>Guards: identical to {@link WriteBenchmark} (non-empty output per iteration, exact input
 * row count re-read at trial teardown).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class WriteConditionalCompressionBenchmark {

  @State(Scope.Benchmark)
  public static class WriteCompressionState {
    @Param({"COMPRESSIBLE", "INCOMPRESSIBLE"})
    public String kind;

    @Param({"SNAPPY", "ZSTD"})
    public String codec;

    @Param({"0.0", "0.5", "0.9"})
    public String minRatio;

    Datasets.Generated data;
    CompressionCodec compressionCodec;
    double ratio;

    @Setup(Level.Trial)
    public void setUp() {
      data = Datasets.generate(Datasets.DatasetKind.valueOf(kind));
      compressionCodec = Datasets.codec(codec);
      ratio = Double.parseDouble(minRatio);
    }
  }

  @Benchmark
  public long writeConditionalCompression(WriteCompressionState state, ScanCounters counters,
                                          GuardState guardState, Verify verify) {
    String what = "WriteCond " + state.kind + "/" + state.codec + "/minRatio=" + state.minRatio;
    Path out = output(state);
    try (ParquetFileWriter writer = new ParquetFileWriter(out, state.data.schema,
        state.compressionCodec, Datasets.Layout.SMALL_PAGES.pageBytes,
        Datasets.Layout.SMALL_PAGES.groupBytes, state.ratio)) {
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

  static Path output(WriteCompressionState state) {
    try {
      Files.createDirectories(Datasets.OUTPUT_DIR);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return Datasets.OUTPUT_DIR.resolve(
        "cond_" + state.kind + "_" + state.codec + "_" + state.minRatio + ".parquet");
  }

  /** Write guard: non-empty output per iteration; row count re-read at trial teardown. */
  @State(Scope.Benchmark)
  public static class Verify {
    @TearDown(Level.Iteration)
    public void check(WriteCompressionState state, GuardState guardState) {
      Path out = output(state);
      long size = out.toFile().length();
      String what = "WriteCond " + state.kind + "/" + state.codec + "/minRatio=" + state.minRatio;
      if (size <= 0) {
        throw new AssertionError(what + ": produced file is empty: " + out);
      }
      guardState.assertIterationRows(state.data.expectation.rows, what);
    }

    @TearDown(Level.Trial)
    public void reRead(WriteCompressionState state) {
      Path out = output(state);
      long expected = state.data.expectation.rows;
      String what = "WriteCond " + state.kind + "/" + state.codec + "/minRatio=" + state.minRatio;
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
