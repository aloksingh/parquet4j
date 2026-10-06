package io.github.aloksingh.parquet.bench;

import io.github.aloksingh.parquet.model.CompressionCodec;
import java.nio.file.Path;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Shared benchmark state: deterministic dataset plus its written Parquet file.
 *
 * <p>The parameter matrix is the benchmark matrix: schema shapes, nullability/repetition, payload
 * entropy, all codecs and both page/row-group targets. Narrow it per run with {@code -p
 * kind=...}, {@code -p codec=...}, {@code -p layout=...} (comma-separated value lists work).
 */
@State(Scope.Benchmark)
public class DatasetState {

  @Param({"NARROW_REQUIRED", "WIDE_REQUIRED", "NARROW_NULLABLE", "WIDE_NULLABLE",
      "MAP_REPEATED", "STRINGS_HIGHCARD", "COMPRESSIBLE", "INCOMPRESSIBLE"})
  public String kind;

  @Param({"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "LZ4", "LZ4_RAW"})
  public String codec;

  @Param({"SMALL_PAGES", "LARGE_PAGES"})
  public String layout;

  public Datasets.Generated data;
  public Path file;
  public long fileBytes;
  public CompressionCodec compressionCodec;
  public Datasets.Layout pageLayout;

  @Setup(Level.Trial)
  public void setUp() {
    data = Datasets.generate(Datasets.DatasetKind.valueOf(kind));
    compressionCodec = Datasets.codec(codec);
    pageLayout = Datasets.layout(layout);
    file = Datasets.materialize(data, compressionCodec, pageLayout);
    fileBytes = file.toFile().length();
    if (fileBytes <= 0) {
      throw new IllegalStateException("Dataset file is empty: " + file);
    }
    if (System.getProperty("bench.guardSelfTest") != null) {
      // Deliberate corruption used to prove the correctness guards actually trip:
      // run with -f 0 and this flag and the run MUST fail with a checksum mismatch.
      data.expectation.corruptForSelfTest();
    }
  }
}
