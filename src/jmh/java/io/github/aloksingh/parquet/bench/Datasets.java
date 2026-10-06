package io.github.aloksingh.parquet.bench;

import io.github.aloksingh.parquet.ParquetFileWriter;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.PrimitiveLogicalType;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Deterministic benchmark datasets and their precomputed checksum guards.
 *
 * <p>All data is generated from a fixed {@link SplittableRandom} seed so benchmark inputs and the
 * expected row counts / checksums are reproducible across machines and JDK runs. Every expected
 * value is computed from the generated in-memory values, never by reading the written file, so a
 * broken scan cannot validate itself against its own output.
 *
 * <p>Files are written to {@code target/bench-data/} (relative to the working directory of the
 * benchmark JVM). Delete that directory to force clean regeneration.
 */
final class Datasets {

  /** Stable seed for every generated dataset. */
  static final long SEED = 20261005L;

  /** Row-level selectivity percentages for the filter benchmark ({@code pick < selectivity}). */
  static final int[] SELECTIVITIES = {0, 10, 50, 100};

  /** Group-pruning predicate on the FILTERABLE dataset: {@code block <= 49}. */
  static final int PRUNE_BLOCK_MAX = 49;

  static final Path DATA_DIR = Path.of("target", "bench-data");
  static final Path OUTPUT_DIR = DATA_DIR.resolve("out");

  private static final char[] ALPHA_NUM =
      "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  private Datasets() {
  }

  /** Page/row-group byte targets; {@code FILTER} forces many small row groups for pruning. */
  enum Layout {
    SMALL_PAGES(64 * 1024, 4 * 1024 * 1024),
    LARGE_PAGES(1024 * 1024, 128 * 1024 * 1024),
    FILTER(16 * 1024, 64 * 1024);

    final int pageBytes;
    final int groupBytes;

    Layout(int pageBytes, int groupBytes) {
      this.pageBytes = pageBytes;
      this.groupBytes = groupBytes;
    }
  }

  /** The benchmark dataset matrix: schema shapes, nullability, repetition and payload entropy. */
  enum DatasetKind {
    /** 4 required columns: INT64, INT32, DOUBLE, STRING. */
    NARROW_REQUIRED(200_000),
    /** 40 required columns cycling INT64/INT32/DOUBLE/STRING/BOOLEAN. */
    WIDE_REQUIRED(50_000),
    /** 4 columns, all optional with 10-30% nulls. */
    NARROW_NULLABLE(200_000),
    /** 40 optional columns with ~20% nulls. */
    WIDE_NULLABLE(50_000),
    /** INT64 id plus an optional MAP<STRING, STRING> with null/empty maps and null values. */
    MAP_REPEATED(100_000),
    /** High-cardinality strings (near-unique UUID-like and label columns). */
    STRINGS_HIGHCARD(100_000),
    /** Repetitive payload: low-cardinality ints and one of eight fixed strings. */
    COMPRESSIBLE(100_000),
    /** Random payload: full-entropy ints and random alphanumeric strings. */
    INCOMPRESSIBLE(100_000),
    /** id/pick/block/pad for the filter benchmark: multi-row-group, prunable statistics. */
    FILTERABLE(100_000);

    final int rows;

    DatasetKind(int rows) {
      this.rows = rows;
    }
  }

  /** Precomputed guard values: never derived by reading the produced file. */
  static final class Expectation {
    final long rows;
    final int leaves;
    final long[] leafHashes;
    final long firstRowHash;
    /** Filter expectations indexed like {@link #SELECTIVITIES}; only for FILTERABLE. */
    final long[] filterRows;
    final long[] filterChecksums;
    long checksum;
    /**
     * Row groups the filter should prune per selectivity (indexed like {@link #SELECTIVITIES}),
     * calibrated from the written file's column statistics after generation: a group is dropped
     * when any predicate branch is impossible for it ({@code pick} min >= selectivity or
     * {@code block} min > {@link #PRUNE_BLOCK_MAX}).
     */
    int[] expectedDroppedGroups;

    Expectation(long rows, int leaves, long[] leafHashes, long firstRowHash,
                long[] filterRows, long[] filterChecksums) {
      this.rows = rows;
      this.leaves = leaves;
      this.leafHashes = leafHashes;
      this.firstRowHash = firstRowHash;
      this.filterRows = filterRows;
      this.filterChecksums = filterChecksums;
      this.checksum = fold(leafHashes, leaves);
    }

    /**
     * Guard self-test: deliberately corrupts every checksum expectation so the next run MUST
     * fail its correctness guards (enabled with {@code -Dbench.guardSelfTest}).
     */
    void corruptForSelfTest() {
      leafHashes[0] ^= 0x123456789ABCDEFL;
      checksum = fold(leafHashes, leaves);
      for (int s = 0; s < filterChecksums.length; s++) {
        filterChecksums[s] ^= 0x123456789ABCDEFL;
      }
    }

    /** Fold per-leaf hashes in schema leaf order (subset folds for projected scans). */
    long checksumFor(int[] leafIndexes) {
      long f = BenchHash.FOLD_SEED;
      for (int leaf : leafIndexes) {
        f = f * BenchHash.PRIME + leafHashes[leaf];
      }
      return f;
    }

    static long fold(long[] leafHashes, int leaves) {
      long f = BenchHash.FOLD_SEED;
      for (int i = 0; i < leaves; i++) {
        f = f * BenchHash.PRIME + leafHashes[i];
      }
      return f;
    }
  }

  /** Generated dataset: source values, write-ready rows and guard expectations. */
  static final class Generated {
    final DatasetKind kind;
    final SchemaDescriptor schema;
    final Object[][] values;
    final RowColumnGroup[] rows;
    final Expectation expectation;
    /** Logical column index to first physical leaf; maps span two consecutive leaves. */
    final int[] logicalLeafStart;
    final boolean[] logicalIsMap;

    Generated(DatasetKind kind, SchemaDescriptor schema, Object[][] values, RowColumnGroup[] rows,
              Expectation expectation, int[] logicalLeafStart, boolean[] logicalIsMap) {
      this.kind = kind;
      this.schema = schema;
      this.values = values;
      this.rows = rows;
      this.expectation = expectation;
      this.logicalLeafStart = logicalLeafStart;
      this.logicalIsMap = logicalIsMap;
    }

    int logicalColumns() {
      return schema.getNumLogicalColumns();
    }

    /** Feed one logical row value into the per-leaf accumulators (row API parity). */
    void feedLogical(BenchHash.LeafAccumulator acc, int logicalIndex, Object value) {
      Datasets.feedLogical(acc, logicalLeafStart, logicalIsMap, logicalIndex, value);
    }
  }

  /**
   * Feeds one logical row value into the per-leaf accumulators. MAP columns contribute one event
   * per entry on each of their two leaves (key and value in entry order); null and empty maps
   * contribute a single null event per leaf, matching the physical level-event stream.
   */
  static void feedLogical(BenchHash.LeafAccumulator acc, int[] logicalLeafStart,
                          boolean[] logicalIsMap, int logicalIndex, Object value) {
    int leaf = logicalLeafStart[logicalIndex];
    if (logicalIsMap[logicalIndex]) {
      Map<?, ?> map = (Map<?, ?>) value;
      if (map == null || map.isEmpty()) {
        acc.event(leaf, null);
        acc.event(leaf + 1, null);
      } else {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          acc.event(leaf, entry.getKey());
          acc.event(leaf + 1, entry.getValue());
        }
      }
    } else {
      acc.event(leaf, value);
    }
  }

  // ------------------------------------------------------------------ generation

  static Generated generate(DatasetKind kind) {
    SchemaDescriptor schema = schemaFor(kind);
    int logicalCount = schema.getNumLogicalColumns();
    int leaves = schema.getNumColumns();
    int[] logicalLeafStart = new int[logicalCount];
    boolean[] logicalIsMap = new boolean[logicalCount];
    int leaf = 0;
    for (int j = 0; j < logicalCount; j++) {
      LogicalColumnDescriptor column = schema.getLogicalColumn(j);
      logicalLeafStart[j] = leaf;
      logicalIsMap[j] = column.isMap();
      leaf += column.getPhysicalColumns().size();
    }

    SplittableRandom rng = new SplittableRandom(SEED + kind.ordinal() * 7919L);
    int rows = kind.rows;
    Object[][] values = new Object[rows][];
    RowColumnGroup[] rowObjects = new RowColumnGroup[rows];
    BenchHash.LeafAccumulator full = new BenchHash.LeafAccumulator(leaves);
    BenchHash.LeafAccumulator first = new BenchHash.LeafAccumulator(leaves);
    BenchHash.LeafAccumulator[] bySelectivity = new BenchHash.LeafAccumulator[SELECTIVITIES.length];
    long[] filterRows = new long[SELECTIVITIES.length];
    for (int s = 0; s < SELECTIVITIES.length; s++) {
      bySelectivity[s] = new BenchHash.LeafAccumulator(leaves);
    }
    for (int i = 0; i < rows; i++) {
      Object[] row = rowValues(kind, rng, i);
      values[i] = row;
      rowObjects[i] = new SimpleRowColumnGroup(schema, row);
      for (int j = 0; j < logicalCount; j++) {
        feedLogical(full, logicalLeafStart, logicalIsMap, j, row[j]);
        if (i == 0) {
          feedLogical(first, logicalLeafStart, logicalIsMap, j, row[j]);
        }
      }
      if (kind == DatasetKind.FILTERABLE) {
        int pick = (Integer) row[1];
        int block = (Integer) row[2];
        for (int s = 0; s < SELECTIVITIES.length; s++) {
          if (matches(pick, block, SELECTIVITIES[s])) {
            filterRows[s]++;
            for (int j = 0; j < logicalCount; j++) {
              feedLogical(bySelectivity[s], logicalLeafStart, logicalIsMap, j, row[j]);
            }
          }
        }
      }
    }

    long[] filterChecksums = new long[SELECTIVITIES.length];
    for (int s = 0; s < SELECTIVITIES.length; s++) {
      filterChecksums[s] = bySelectivity[s].fold(leaves);
    }
    Expectation expectation = new Expectation(rows, leaves, full.hashes, first.fold(leaves),
        filterRows, filterChecksums);
    return new Generated(kind, schema, values, rowObjects, expectation,
        logicalLeafStart, logicalIsMap);
  }

  /** The filter benchmark predicate: row selectivity plus a group-prunable block bound. */
  static boolean matches(int pick, int block, int selectivity) {
    return pick < selectivity && block <= PRUNE_BLOCK_MAX;
  }

  // ------------------------------------------------------------------ file materialization

  /** Writes the dataset to {@code target/bench-data/} and calibrates pruning expectations. */
  static Path materialize(Generated generated, CompressionCodec codec, Layout layout) {
    try {
      Files.createDirectories(DATA_DIR);
      Path file = DATA_DIR.resolve(generated.kind + "_" + codec + "_" + layout + ".parquet");
      try (ParquetFileWriter writer = new ParquetFileWriter(file, generated.schema, codec,
          layout.pageBytes, layout.groupBytes)) {
        for (RowColumnGroup row : generated.rows) {
          writer.addRow(row);
        }
      }
      if (generated.kind == DatasetKind.FILTERABLE) {
        calibratePruning(generated, file);
      }
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Computes how many row groups the filter should prune for each selectivity from the written
   * file's column statistics — an independent read of the metadata, not the filter's own decision.
   * A group is dropped when any AND branch is impossible: {@code pick < s} with min(pick) >= s,
   * or {@code block <= 49} with min(block) > 49.
   */
  private static void calibratePruning(Generated generated, Path file) throws IOException {
    try (io.github.aloksingh.parquet.ParquetFileReader reader =
             new io.github.aloksingh.parquet.ParquetFileReader(file)) {
      int pickLeaf = generated.logicalLeafStart[1];
      int blockLeaf = generated.logicalLeafStart[2];
      int[] dropped = new int[SELECTIVITIES.length];
      int groups = reader.getNumRowGroups();
      for (int s = 0; s < SELECTIVITIES.length; s++) {
        for (int g = 0; g < groups; g++) {
          var chunks = reader.getMetadata().rowGroups().get(g).columns();
          boolean impossiblePick = minOf(chunks.get(pickLeaf).statistics()) >= SELECTIVITIES[s];
          boolean impossibleBlock = minOf(chunks.get(blockLeaf).statistics()) > PRUNE_BLOCK_MAX;
          if (impossiblePick || impossibleBlock) {
            dropped[s]++;
          }
        }
      }
      generated.expectation.expectedDroppedGroups = dropped;
    }
  }

  /** Little-endian INT32 minimum from raw statistics bytes; Integer.MAX_VALUE when absent. */
  private static int minOf(io.github.aloksingh.parquet.model.ColumnStatistics statistics) {
    if (statistics == null || !statistics.hasMin()) {
      return Integer.MAX_VALUE;
    }
    return java.nio.ByteBuffer.wrap(statistics.min()).order(java.nio.ByteOrder.LITTLE_ENDIAN)
        .getInt();
  }

  // ------------------------------------------------------------------ schemas and values

  static SchemaDescriptor schemaFor(DatasetKind kind) {
    List<LogicalColumnDescriptor> columns = new ArrayList<>();
    switch (kind) {
      case NARROW_REQUIRED -> {
        columns.add(prim("id", Type.INT64, false, false));
        columns.add(prim("val", Type.INT32, false, false));
        columns.add(prim("amount", Type.DOUBLE, false, false));
        columns.add(prim("name", Type.BYTE_ARRAY, false, true));
      }
      case NARROW_NULLABLE -> {
        columns.add(prim("id", Type.INT64, false, false));
        columns.add(prim("val", Type.INT32, true, false));
        columns.add(prim("amount", Type.DOUBLE, true, false));
        columns.add(prim("name", Type.BYTE_ARRAY, true, true));
      }
      case WIDE_REQUIRED, WIDE_NULLABLE -> {
        boolean optional = kind == DatasetKind.WIDE_NULLABLE;
        for (int j = 0; j < 40; j++) {
          switch (j % 5) {
            case 0 -> columns.add(prim(String.format("c%02d", j), Type.INT64, optional, false));
            case 1 -> columns.add(prim(String.format("c%02d", j), Type.INT32, optional, false));
            case 2 -> columns.add(prim(String.format("c%02d", j), Type.DOUBLE, optional, false));
            case 3 -> columns.add(prim(String.format("c%02d", j), Type.BYTE_ARRAY, optional, true));
            default -> columns.add(prim(String.format("c%02d", j), Type.BOOLEAN, optional, false));
          }
        }
      }
      case MAP_REPEATED -> {
        columns.add(prim("id", Type.INT64, false, false));
        columns.add(SchemaDescriptor.createStringMapColumn("tags", true, true));
      }
      case STRINGS_HIGHCARD -> {
        columns.add(prim("id", Type.INT64, false, false));
        columns.add(prim("uuid", Type.BYTE_ARRAY, false, true));
        columns.add(prim("label", Type.BYTE_ARRAY, false, true));
      }
      case COMPRESSIBLE, INCOMPRESSIBLE -> {
        columns.add(prim("id", Type.INT64, false, false));
        columns.add(prim("val", Type.INT32, false, false));
        columns.add(prim("name", Type.BYTE_ARRAY, false, true));
      }
      case FILTERABLE -> {
        columns.add(prim("id", Type.INT64, false, false));
        columns.add(prim("pick", Type.INT32, false, false));
        columns.add(prim("block", Type.INT32, false, false));
        columns.add(prim("pad", Type.BYTE_ARRAY, false, true));
      }
    }
    return SchemaDescriptor.fromLogicalColumns("bench_" + kind.name().toLowerCase(), columns);
  }

  private static LogicalColumnDescriptor prim(String name, Type type, boolean optional,
                                              boolean text) {
    ColumnDescriptor descriptor = text
        ? new ColumnDescriptor(type, new String[] {name}, optional ? 1 : 0, 0, 0,
        PrimitiveLogicalType.string())
        : new ColumnDescriptor(type, new String[] {name}, optional ? 1 : 0, 0, 0);
    return new LogicalColumnDescriptor(name, LogicalType.PRIMITIVE, type, descriptor);
  }

  private static Object[] rowValues(DatasetKind kind, SplittableRandom rng, int row) {
    switch (kind) {
      case NARROW_REQUIRED -> {
        return new Object[] {
            (long) row,
            rng.nextInt(),
            rng.nextDouble(),
            "user-" + row + "-" + randomText(rng, 8)};
      }
      case NARROW_NULLABLE -> {
        return new Object[] {
            (long) row,
            rng.nextInt(100) < 10 ? null : rng.nextInt(),
            rng.nextInt(100) < 20 ? null : rng.nextDouble(),
            rng.nextInt(100) < 30 ? null : "user-" + row + "-" + randomText(rng, 8)};
      }
      case WIDE_REQUIRED, WIDE_NULLABLE -> {
        boolean optional = kind == DatasetKind.WIDE_NULLABLE;
        Object[] values = new Object[40];
        for (int j = 0; j < 40; j++) {
          if (optional && rng.nextInt(100) < 20) {
            values[j] = null;
            continue;
          }
          switch (j % 5) {
            case 0 -> values[j] = rng.nextLong();
            case 1 -> values[j] = rng.nextInt();
            case 2 -> values[j] = rng.nextDouble();
            case 3 -> values[j] = randomText(rng, 12);
            default -> values[j] = rng.nextBoolean();
          }
        }
        return values;
      }
      case MAP_REPEATED -> {
        int roll = rng.nextInt(100);
        Map<String, String> tags;
        if (roll < 20) {
          tags = null;
        } else if (roll < 30) {
          tags = new LinkedHashMap<>();
        } else {
          tags = new LinkedHashMap<>();
          int entries = 1 + rng.nextInt(6);
          for (int k = 0; k < entries; k++) {
            tags.put("k" + k, rng.nextInt(8) == 0 ? null : "v" + rng.nextInt(1000));
          }
        }
        return new Object[] {(long) row, tags};
      }
      case STRINGS_HIGHCARD -> {
        return new Object[] {(long) row, randomText(rng, 24), randomText(rng, 12)};
      }
      case COMPRESSIBLE -> {
        return new Object[] {(long) row, row % 10, "category-" + (row % 8)};
      }
      case INCOMPRESSIBLE -> {
        return new Object[] {(long) row, rng.nextInt(), randomText(rng, 16)};
      }
      case FILTERABLE -> {
        return new Object[] {(long) row, rng.nextInt(100), row / 1000, "pad-" + randomText(rng, 44)};
      }
      default -> throw new IllegalStateException("Unknown kind " + kind);
    }
  }

  private static String randomText(SplittableRandom rng, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(ALPHA_NUM[rng.nextInt(ALPHA_NUM.length)]);
    }
    return sb.toString();
  }

  static CompressionCodec codec(String name) {
    return CompressionCodec.valueOf(name);
  }

  static Layout layout(String name) {
    return Layout.valueOf(name);
  }
}
