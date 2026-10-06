# Benchmark results

**2026-10-05 smoke run — INDICATIVE, NOT STABLE NUMBERS.**

Environment: JDK 26 (OpenJDK 26+35-2893, OpenJDK 64-Bit Server VM), 16 cores, Linux
7.0.0-31-generic, warm-cache (datasets fit in the OS page cache), single-threaded benchmarks.
JMH 1.37. Smoke settings: `-f 1 -wi 2 -i 3` (1 fork, 2x1s warmup, 3x1s measurement) — far
fewer samples than the default 2 forks / 5+5 iterations, which is why several rows carry
tens-of-percent error bars. Every run below passed its checksum guards (see
[README.md](README.md#correctness-guards)).

For publishable numbers run the full command at the bottom and use the default warmup/forks.

## Read access: row API vs columnar batches vs boxed lists (SNAPPY, SMALL_PAGES)

Command:

```bash
java -jar target/benchmarks.jar 'RowScanBenchmark|ColumnBatchScanBenchmark|ColumnValuesScanBenchmark' \
    -f 1 -wi 2 -i 3 -p kind=NARROW_REQUIRED,WIDE_REQUIRED,MAP_REPEATED,INCOMPRESSIBLE \
    -p codec=SNAPPY -p layout=SMALL_PAGES
```

```
Benchmark                                                       (codec)           (kind)     (layout)  Mode  Cnt     Score     Error  Units
ColumnBatchScanBenchmark.columnBatchAccess                       SNAPPY  NARROW_REQUIRED  SMALL_PAGES  avgt    3    17.669 ±   2.660  ms/op
ColumnBatchScanBenchmark.columnBatchAccess                       SNAPPY    WIDE_REQUIRED  SMALL_PAGES  avgt    3    32.214 ±   3.571  ms/op
ColumnBatchScanBenchmark.columnBatchAccess                       SNAPPY     MAP_REPEATED  SMALL_PAGES  avgt    3    34.195 ±   5.818  ms/op
ColumnBatchScanBenchmark.columnBatchAccess                       SNAPPY    INCOMPRESSIBLE  SMALL_PAGES  avgt    3     6.660 ±   1.204  ms/op
ColumnValuesScanBenchmark.columnValuesListAccess                 SNAPPY  NARROW_REQUIRED  SMALL_PAGES  avgt    3    16.693 ±   7.030  ms/op
ColumnValuesScanBenchmark.columnValuesListAccess                 SNAPPY    WIDE_REQUIRED  SMALL_PAGES  avgt    3    33.683 ±  21.433  ms/op
ColumnValuesScanBenchmark.columnValuesListAccess                 SNAPPY     MAP_REPEATED  SMALL_PAGES  avgt    3    22.866 ±  50.743  ms/op
ColumnValuesScanBenchmark.columnValuesListAccess                 SNAPPY    INCOMPRESSIBLE  SMALL_PAGES  avgt    3     6.014 ±   0.490  ms/op
RowScanBenchmark.rowIteration                                    SNAPPY  NARROW_REQUIRED  SMALL_PAGES  avgt    3    81.579 ±  23.338  ms/op
RowScanBenchmark.rowIteration                                    SNAPPY    WIDE_REQUIRED  SMALL_PAGES  avgt    3   251.803 ± 391.276  ms/op
RowScanBenchmark.rowIteration                                    SNAPPY     MAP_REPEATED  SMALL_PAGES  avgt    3    91.209 ±   5.236  ms/op
RowScanBenchmark.rowIteration                                    SNAPPY    INCOMPRESSIBLE  SMALL_PAGES  avgt    3    35.743 ±  41.544  ms/op
```

(secondary counters `rowsProcessed`/`bytesRead`/`outputBytes`/`groupsPruned` omitted here for
brevity; they appear in every run — see README for how to derive rows/s and MB/s from them.)

Derived (rows/s = rows per operation / score in seconds; MB/s = file size / score):

| Scan (SNAPPY, SMALL_PAGES) | Rows    | File size | Row scan                          | Column batch                      | ColumnValues list                 |
|----------------------------|---------|-----------|-----------------------------------|-----------------------------------|-----------------------------------|
| NARROW_REQUIRED            | 200,000 | 6.09 MB   | 81.6 ms = 2.45 M rows/s, 75 MB/s  | 17.7 ms = 11.3 M rows/s, 345 MB/s | 16.7 ms = 12.0 M rows/s, 365 MB/s |
| WIDE_REQUIRED (40 cols)    | 50,000  | 13.74 MB  | 251.8 ms = 0.20 M rows/s, 55 MB/s | 32.2 ms = 1.55 M rows/s, 426 MB/s | 33.7 ms = 1.48 M rows/s, 408 MB/s |
| MAP_REPEATED               | 100,000 | 2.08 MB   | 91.2 ms = 1.10 M rows/s           | 34.2 ms = 2.92 M rows/s           | 22.9 ms = 4.37 M rows/s           |
| INCOMPRESSIBLE             | 100,000 | 2.80 MB   | 35.7 ms = 2.80 M rows/s, 78 MB/s  | 6.7 ms = 15.0 M rows/s, 421 MB/s  | 6.0 ms = 16.6 M rows/s, 466 MB/s  |

Takeaways: columnar access is ~5-10x faster than full row materialization on these shapes;
the wide 40-column row API scan is the worst case; the checksum guards confirm all three
paths return identical values (they share the same expected checksum).

## Codec sweep (RowScan, NARROW_REQUIRED, SMALL_PAGES)

```
Benchmark                                         (codec)           (kind)     (layout)  Mode  Cnt     Score    Error  Units
RowScanBenchmark.rowIteration                UNCOMPRESSED  NARROW_REQUIRED  SMALL_PAGES  avgt    3    81.074 ± 37.244  ms/op
RowScanBenchmark.rowIteration                      SNAPPY  NARROW_REQUIRED  SMALL_PAGES  avgt    3    84.406 ± 20.946  ms/op
RowScanBenchmark.rowIteration                        GZIP  NARROW_REQUIRED  SMALL_PAGES  avgt    3   101.057 ± 13.439  ms/op
RowScanBenchmark.rowIteration                        ZSTD  NARROW_REQUIRED  SMALL_PAGES  avgt    3    93.464 ± 31.252  ms/op
RowScanBenchmark.rowIteration                         LZ4  NARROW_REQUIRED  SMALL_PAGES  avgt    3    92.247 ± 19.458  ms/op
RowScanBenchmark.rowIteration                     LZ4_RAW  NARROW_REQUIRED  SMALL_PAGES  avgt    3    85.630 ±   5.900  ms/op
```

File sizes for the same dataset (bytes):

| Codec        | File size | vs uncompressed |
|--------------|-----------|-----------------|
| UNCOMPRESSED | 8,700,163 | —               |
| SNAPPY       | 6,091,158 | 0.70x           |
| LZ4          | 5,884,068 | 0.68x           |
| LZ4_RAW      | 5,883,284 | 0.68x           |
| GZIP         | 4,625,356 | 0.53x           |
| ZSTD         | 4,535,495 | 0.52x           |

All codecs land within the error bars of each other on this scan; GZIP is the slowest
decompressor here. Do not read fine ordering into smoke-sized error bars.

## Page/row-group targets (RowScan, SNAPPY)

```
Benchmark                                    (codec)           (kind)     (layout)  Mode  Cnt     Score     Error  Units
RowScanBenchmark.rowIteration                 SNAPPY  NARROW_REQUIRED  SMALL_PAGES  avgt    3    84.561 ±  16.743  ms/op
RowScanBenchmark.rowIteration                 SNAPPY  NARROW_REQUIRED  LARGE_PAGES  avgt    3    91.175 ±  81.637  ms/op
RowScanBenchmark.rowIteration                 SNAPPY    WIDE_REQUIRED  SMALL_PAGES  avgt    3   254.157 ± 200.852  ms/op
RowScanBenchmark.rowIteration                 SNAPPY    WIDE_REQUIRED  LARGE_PAGES  avgt    3   258.534 ± 176.619  ms/op
```

`SMALL_PAGES` = 64 KiB pages / 4 MiB row groups (multi-page chunks); `LARGE_PAGES` = 1 MiB /
128 MiB. No significant warm-cache difference on these sizes — a storage-limited regime would
distinguish them more sharply.

## Projection and first-row latency (NARROW_REQUIRED, SNAPPY, SMALL_PAGES)

```
Benchmark                                           (codec)           (kind)     (layout)  (projection)  Mode  Cnt     Score    Error  Units
FirstRowBenchmark.timeToFirstRow                     SNAPPY  NARROW_REQUIRED  SMALL_PAGES           N/A  avgt    3     2.758 ±  2.254  ms/op
ProjectionBenchmark.projectedRowScan                 SNAPPY  NARROW_REQUIRED  SMALL_PAGES        single  avgt    3    25.278 ±  5.220  ms/op
ProjectionBenchmark.projectedRowScan                 SNAPPY  NARROW_REQUIRED  SMALL_PAGES         multi  avgt    3    61.116 ± 50.182  ms/op
```

First row (open + footer parse + iterator + one row) costs ~2.8 ms on a 6 MB file. Single-column
projection scans 200k rows in 25 ms (7.9 M rows/s) vs 81.6 ms for all four columns.

## Selective filters with row-group pruning (FILTERABLE, 100k rows, SNAPPY)

```
Benchmark                                       (pruning)  (selectivity)  Mode  Cnt     Score    Error  Units
FilterScanBenchmark.filteredScan                     true              0  avgt    3     0.265 ±  0.053  ms/op
FilterScanBenchmark.filteredScan                     true             10  avgt    3    20.660 ±  0.830  ms/op
FilterScanBenchmark.filteredScan                     true             50  avgt    3    23.471 ±  6.436  ms/op
FilterScanBenchmark.filteredScan                     true            100  avgt    3    26.845 ± 15.013  ms/op
FilterScanBenchmark.filteredScan                    false              0  avgt    3    39.588 ± 33.093  ms/op
FilterScanBenchmark.filteredScan                    false             10  avgt    3    40.825 ± 12.579  ms/op
FilterScanBenchmark.filteredScan                    false             50  avgt    3    43.269 ±  2.774  ms/op
FilterScanBenchmark.filteredScan                    false            100  avgt    3    45.929 ± 26.158  ms/op
```

Pruning (statistics-based row-group dropping) cuts scan time roughly in half at every
selectivity and collapses the 0% case to 0.27 ms (all row groups are pruned: 104 of 104). At
selectivity 100% pruning still drops 52 of 104 groups via the `block <= 49` branch. The
dropped-group counts are asserted per operation against a calibration read from the file's
column statistics.

## Writes (SMALL_PAGES)

```
Benchmark                                   (codec)        (kind)     (layout)  Mode  Cnt     Score     Error  Units
WriteBenchmark.writeFullFile                 SNAPPY  NARROW_REQUIRED  SMALL_PAGES  avgt    3   184.346 ±  49.167  ms/op
WriteBenchmark.writeFullFile                 SNAPPY     COMPRESSIBLE  SMALL_PAGES  avgt    3    81.047 ± 344.150  ms/op
WriteBenchmark.writeFullFile                 SNAPPY    INCOMPRESSIBLE  SMALL_PAGES  avgt    3    65.571 ±  12.338  ms/op
WriteBenchmark.writeFullFile                   ZSTD  NARROW_REQUIRED  SMALL_PAGES  avgt    3   198.784 ±  95.520  ms/op
WriteBenchmark.writeFullFile                   ZSTD     COMPRESSIBLE  SMALL_PAGES  avgt    3    93.231 ± 577.480  ms/op
WriteBenchmark.writeFullFile                   ZSTD    INCOMPRESSIBLE  SMALL_PAGES  avgt    3    80.197 ± 128.627  ms/op
```

Produced file sizes (output size, from `target/bench-data/out/`):

```
489069   write_COMPRESSIBLE_SNAPPY_SMALL_PAGES.parquet
153115   write_COMPRESSIBLE_ZSTD_SMALL_PAGES.parquet
2804213  write_INCOMPRESSIBLE_SNAPPY_SMALL_PAGES.parquet
1762369  write_INCOMPRESSIBLE_ZSTD_SMALL_PAGES.parquet
6091158  write_NARROW_REQUIRED_SNAPPY_SMALL_PAGES.parquet
4535495  write_NARROW_REQUIRED_ZSTD_SMALL_PAGES.parquet
```

## Conditional compression (write, SMALL_PAGES, SNAPPY)

```
Benchmark                                                                       (codec)          (kind)  (minRatio)  Mode  Cnt     Score     Error  Units
WriteConditionalCompressionBenchmark.writeConditionalCompression                 SNAPPY    COMPRESSIBLE         0.0  avgt    3    91.853 ± 531.631  ms/op
WriteConditionalCompressionBenchmark.writeConditionalCompression                 SNAPPY    COMPRESSIBLE         0.5  avgt    3    87.246 ± 655.631  ms/op
WriteConditionalCompressionBenchmark.writeConditionalCompression                 SNAPPY    COMPRESSIBLE         0.9  avgt    3    99.267 ± 787.379  ms/op
WriteConditionalCompressionBenchmark.writeConditionalCompression                 SNAPPY   INCOMPRESSIBLE         0.0  avgt    3    64.346 ±  65.103  ms/op
WriteConditionalCompressionBenchmark.writeConditionalCompression                 SNAPPY   INCOMPRESSIBLE         0.5  avgt    3    64.651 ±   7.524  ms/op
WriteConditionalCompressionBenchmark.writeConditionalCompression                 SNAPPY   INCOMPRESSIBLE         0.9  avgt    3    69.476 ±  34.718  ms/op
```

Output sizes:

```
2603015  cond_COMPRESSIBLE_SNAPPY_0.0.parquet      (minRatio 0.0 = compression disabled)
888793   cond_COMPRESSIBLE_SNAPPY_0.5.parquet
489069   cond_COMPRESSIBLE_SNAPPY_0.9.parquet
3203939  cond_INCOMPRESSIBLE_SNAPPY_0.0.parquet
3203939  cond_INCOMPRESSIBLE_SNAPPY_0.5.parquet    (Snappy cannot beat 0.5 on random data; page kept raw)
2804213  cond_INCOMPRESSIBLE_SNAPPY_0.9.parquet
```

The conditional-compression behavior is exactly what the thresholds promise: compressible data
shrinks as the threshold tightens; incompressible data is kept raw unless the threshold is
loose. Semantics are pinned by `ConditionalCompressionTest`.

## Allocation profiling (`-prof gc`, RowScan NARROW_REQUIRED/SNAPPY/SMALL_PAGES)

```
                 gc.alloc.rate:      2066.912 MB/sec
                 gc.alloc.rate.norm: 228614342.545 B/op
                 gc.count:           5.000 counts
                 gc.time:            6.000 ms
```

**Allocation per row** = `gc.alloc.rate.norm` / rows per operation =
228,614,342 B/op / 200,000 rows ≈ **1,143 B/row** for the row-API full scan (all values
materialized). `gc.count`/`gc.time` show the GC pressure during the run (5 collections,
6 ms total in this sample). Compare the same metric on `ColumnBatchScanBenchmark` /
`ColumnValuesScanBenchmark` to see the boxing cost of the list accessors.

## JFR capture (FirstRowBenchmark, `-jvmArgsAppend '-XX:StartFlightRecording=...,settings=profile'`)

```
 Version: 2.1
 Chunks: 1
 Start: 2026-10-06 01:04:37 (UTC)
 Duration: 3 s

 Event Type                              Count  Size (bytes)
=============================================================
 jdk.GCPhaseParallel                      3429         84661
 jdk.PromoteObjectInNewPLAB               1409         23797
 jdk.BooleanFlag                          705         21078
 jdk.ObjectAllocationSample               673         10364
 jdk.ModuleExport                         533          5718
 jdk.SystemProcess                        474         47985
```

Analyze with `jfr print --events jdk.ObjectAllocationSample` (allocation hot spots) and
`jfr print --events jdk.ExecutionSample` (CPU hot spots).

## Guard self-test (deliberately corrupted expectations — the run MUST fail)

```
$ java -Dbench.guardSelfTest=true -jar target/benchmarks.jar 'RowScanBenchmark.rowIteration' \
      -f 0 -wi 0 -i 1 -p kind=NARROW_REQUIRED -p codec=SNAPPY -p layout=SMALL_PAGES
java.lang.AssertionError: RowScan NARROW_REQUIRED/SNAPPY/SMALL_PAGES: scan produced rows=200000
    (expected 200000), checksum=363360230752236345 (expected -1817404339651589436)
```

This proves a wrong checksum fails the run rather than posting a fast time. During development
the guards also caught three real bugs (a per-iteration vs per-op accounting error, a first-row
checksum that accidentally folded every row, and a pruning calibration that ignored the
`pick < selectivity` branch) — each failed loudly before any number was recorded.

## Publishable numbers

Do NOT publish the smoke numbers. Run the full configuration on quiet hardware:

```bash
java -jar target/benchmarks.jar \
    'RowScanBenchmark|ColumnBatchScanBenchmark|ColumnValuesScanBenchmark|ProjectionBenchmark|FilterScanBenchmark|FirstRowBenchmark|WriteBenchmark|WriteConditionalCompressionBenchmark' \
    -rf json -rff target/bench-data/jmh-results.json
```

(2 forks, 5x1s warmup, 5x1s measurement per run; the full parameter matrix is hundreds of
runs — hours. Narrow with `-p` for targeted studies.) **No regression thresholds are
configured yet** — thresholds should be established only after this full run has been repeated
enough times to characterize its variance.
