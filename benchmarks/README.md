# parquet4j benchmarks

Reproducible JMH benchmarks and allocation profiling for parquet4j. The suite measures row
vs. columnar access, projections, selective filters (with row-group pruning), narrow/wide
schemas, nullable and repeated (MAP) columns, high-cardinality strings, compressible vs
incompressible payloads, every codec, and multiple page/row-group targets — with checksum
guards so a broken, empty or partial scan **fails the run** instead of posting a fast time.

Latest measured numbers: [RESULTS.md](RESULTS.md) (smoke run, indicative only).

## Build

Benchmarks live behind the Maven profile `benchmarks` and are **never** part of the library's
runtime dependency graph:

```bash
# Default build — no JMH anywhere, plain jar unchanged:
mvn -B -ntp test
mvn -B -ntp package

# Benchmark build — adds src/jmh/java and produces target/benchmarks.jar:
mvn -B -ntp -Pbenchmarks package -DskipTests
```

> **NEVER build a release with `-Pbenchmarks`.** The `benchmarks` profile exists only to
> compile and shade the benchmark suite. Releases must be built with the default profile so
> the published jar and its dependency graph stay free of JMH (verify with
> `mvn dependency:tree | grep -i jmh` — it must print nothing).

The shaded `target/benchmarks.jar` (Main-Class `org.openjdk.jmh.Main`) bundles JMH plus the
library and its runtime dependencies. JMH `1.37`, wired via `maven-compiler-plugin`'s
`annotationProcessorPaths` (required on JDK 23+, where implicit annotation processing is
disabled).

## Run

```bash
# List all benchmarks
java -jar target/benchmarks.jar -l

# FULL run (recommended for publishable numbers): all benchmarks, default 2 forks,
# 5 x 1s warmup + 5 x 1s measurement each, JSON results for later analysis.
# The complete parameter matrix is large (hundreds of runs, hours); see "Parameter matrix".
java -jar target/benchmarks.jar -rf json -rff target/bench-data/jmh-results.json

# QUICK SMOKE run (minutes, not hours): reduced forks/warmup/iterations, narrowed params.
# Same commands used for RESULTS.md:
java -jar target/benchmarks.jar 'RowScanBenchmark|ColumnBatchScanBenchmark|ColumnValuesScanBenchmark' \
    -f 1 -wi 2 -i 3 -p kind=NARROW_REQUIRED,WIDE_REQUIRED,MAP_REPEATED,INCOMPRESSIBLE \
    -p codec=SNAPPY -p layout=SMALL_PAGES
java -jar target/benchmarks.jar 'RowScanBenchmark' -f 1 -wi 2 -i 3 -p kind=NARROW_REQUIRED \
    -p codec=UNCOMPRESSED,SNAPPY,GZIP,ZSTD,LZ4,LZ4_RAW -p layout=SMALL_PAGES
java -jar target/benchmarks.jar 'RowScanBenchmark' -f 1 -wi 2 -i 3 -p kind=NARROW_REQUIRED,WIDE_REQUIRED \
    -p codec=SNAPPY -p layout=SMALL_PAGES,LARGE_PAGES
java -jar target/benchmarks.jar 'ProjectionBenchmark|FirstRowBenchmark' -f 1 -wi 2 -i 3 \
    -p kind=NARROW_REQUIRED -p codec=SNAPPY -p layout=SMALL_PAGES
java -jar target/benchmarks.jar 'FilterScanBenchmark' -f 1 -wi 2 -i 3
java -jar target/benchmarks.jar 'WriteBenchmark' -f 1 -wi 2 -i 3 \
    -p kind=NARROW_REQUIRED,COMPRESSIBLE,INCOMPRESSIBLE -p codec=SNAPPY,ZSTD -p layout=SMALL_PAGES
java -jar target/benchmarks.jar 'WriteConditionalCompressionBenchmark' -f 1 -wi 2 -i 3 \
    -p kind=COMPRESSIBLE,INCOMPRESSIBLE -p codec=SNAPPY -p minRatio=0.0,0.5,0.9

# Single benchmark by regex (matches the fully qualified method name):
java -jar target/benchmarks.jar 'RowScanBenchmark.rowIteration' -p kind=NARROW_REQUIRED -p codec=ZSTD

# Allocation / GC profiling (-prof gc adds gc.alloc.rate, gc.alloc.rate.norm, gc.count, gc.time):
java -jar target/benchmarks.jar 'RowScanBenchmark' -f 1 -wi 2 -i 3 \
    -p kind=NARROW_REQUIRED -p codec=SNAPPY -p layout=SMALL_PAGES -prof gc

# JFR capture on the forked benchmark JVMs (JDK flight recorder):
java -jar target/benchmarks.jar 'FirstRowBenchmark' -f 1 -wi 1 -i 1 \
    -p kind=NARROW_REQUIRED -p codec=SNAPPY -p layout=SMALL_PAGES \
    -jvmArgsAppend '-XX:StartFlightRecording=filename=/tmp/bench.jfr,settings=profile'
jfr summary /tmp/bench.jfr          # event counts and sizes
jfr print --events jdk.ObjectAllocationSample /tmp/bench.jfr | less   # allocation hot spots
jfr print --events jdk.ExecutionSample /tmp/bench.jfr | less          # CPU hot spots
```

Everything annotated on the benchmark classes (`@Fork(2)`, `@Warmup(iterations=5, time=1)`,
`@Measurement(iterations=5, time=1)`) is overridable from the command line (`-f`, `-wi`,
`-w`, `-i`, `-r`, `-t`, ...), which is what the smoke commands above do.

## Benchmark inventory

| Benchmark                                                          | Measures                                                                                                           | Correctness guard                                                                               |
|--------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `RowScanBenchmark.rowIteration`                                    | Full row scan via `ParquetFileReader.rowIterator()` (row API, all values of all columns)                           | Per-op row count + value checksum vs. generated values                                          |
| `ColumnBatchScanBenchmark.columnBatchAccess`                       | Columnar batch access: `RowGroupReader.readColumnBatch` per physical leaf, walked slot by slot                     | Same row count + checksum (identical across access paths)                                       |
| `ColumnValuesScanBenchmark.columnValuesListAccess`                 | Boxed column-list access: `RowGroupReader.readColumn` + `decodeAs*` lists                                          | Same row count + checksum                                                                       |
| `ProjectionBenchmark.projectedRowScan`                             | Projected row scan via `ReadOptions.project(...)` ("single" = first logical column, "multi" = first three)         | Row count + checksum over **exactly the projected leaves**                                      |
| `FilterScanBenchmark.filteredScan`                                 | Selective filters at 0/10/50/100% row pass rate with row-group pruning ON vs OFF                                   | Matched-row count + checksum; dropped-group count asserted against statistics-based calibration |
| `FirstRowBenchmark.timeToFirstRow`                                 | Time to first row: file open + footer parse + iterator construction + first row materialization                    | First row's value checksum                                                                      |
| `WriteBenchmark.writeFullFile`                                     | Full-file write through `ParquetFileWriter` (addRow + close) across codecs, layouts and schema shapes              | Produced file non-empty every iteration; re-read row count at trial teardown                    |
| `WriteConditionalCompressionBenchmark.writeConditionalCompression` | Conditional-compression write path across `minCompressionRatio` 0.0/0.5/0.9 x compressible/incompressible payloads | Same as `WriteBenchmark`                                                                        |

## Parameter matrix

`DatasetState` parameters (read and write benchmarks):

- `kind` — dataset shape/payload:
    - `NARROW_REQUIRED` (200k rows): INT64, INT32, DOUBLE, STRING, all required.
    - `WIDE_REQUIRED` (50k rows): 40 required columns cycling INT64/INT32/DOUBLE/STRING/BOOLEAN.
    - `NARROW_NULLABLE` (200k rows): 4 optional columns with 10–30% nulls.
    - `WIDE_NULLABLE` (50k rows): 40 optional columns with ~20% nulls.
    - `MAP_REPEATED` (100k rows): INT64 + optional MAP<STRING, STRING> with null maps, empty maps, 1–6 entries and
      occasional null values (repeated leaves).
    - `STRINGS_HIGHCARD` (100k rows): near-unique 24-char and 12-char strings (dictionary-hostile).
    - `COMPRESSIBLE` (100k rows): `val = row % 10`, 8-value string dictionary.
    - `INCOMPRESSIBLE` (100k rows): random ints and 16-char random alphanumeric strings.
- `codec` — `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `ZSTD`, `LZ4`, `LZ4_RAW` (every codec the library supports).
- `layout` — page/row-group byte targets: `SMALL_PAGES` (64 KiB / 4 MiB, forces multi-page chunks) or `LARGE_PAGES` (1
  MiB / 128 MiB).

`FilterScanBenchmark` parameters: `selectivity` (0/10/50/100 — `pick < selectivity`) x
`pruning` (true/false). Its dataset is fixed: `FILTERABLE` (100k rows, ~1k rows per row group)
with `pick` uniform 0..99 and `block = row/1000`; the combined predicate
`pick < selectivity AND block <= 49` lets the `block` branch prune about half the row groups
from statistics (and at selectivity 0 the `pick` branch prunes everything).

`WriteConditionalCompressionBenchmark` parameters: `kind` (`COMPRESSIBLE`/`INCOMPRESSIBLE`) x
`codec` (`SNAPPY`/`ZSTD`) x `minRatio` (`0.0`/`0.5`/`0.9`; note the library reads `0.0` as
"do not compress", semantics covered by `ConditionalCompressionTest`).

`ProjectionBenchmark` adds `projection` (`single`/`multi`).

The full cross-product is large (e.g. read scans: 8 kinds x 6 codecs x 2 layouts = 96 runs
per benchmark). Use `-p name=a,b` value lists to select the slice you care about; the smoke
commands above are a good starting point.

## Correctness guards

Datasets are generated in `@Setup(Level.Trial)` from a **fixed seed**
(`Datasets.SEED = 20261005L`, `SplittableRandom`) and the expected row counts and value
checksums are computed **from the generated in-memory values**, never by reading the written
file. The checksum is a per-physical-leaf rolling hash folded in schema order, so the row API,
`ColumnBatch` access and `ColumnValues` list access all produce identical checksums regardless
of traversal order (see `BenchHash`).

Two guard levels:

1. **Per operation** — every measured operation must reproduce the exact expected row count
   and checksum (`GuardState.endOp`); a mismatch throws immediately and fails the run.
2. **Per iteration** — each benchmark's `@TearDown(Level.Iteration)` asserts iteration-level
   accounting (total rows = operations x expected rows, last checksum exact). Write benchmarks
   also assert the produced file is non-empty per iteration and, in `@TearDown(Level.Trial)`
   outside the measured operation, re-read the produced file and assert its row count equals
   the input row count.

The filter benchmark additionally asserts the exact number of row groups the pruning should
drop, calibrated from the written file's column statistics (independent of the pruning code
path).

**Proving the guards trip** — run with `-Dbench.guardSelfTest` (with `-f 0` so the forked JVM
sees the flag). This deliberately corrupts the expected checksums and the run MUST fail:

```bash
java -Dbench.guardSelfTest=true -jar target/benchmarks.jar 'RowScanBenchmark.rowIteration' \
    -f 0 -wi 0 -i 1 -p kind=NARROW_REQUIRED -p codec=SNAPPY -p layout=SMALL_PAGES
# expected: java.lang.AssertionError: RowScan ...: scan produced rows=200000 (expected 200000),
#           checksum=363360230752236345 (expected -1817404339651589436)
```

Datasets regenerate deterministically on every run (the file appears under
`target/bench-data/`); delete that directory to force a clean regeneration. Deterministic
seeds mean identical inputs and identical expected checksums across machines and JDK runs.

## Reading the results

Primary score is `ms/op` (average time per benchmark operation — one full scan, one full
write, one first row). Secondary metrics come from the instrumented counters in
`ScanCounters` (`AuxCounters.Type.OPERATIONS`, so JMH normalizes them to benchmark time —
the displayed value is **time per unit**):

| Counter         | Displayed value                                        | Derive                                             |
|-----------------|--------------------------------------------------------|----------------------------------------------------|
| `rowsProcessed` | ms per logical row read/written                        | **rows/s** = 1000 / value                          |
| `bytesRead`     | ms per input byte (charged as full file size per scan) | **MB/s** = 1000 / value / 1e6                      |
| `outputBytes`   | ms per output byte (write benchmarks)                  | **output size (bytes/op)** = score / value         |
| `groupsPruned`  | ms per pruned row group                                | groups pruned per op = score / value (read counts) |

Read counts (rows per operation) are also dataset constants (the table above) and appear in
every guard message. Example from RESULTS.md: `RowScan NARROW_REQUIRED/SNAPPY` scores
81.6 ms/op over 200,000 rows → rows/s = 200000 / 0.0816 ≈ 2.45 M rows/s.

**Allocation per row** — run with `-prof gc` and divide `gc.alloc.rate.norm` (bytes per
operation) by the rows per operation. Example from RESULTS.md: 228,614,342 B/op over 200,000
rows ≈ **1,143 B/row** for the row API full scan. `gc.count`/`gc.time` give peak-GC pressure
during the run. For deeper heap profiling capture a JFR recording per the commands above.

**First-row latency** is its own benchmark (`FirstRowBenchmark`, `ms/op` = one
open + first row).

## Warm-cache vs storage-limited runs

All numbers above are **warm-cache**: datasets are read repeatedly and stay in the OS page
cache, so they measure decoding and materialization, not storage. This is the default and the
right setting for comparing access paths and codecs.

For **storage-limited** measurements the OS page cache must be dropped between operations;
this needs root and cannot be automated from inside JMH, so run single-shot style from a
wrapper script:

```bash
# between each single-shot invocation (requires root):
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
java -jar target/benchmarks.jar 'RowScanBenchmark' -f 1 -wi 0 -i 1 -bm ss \
    -p kind=NARROW_REQUIRED -p codec=SNAPPY -p layout=SMALL_PAGES
```

State clearly which regime a set of numbers came from; never mix the two in one comparison.

## Regression thresholds

**No regression thresholds are configured yet, on purpose.** Thresholds should only be added
after full-run repeatability data has been collected on quiet hardware (see RESULTS.md for the
current error bars — smoke settings show several-tens-of-percent variance). Do not select
optimizations (SIMD, parallel decompression, new encodings) on intuition or on smoke numbers
alone; run the relevant benchmark slice with the default 2 forks / 5+5 iterations first.

## parquet-java comparison

Comparative benchmarking against parquet-java is explicitly **out of scope** for this suite:
correctness parity with the wider Parquet ecosystem is already covered by the test suite's
DuckDB cross-reads (see `ParquetInteroperabilityTest`), and a fair comparative benchmark study
is a separate effort (it needs matched codecs, encodings and memory budgets on both sides).
This suite measures parquet4j against itself across its own parameter matrix.

## Hardware / JDK caveats

- Numbers depend heavily on JDK version and CPU. RESULTS.md was produced on JDK 26
  (OpenJDK 26+35-2893), 16 cores, Linux; treat them as **indicative, not stable numbers**.
- JMH blackholes: JDK 26 supports compiler blackholes (auto-detected). Keep the blackhole mode
  consistent when comparing runs (`-Djmh.blackhole.autoDetect=false` disables).
- The checksum guard is always on and its (tiny, per-value) cost is included in every scan
  measurement equally — comparisons between access paths remain apples-to-apples.
- Benchmarks are single-threaded (`@State(Scope.Benchmark)` guard state assumes one thread);
  parallel scan behavior is not covered.
