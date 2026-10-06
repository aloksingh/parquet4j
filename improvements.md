# Ranked improvements for parquet4j

## Scope and ranking

This review covers the reader and writer, page/encoding/compression implementations, schema and row models,
filtering/query APIs, utilities, tests, Maven configuration, and CI. References are repository-relative `path:line`
locations at commit `6a3bb28` on `main`.

Recommendations are ordered by expected overall impact: prevent silent corruption and misleading success first, then
remove large sources of unnecessary work and memory use, then improve maintainability and the public interface.
Correctness fixes come before speculative optimization because a faster incorrect reader or writer is not useful. The
largest performance opportunities are projected/batched reading, real byte-based writer sizing, and safe predicate
pushdown.

Evidence labels:

- **Reproduced:** observed in executed, narrowly scoped Java probes.
- **Static:** confirmed by tracing implementations and their callers; not independently exercised for every variant.
- **Performance opportunity:** avoidable work is identifiable, but throughput/allocation improvements have not been
  benchmarked.

Effort: **S** = localized fix and regression tests; **M** = coordinated changes across several classes; **L** =
substantial pipeline/API work. Some entries have a small immediate safety fix and a larger long-term implementation.

## Verification performed

- `mvn -B -ntp -Djava.io.tmpdir="$TMPDIR" test`: **830 tests, 66 suites, zero failures, errors, or skipped tests**.
- `mvn -B -ntp -Dmaven.compiler.release=21 -Djava.io.tmpdir="$TMPDIR" clean test`: the same **830 tests passed**. This
  establishes Java 21 language/API compilation on the installed JDK 26 and execution on JDK 26, **not** execution on a
  Java 21 runtime.
- `mvn -B -ntp -Dgpg.skip=true -Djava.io.tmpdir="$TMPDIR" verify`: **passed**, including tests and artifact/Javadoc
  generation. Javadoc warnings remain. No release was published or signing credentials used.
- **32 targeted probe scenarios** were executed outside the repository. They exposed defects that the passing baseline
  does not cover. Selected exact outcomes appear below.
- Runtime dependency inspection confirmed that a publishing Maven plugin is currently included in the consumer
  dependency graph.

No production code or test source was changed. Performance speedups are not quantified. An optional PyArrow setup was
not completed because its installation approval timed out; no newly generated files were tested with an external Python
reader. Format claims below are grounded in the Apache specifications/reference implementation, not a claimed external
round trip.

## Priority overview

| Rank | Improvement                                                                 | Main benefit                  | Effort               |
|------|-----------------------------------------------------------------------------|-------------------------------|----------------------|
| 1    | Prevent silent writer corruption and reject invalid rows/schemas            | Correctness, usability        | S–M; full nesting L  |
| 2    | Correct null, level, and cross-page container decoding                      | Correctness, readability      | M–L                  |
| 3    | Bound and validate page parsing/decompression                               | Reliability, interoperability | M                    |
| 4    | Propagate failures and make resource/output lifecycles safe                 | Reliability, usability        | M                    |
| 5    | Make statistics and aggregate metadata trustworthy                          | Correctness, query safety     | M                    |
| 6    | Fix LZ4 wire compatibility and add real LZ4_RAW writing                     | Interoperability, usability   | S–M                  |
| 7    | Correct and bind typed predicate semantics                                  | Correctness, speed            | M                    |
| 8    | Replace weak assertions with meaningful regression/interoperability tests   | Code quality                  | M                    |
| 9    | Add lazy, projected, bounded-batch reading                                  | Speed, memory, usability      | L                    |
| 10   | Honor byte-based page/row-group sizing with column builders                 | Speed, memory, usability      | L                    |
| 11   | Integrate conservative row-group pruning through a bound query plan         | Speed                         | M–L                  |
| 12   | Offer primitive columnar results, not only boxed lists/rows                 | Speed, memory                 | M–L                  |
| 13   | Remove inner-loop allocations, repeated copies, and redundant I/O           | Speed, memory                 | M                    |
| 14   | Preserve logical annotations and align logical/physical row interfaces      | Correctness, usability        | L                    |
| 15   | Add reproducible performance benchmarks and allocation profiling            | Performance discipline        | M                    |
| 16   | Split monolithic decoding/writing into shared, testable components          | Readability, code quality     | M–L                  |
| 17   | Clarify conditional compression and expand encoding choices selectively     | Speed, file size              | S; new encodings M–L |
| 18   | Remove build tooling and unused dependencies from the library graph         | Usability, code quality       | S                    |
| 19   | Make schema identity, ownership, and index caches stable                    | Usability, correctness        | M                    |
| 20   | Provide options/builders, compilable examples, and clear contracts          | Usability, readability        | S–M                  |
| 21   | Pin build tooling, validate an LTS minimum, and align release configuration | Usability, code quality       | S–M                  |
| 22   | Stream JSON export and make CLI output machine-safe                         | Memory, usability             | S–M                  |

## Implementation status (verified 2026-10-04)

Verified against the uncommitted working tree on branch `alok/2026_10_04/improvements` (vs. review commit `6a3bb28`) by
code inspection and full `mvn test` runs. **Final (2026-10-05): the suite reports 1469 tests, zero failures, errors, or
skipped tests, and all of improvements 1-15 are COMPLETE.** Status detail appears under each heading.

| #  | Status   | Remaining work                                                                                                                                              |
|----|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1  | COMPLETE | —                                                                                                                                                           |
| 2  | COMPLETE | —                                                                                                                                                           |
| 3  | COMPLETE | —                                                                                                                                                           |
| 4  | COMPLETE | —                                                                                                                                                           |
| 5  | COMPLETE | —                                                                                                                                                           |
| 6  | COMPLETE | —                                                                                                                                                           |
| 7  | COMPLETE | —                                                                                                                                                           |
| 8  | COMPLETE | —                                                                                                                                                           |
| 9  | COMPLETE | —                                                                                                                                                           |
| 10 | COMPLETE | — (deferred heap/throughput measurements belong to 15)                                                                                                      |
| 11 | COMPLETE | —                                                                                                                                                           |
| 12 | COMPLETE | —                                                                                                                                                           |
| 13 | COMPLETE | —                                                                                                                                                           |
| 14 | COMPLETE | — (multi-level repeated leaves flatten to one per-row item list, baseline-equivalent)                                                                       |
| 15 | COMPLETE | — (regression thresholds intentionally deferred until repeatability data exists; parquet-java comparison explicitly out of scope, see benchmarks/README.md) |

## 1. Prevent silent writer corruption and reject invalid rows/schemas

**Status: COMPLETE (verified 2026-10-04).** Full schema/row validation before any output opens (requiredness, physical
types, numeric ranges, FIXED_LEN lengths, MAP key/value constraints, explicit rejection of LIST/STRUCT/INT96/nested
primitives), PLAIN BOOLEAN bit-packed per the format, single-sourced logical-to-physical mapping, and column-access
errors propagated instead of reinterpreted as nulls. Covered by golden-byte and invalid-input tests (WriterEncodingTest,
WriterValidationTest), which pass.

**Evidence: reproduced and static.** This is the most urgent improvement because normal-looking writes can lose or alter
values:

- PLAIN BOOLEAN encoding writes one byte per value instead of packing bits. Writing
  `[false, true, false, true, false, true, false, true]` read back as eight `false` values.
- A primitive after a MAP is read using a physical index against a logically indexed row. A MAP followed by optional
  INT32 `42` wrote that integer as `null`.
- Row-schema validation checks names/counts, not full type compatibility. A same-name INT64 row containing `4294967296`
  accepted by an INT32 writer read back as `0`.
- A required INT32 accepted `null`, producing a page that underflowed when decoded.
- A LIST-only schema accepted a row but emitted **zero physical columns** while retaining a footer row count of one.
- Static inspection also finds unchecked fixed-length byte-array lengths, unsupported nested primitive schema
  flattening, and invalid MAP inputs/getter failures being converted into missing data. MAP keys must not be null.

**Change:** validate the complete writable schema and row before buffering/opening output; enforce requiredness,
physical types, numeric ranges, fixed lengths, and MAP key/value constraints. Reject unsupported LIST/nested/INT96
writing explicitly until implemented. Derive physical-to-logical mappings once, and bit-pack PLAIN booleans according to
the format. Do not catch column-access errors and reinterpret them as nulls.

**Validate:** add independent golden-byte BOOLEAN checks and primitive–MAP–primitive round trips; assert invalid input
fails with row/column context. Check rejected input does not overwrite an existing destination. Use a reference reader
for supported output schemas.

References: `src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:189-248`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:280-381`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:852-890`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:1047-1174`,
`src/main/java/io/github/aloksingh/parquet/MapColumnWriter.java:52-59`.

## 2. Correct null, level, and cross-page container decoding

**Status: COMPLETE (verified 2026-10-05).** One level-aware V1/V2 decode cursor (ColumnPageDecoder) consumes physical
values only for present definition levels; list/MAP container state is preserved across V1 pages; MAP leaf streams join
by row/level events, not page numbers; V1 levels, dictionary indexes, and BOOLEAN RLE (4-byte length prefix) are framed
separately. Row-level nested containers now fully decode: MAP-of-MAP (nested repetition levels), required MAPs with null
value entries (map_no_value shape), nested Impala struct/list columns, and null-element slots at continuation events and
page boundaries (a null element appends a null entry and keeps the container active); only genuinely ambiguous shapes
are rejected explicitly. Covered by DecodingContainerTest, DecodingNullablePageTest, DecodingFixtureTest,
ColumnPageDecoder*Test, MapTypeTest, NullableImpalaTest, and new NestedContainerDecodingTest (map key/value leaves split
at different V1 page boundaries, null-container/empty-container boundaries).

**Evidence: reproduced.** Decoding rules differ between physical types and page versions:

- Nullable V2 FLOAT and DOUBLE `[1.25, null, 2.5]` each throw `BufferUnderflowException` because values are consumed for
  null positions.
- V2 BYTE_ARRAY with definition levels `[2, 1, 2]` also underflows; any level below the schema maximum can represent
  absence, not just level zero.
- A valid V1 RLE BOOLEAN length prefix is interpreted as a bit width.
- A V1 list continued onto the next page decoded `[[10, 20]]` as `[[10]]`; reconstruction resets the active list at each
  page.
- MAP keys split across two pages with corresponding values in one page throw `IndexOutOfBoundsException`. Independent
  column chunks need not have aligned page boundaries.

**Change:** share a level-aware page cursor across types and V1/V2. Consume one physical value only when its definition
level indicates presence. Preserve repeated/container state across V1 pages and combine MAP leaf streams by row/level
events, not page number. Frame V1 levels, dictionary indexes, and BOOLEAN RLE separately rather than guessing their
prefixes.

**Validate:** cover every supported type/encoding with required, optional, and nested null levels; split lists/maps at
different page boundaries; assert exact nested values and counts. Preserve explicit support/rejection rules rather than
silently dropping unsupported combinations.

References: `src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:586-867`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:870-1141`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:1166`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:1930-1986`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:2173-2190`.

## 3. Bound and validate page parsing/decompression

**Status: COMPLETE (verified 2026-10-04).** Incremental streaming Thrift header reader (PageHeaderInputStream) bounded
by chunk remainder and a configurable byte limit; offset/count/size and level-length validation before allocation;
bounded decompression with exact output-size enforcement (BoundedDecompressor under all codecs); explicit CRC
verification policy (PageReadOptions.verifyChecksums); truncated RLE runs rejected instead of zero-filled; width-32 uses
long masks (RleDecoder/RleEncoder/BitPackedReader). Covered by PageReaderSafety/Size/Metadata/Counts/Checksum/V2Safety,
RleDecoderSafety/RleEncoderSafety, CodecExactSize/CodecGzipSafety tests.

**Evidence: reproduced and static.** `PageReader` tries to parse every header from a fixed 256-byte read. A generated
1,049-byte header with binary statistics failed to parse. A truncated hybrid-RLE bit-packed payload produced eight zeros
rather than an error. GZIP declared as one uncompressed byte returned 64 bytes. The existing corrupt-checksum fixture
was accepted and its four pages read without CRC verification.

The width-32 masks in public RLE encoder/decoder paths also corrupt values: an RLE value `123456789` decoded as `0`, and
a bit-packed encoder input beginning with `1000` emitted a first packed value of `0`. Current small
definition/repetition-level writer calls do not exercise these widths.

**Change:** use an incremental/streaming Thrift header reader with configurable limits, bounded by the column chunk.
Check offset/count arithmetic, nonnegative sizes, complete reads, level lengths, dictionary indexes, and expected output
counts before allocation. Enforce decompressed-size and absolute memory limits; check CRC when present under an explicit
verification policy. Reject truncated RLE runs instead of zero-filling, and handle width 32 without Java's wrapping
`int` shift mask.

**Validate:** test headers above 256 bytes, short reads, truncated/oversized pages, invalid indexes, CRC failures,
decompression-size mismatches, and widths 0–32 against independent encoding bytes. Add malformed-input/fuzz tests with
bounded memory consumption.

References: `src/main/java/io/github/aloksingh/parquet/PageReader.java:103-265`,
`src/main/java/io/github/aloksingh/parquet/FileChunkReader.java:73-97`,
`src/main/java/io/github/aloksingh/parquet/RleDecoder.java:1`,
`src/main/java/io/github/aloksingh/parquet/RleEncoder.java:245-257`,
`src/main/java/io/github/aloksingh/parquet/codec/GzipDecompressor.java:1`.

## 4. Propagate failures and make resource/output lifecycles safe

**Status: COMPLETE (verified 2026-10-05).** Sticky predicate/decode failure propagation distinct from delegate
exhaustion (FilteringParquetRowIterator); deterministic constructor-failure cleanup of owned inputs with an
external-reader ownership contract (ParquetFileReader/FileChunkReader); writer NEW/OPEN/FAILED/CLOSED state machine with
no retry after failure and temp-file publication only after successful footer + close (ParquetFileWriter); cleanup
failures preserved as suppressed exceptions without self-suppression. Propagated errors now carry full context and keep
the original as cause: 'Failed to read row group N in <source>', "Failed to decode column '<path>' of type <T>
in <source>", and 'Failed to evaluate filter <expression> at row <pos> in <source>' (source = file path; expression text
supplied by the bound filters via ColumnFilter.expression()). Covered by WriterTransactionalTest,
FilteringIteratorFailureTest (cause identity + context + stickiness across calls), ReaderErrorContextTest (file identity
on read/decode/malformed-chunk paths), and ReaderResourceTest.

**Evidence: reproduced and static.** A predicate throwing `IllegalStateException` made
`FilteringParquetRowIterator.hasNext()` return `false`: failure became normal EOF. Opening invalid metadata 25 times
increased live file-descriptor count by 25 before GC, showing that constructor-failure cleanup is not deterministic. The
writer truncates its destination before validating its first row and can retry buffered flush work during `close()`
after a prior write failure.

**Change:** propagate decoding/predicate errors with file, row group, column, and expression context; distinguish
delegate exhaustion from predicate-thrown exceptions. Close internally owned input on constructor failure, but never
close externally supplied readers without the agreed ownership contract. Give the writer explicit NEW/OPEN/FAILED/CLOSED
states; failed writes must not be retried or finalized implicitly. Offer temporary-file publication after a successful
footer/close, with an explicit overwrite policy and preservation of the original error.

**Validate:** inject read/write/close failures, throwing predicates, and invalid metadata; assert no success-looking
partial result, duplicate flush, leaked owned descriptor, or replacement of a previous destination. Verify repeated
close and external-reader ownership.

References: `src/main/java/io/github/aloksingh/parquet/FilteringParquetRowIterator.java:81-106`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileReader.java:47-81`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:159-171`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:189-264`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:1183-1236`.

## 5. Make statistics and aggregate metadata trustworthy

**Status: COMPLETE (verified 2026-10-05).** Statistics accumulated from encoded values and level events; NaNs excluded
from bounds with untrustworthy bounds omitted; binary distinct counts by encoded content with a bounded-cost omission
rule; null-only statistics retained; modern min_value/max_value with TYPE_ORDER column_orders and deprecated bounds only
for signed-order types; long aggregate counters with checked arithmetic; total_byte_size/total_compressed_size split
into uncompressed vs compressed totals. Signed-zero bounds now follow parquet.thrift TypeDefinedOrder: a zero min
serializes as -0.0 and a zero max as +0.0 for FLOAT/DOUBLE in both min_value/max_value and deprecated min/max. Covered
by WriterStatisticsTest including a 120-generated-array x 4-type property test asserting every finite present value lies
within emitted bounds, bounds omitted exactly when nothing comparable exists, and the spec sign bits on zero bounds.

**Evidence: reproduced and static.** The BOOLEAN probe emitted `min=true, max=false`. DOUBLE `[1.0, NaN, 2.0]` emitted
`min=max=2.0`, incorrectly excluding an existing finite value. Two equal-content `byte[]` values emitted
`distinct_count=2`. A compressed row group's `total_byte_size` was 40, its compressed total, rather than the
uncompressed column total of 419.

Static inspection also finds lost null-count statistics for all-null columns, MAP statistics computed from payloads
rather than definition-level events, an `int` file row counter, and binary/order metadata that is not consistently
described by `column_orders`.

**Change:** accumulate statistics with encoded values and level events. Exclude NaNs from bounds, handle signed zeros
using the supported Parquet ordering, and omit untrustworthy bounds. Count binary values by content or omit expensive
exact distinct counts. Retain null-only statistics. Emit compatible modern bounds/order metadata and avoid deprecated
bounds with conflicting ordering. Use `long` aggregate counts and separate compressed/uncompressed totals with checked
page limits.

**Validate:** compare metadata with a reference writer for NaNs, zeros, null/empty MAPs, repeated binary values, and
retained compression. Property-check that every finite present value is inside emitted bounds. Test counter boundaries
without creating enormous files. Bad statistics can cause **external** statistics-based readers to discard matches; this
review did not observe such an external query execution.

References: `src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:91`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:258-263`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:384-431`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:494-629`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:1197-1209`.

## 6. Fix LZ4 wire compatibility and add real LZ4_RAW writing

**Status: COMPLETE (verified 2026-10-04).** Unframed LZ4_RAW compressor added and wired into Compressor.create; raw vs
legacy decode paths separated by declared codec (Lz4RawDecompressor = exactly one raw block, LZ4Decompressor = Hadoop
framing only, no heuristic fallback); legacy LZ4 writing now emits real Hadoop BlockCompressorStream framing and is
documented deprecated in favor of LZ4_RAW; private-framing/Hadoop-compat claims removed. Covered by CodecLz4FramingTest
with golden framing bytes and a compression-retained assertion. Note: the legacy non_hadoop_lz4_compressed.parquet
corpus fixture is now rejected by design and must become an explicit expected-failure entry (see 8).

**Evidence: static, specification-backed; unsupported factory case reproduced.** `Lz4Compressor` writes only a
little-endian compressed-length prefix plus a raw block. Hadoop framing writes an uncompressed-length header followed by
big-endian compressed-block lengths. The local decompressor mirrors/heuristically handles framing, so self-round trips
do not establish interoperability. `Compressor.create(LZ4_RAW)` throws despite writable-codec support claims.

**Change:** implement an unframed raw-block `LZ4_RAW` compressor and separate raw versus legacy decoder paths by the
declared codec. Deprecate legacy LZ4 writing as recommended by Parquet, or implement actual Hadoop framing. Remove
claims that the private framing is Hadoop-compatible.

**Validate:** use compressible data and assert the codec was actually retained rather than falling back to UNCOMPRESSED.
Check framing against reference bytes, then cross-read generated legacy/raw files with parquet-java and PyArrow/DuckDB
respectively. Those cross-reader checks remain proposed, not completed here.

References: `src/main/java/io/github/aloksingh/parquet/codec/Lz4Compressor.java:49-68`,
`src/main/java/io/github/aloksingh/parquet/codec/LZ4Decompressor.java:83-121`,
`src/main/java/io/github/aloksingh/parquet/Compressor.java:39-47`. Format
sources: [Parquet compression](https://raw.githubusercontent.com/apache/parquet-format/master/Compression.md), [Hadoop BlockCompressorStream](https://raw.githubusercontent.com/apache/hadoop/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/BlockCompressorStream.java).

## 7. Correct and bind typed predicate semantics

**Status: COMPLETE (verified 2026-10-04).** Column indexes/types/constants bound once before iteration (
TypedColumnFilter, RowColumnGroupFilterSet); unknown/ambiguous columns and incompatible operators rejected; exact
integral conversions via BigDecimal intValueExact/longValueExact (no wrap/truncation); strict boolean parsing;
documented and implemented null-map/absent-key/present-null truth table; delimiter parsing replaced by a
quote/escape-aware, full-input-validated query grammar with neq() and wildcard escaping (BaseQueryParser). Covered by
StrictQueryParserTest and ColumnFilterBindingTest.

**Evidence: reproduced and static.** INT32 query literal `4294967296` converts to `0`; `1.9` converts to `1`. Invalid
boolean text silently becomes `false`. Keyed null-map predicates currently return `isNull=false, isNotNull=true`.
Unknown columns return a null filter. Ordered predicates can turn incompatible comparisons into non-matches, while MAP
predicates repeatedly parse constants against runtime value classes.

**Change:** bind column indexes/types/constants once before iteration and reject unknown/ambiguous columns and
incompatible operators. Use exact integral conversions for a strict API, or retain wider numeric thresholds with
deliberate comparison semantics. Reject invalid booleans. Define a truth table for null maps, absent keys, null values,
equality, and inequality; do not imply SQL semantics unless implemented. Replace delimiter-based query parsing with a
small quote/escape-aware, full-input-validated grammar, including clear handling of `neq` and wildcard escaping.

**Validate:** use descriptors with real physical types, not only null-type test descriptors. Cover fractional/overflow
literals, invalid booleans, primitive/MAP parity, null truth tables, quoted `=`/brackets, escaped quotes, trailing
garbage, and unsupported operators. Profile any gain from removing per-row conversion.

References: `src/main/java/io/github/aloksingh/parquet/util/filter/ColumnFilterHelper.java:23-93`,
`src/main/java/io/github/aloksingh/parquet/util/filter/ColumnFilters.java:10-19`,
`src/main/java/io/github/aloksingh/parquet/util/filter/ColumnIsNullFilter.java:23-34`,
`src/main/java/io/github/aloksingh/parquet/util/filter/ColumnIsNotNullFilter.java:23-34`,
`src/main/java/io/github/aloksingh/parquet/util/filter/query/BaseQueryParser.java:1`.

## 8. Replace weak assertions with meaningful regression/interoperability tests

**Status: COMPLETE (verified 2026-10-05).** All vacuous/swallowing assertions are replaced with exact values, counts,
and bytes: DeltaEncodingTest asserts exact DELTA_BINARY_PACKED values and page encodings (assertTrue(true) and the
swallow-catch loop removed); DataPageV2Test asserts exact per-column values, page counts, and encodings (all four
print-only skips removed); ParquetReaderTest, BooleanReaderTest, and DictionaryEncodingTest assert golden values (
catch-and-print removed); ParquetJsonValidationTest compares every row of all 46 corpus fixtures cell-by-cell exactly (
40% threshold, name-based skips, and silent column skips removed) with self-checking declared tables —
UNSUPPORTED_COLUMNS (INT96/DECIMAL capability gaps with reasons), EXPORT_QUIRKS (NaN golden, empty-list golden, the
concatenated_gzip_members golden typo pinned to the provably correct value) — and explicit expected-rejection entries
for nation.dict-malformed.parquet ('Page body exceeds column chunk boundary') and non_hadoop_lz4_compressed.parquet (
legacy private-framed LZ4 rejected by the codec-separated decoder), each asserting exception type and message. Binary
columns compare by full raw content; a declared column silently disappearing now fails.

**Evidence: static, reinforced by reproduced defects despite the green baseline.** Several filtering tests only assert
inside loops, so an empty result can pass. Some MAP/V2 tests catch unexpected exceptions and print skip messages without
failing or recording JUnit skips. A delta test ends with `assertTrue(true)`. Corpus validation can skip missing/broadly
named columns and compare binary lengths rather than complete content. Existing equality-pruning tests preserve an
inverted contract.

**Change:** assert exact row identifiers, counts, column values, nested structures, and bytes. Let unexpected exceptions
fail; use explicit assumptions only for genuinely unavailable optional inputs. Correct the MAP fixture that populates
the wrong map. Add targeted regressions from this review, independent encoding bytes, property tests, and automated
writer-to-reference-reader/reference-writer-to-reader tests. Keep a fixture-backed capability matrix; replace implicit
omissions with explicit unsupported-feature expectations.

**Validate:** temporarily return no rows or inject a decode failure and confirm relevant tests fail. Check that every
claimed feature has a non-vacuous assertion and that deliberately corrupted input fails under verification mode.

References: `src/test/java/io/github/aloksingh/parquet/FilteringParquetRowIteratorTest.java:198-219`,
`src/test/java/io/github/aloksingh/parquet/FilteringParquetRowIteratorTest.java:519-604`,
`src/test/java/io/github/aloksingh/parquet/DataPageV2Test.java:232-249`,
`src/test/java/io/github/aloksingh/parquet/DeltaEncodingTest.java:182-200`,
`src/test/java/io/github/aloksingh/parquet/ParquetJsonValidationTest.java:209-290`,
`src/test/java/io/github/aloksingh/parquet/util/filter/query/ParquetReaderQueryTest.java:81-90`.

## 9. Add lazy, projected, bounded-batch reading

**Status: COMPLETE (verified 2026-10-05).** Iterator construction is metadata-only (instrumented ReaderStreamingTest);
ReadOptions projection reads only selected physical chunks; the row limit stops further group decoding; page decode is
lazy with per-column caching. batchSize/maxBatchBytes now bound retained memory: rows are materialized in bounded
batches (a lazy page cursor decodes one page at a time and drops consumed pages) and row iteration is an adapter over
batches; batches always end on row boundaries with cursors continuous across seams, so nested lists/maps cannot lose or
duplicate elements at batch boundaries. Predicate-required leaves are merged into the scan (projection union
filter.requiredColumns) but hidden from output rows, which contain exactly the projected logical columns;
ReadOptions.filter() constructs the residual filtering path. Covered by BatchedScanTest (limit/budget decode-bound
compliance via materialized-row and decoded-page counters, nested map/list exactness across batch seams, hidden
predicate columns read but absent from output rows, batched == unbounded full-scan equivalence) and ReaderStreamingTest.

**Evidence: reproduced eager work; performance opportunity.** Constructing the iterator for the 11-column
`alltypes_plain.parquet` fixture made **42 data-read calls and read 5,728 bytes before consuming a row**. Each row group
is read/decoded for every logical column, with whole-column result lists retained. A caller needing one column or one
row still pays for the full first group.

**Change:** expose column projection and a lazy page/batch cursor; do not load data in the iterator constructor. Decode
only projected and predicate-required leaves, retaining the latter internally when they are not output columns. Bound
memory by a configurable batch budget rather than the entire row group. Keep row iteration as a convenient adapter over
batches and preserve nested state correctly.

**Impact:** this can eliminate I/O and decoding outright for wide/partial scans and improve time to first row. Exact
throughput gains depend on workload and storage.

**Validate:** instrument `ChunkReader` and assert unselected chunks are never read, constructor I/O is metadata-only,
and a small limit does not decode the remaining group. Benchmark wide projections, early termination, and large row
groups; compare exact results against the existing full scan.

References: `src/main/java/io/github/aloksingh/parquet/ParquetRowIterator.java:57-113`,
`src/main/java/io/github/aloksingh/parquet/ParquetRowIterator.java:251-290`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileReader.java:152-163`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileReader.java:265`.

## 10. Honor byte-based page/row-group sizing with column builders

**Status: COMPLETE (verified 2026-10-05).** Incremental per-column builders flush pages and row groups by encoded byte
targets (no row-count flush); oversized rows/values handled under defined hard limits; addRow snapshots rows so caller
mutation cannot affect output; conditional compression is chunk-consistent via correct V2 per-page is_compressed flags
and honest codec retention. Mixed-retention coverage added: one chunk containing both retained-compressed and
stored-uncompressed pages (minCompressionRatio 0.5, small page target) asserting per-page flags match actual storage,
chunk codec and totals stay consistent, and rows round-trip exactly, plus large nested MAP rows across mixed pages and
negative/positive codec-retention controls. The stale '1000 rows' Javadoc is replaced with the byte-target contract. (
The Validate bullet's peak-heap/throughput measurements are deferred to improvement 15's benchmark harness.)

**Evidence: reproduced configuration defect; performance opportunity.** `pageSize` and `rowGroupSize` are stored but
unused. A 1,001-row INT32 write using 64-byte targets was byte-identical to one using 1 MiB pages/128 MiB groups. Both
produced groups of 1,000 and one row, with one data page in the first column and a 4,020-byte uncompressed column total
despite the 64-byte page target.

**Change:** incrementally encode into bounded per-column builders, flush multiple pages according to encoded/estimated
bytes, and aggregate pages into byte-sized row groups. Define handling of oversized rows/values and maximum
representable page sizes. Avoid retaining arbitrary mutable row objects until a 1,000-row flush: reused input `[1]`
mutated to `[2]` produced `[2, 2]`, not snapshot-style `[1, 2]`.

**Important:** conditional compression must remain consistent with chunk metadata when introducing multiple V1 pages. Do
not mark one chunk with a codec while storing selected V1 pages uncompressed; make a chunk-wide decision or use correct
V2 per-page flags.

**Validate:** vary the two targets independently, inspect actual boundaries, and cross-read alternating
compressible/incompressible pages and large nested values. Measure peak heap, metadata overhead, file size, and
throughput; larger groups are not universally better.

References: `src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:137-151`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:202-210`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:313-318`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:748-790`.

## 11. Integrate conservative row-group pruning through a bound query plan

**Status: COMPLETE (verified 2026-10-05).** canDrop is defined as "no row can match" over bound constants with
conservative keeps (missing/inconsistent stats, NaNs, unordered/binary types, keyed MAP/repeated leaves), correct AND/OR
composition (AND drops when any child is impossible, OR only when all children are), residual predicates bound to their
target columns only, and the old inverted skip() retained as a corrected deprecated alias. The reader now evaluates
canDrop against each row group's statistics before any chunk read — both plain and filtering iterators share the
loading-path mechanism, ReadOptions.pruning() controls it (default on when a filter is bound), and dropped groups are
counted via getDroppedRowGroupCount(). Covered by RowGroupPruningTest (zero-chunk-read proof with a counting
ChunkReader: dropped groups contribute 0 reads/0 bytes; pruning on/off result equivalence across in-range, out-of-range,
boundary, NaN-stats, all-null, and compound AND/OR cases; dropped-group counter) plus the StatisticsPruningTest and
RowColumnGroupFilterSetTest property checks.

**Evidence: reproduced broken contract and static absence of integration; performance opportunity.** Equality `skip()`
returns true for `5` inside bounds `[1, 10]` and false for `20` outside them. Ordered predicates use similarly
match-possible semantics, while null predicates use can-drop semantics. Compound AND/OR combinations are inconsistent
with safe dropping. Current reader/query paths do **not** invoke this pruning API, so this is not a claim that current
scans actively drop groups incorrectly.

**Change:** define `canDrop` as “no row can match,” using each predicate's bound constant, then evaluate before reading
a group. AND can drop when any child is impossible; OR can drop only when all children are impossible. Missing/unsafe
statistics, unsupported ordering, and NaNs must conservatively keep the group. Evaluate residual predicates only on
bound columns and construct output rows after filtering. Treat page indexes/Bloom filters as a later extension, not a
prerequisite.

**Validate:** property-check `canDrop => no actual row matches`; compare all filtered results with pruning disabled and
instrument skipped groups to prove no chunk reads occurred. Implement only after metadata and predicate corrections
above.

References: `src/main/java/io/github/aloksingh/parquet/util/filter/ColumnEqualFilter.java:92-138`,
`src/main/java/io/github/aloksingh/parquet/util/filter/ColumnGreaterThanFilter.java:73-124`,
`src/main/java/io/github/aloksingh/parquet/util/filter/ColumnFilterSet.java:57-78`,
`src/main/java/io/github/aloksingh/parquet/util/filter/RowColumnGroupFilterSet.java:24-52`,
`src/main/java/io/github/aloksingh/parquet/FilteringParquetRowIterator.java:91-95`.

## 12. Offer primitive columnar results, not only boxed lists/rows

**Status: COMPLETE (verified 2026-10-05).** ColumnBatch typed batches (primitive arrays + validity bitmap; BinaryValues
offsets over one shared read-only payload; dictionary indexes preserved with lazy materialization; copy-on-construct
lifetime) are wired into the reader: RowGroupReader.readColumnBatch(int|String) and readColumnPageBatches(int) return
owning batches decoded lazily per page, alongside ColumnValues.toBatch()/toPageBatches() and an unboxed
decodeRequiredUnboxed() fast path — ColumnValues.decodePhysicalColumn routes required nonrepeated chunks through unboxed
arrays and boxes only at list insert. Covered by BatchEquivalenceTest (explicit encoding x null-pattern matrix — PLAIN
required/sparse/all-null/nested, dictionaries incl. INT96 and nested, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY,
DELTA_BYTE_ARRAY incl. all-null, BYTE_STREAM_SPLIT incl. FLBA, RLE boolean — asserting batch == list == row-API with
raw-byte binary comparison), BatchReaderApiTest (name/path resolution, ownership across reader close, copy-on-construct
isolation, read-only views, dictionary preservation), and BatchFastPathAllocationTest (measured allocated-bytes margin
over the boxed path).

**Evidence: static; performance opportunity.** Low-level decoders already produce primitive arrays, but `ColumnValues`
commonly converts them to `List<Integer/Long/Float/Double>`, and row decoding builds further object lists. Required
columns also go through generalized nullable processing. Binary/string paths allocate per-value arrays and can retain
intermediate representations.

**Change:** add typed batch results using primitive arrays plus a validity bitmap; represent variable-width values with
offsets and a shared byte buffer. Preserve dictionary indexes until callers need materialized values. Add a fast
required/nonrepeated path, and provide existing lists/rows as adapters. Make buffer/view lifetime and copying explicit
so speed does not introduce mutation surprises.

**Validate:** compare batch/list/row results for every encoding and null pattern. Use allocation profiling to
demonstrate fewer objects and lower retained heap; measure column-only scans separately from row consumers.

References: `src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:148`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:398`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:586`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:870`,
`src/main/java/io/github/aloksingh/parquet/ParquetRowIterator.java:251-290`.

## 13. Remove inner-loop allocations, repeated copies, and redundant I/O

**Status: COMPLETE (verified 2026-10-04).** Per-value ByteUtils/ByteBuffer helpers replaced by direct little-endian
writes into reusable buffers (WriterByteBuffer; ByteUtils now test-only), FLOAT/DOUBLE reassembled via
intBitsToFloat/longBitsToDouble instead of per-value wrappers (ByteStreamSplitDecoder), V2 level/payload sections sliced
read-only instead of copied, dictionaries built once per column chunk and cached, bulk run/bit unpacking with checked
tails (RleDecoder/BitPackedReader), positional FileChannel reads into caller-owned buffers with complete-read loops (
FileChunkReader.readInto), and hot-path debug printing removed. Covered by ByteStreamSplitSafetyTest,
ColumnPageDecoderBuffer/DictionaryTest, PageFileChunkReaderTest. (Bounded read-ahead/coalescing appeared only under "
consider" and is not implemented.)

**Evidence: static; performance opportunity.** Numeric writer helpers create per-value `ByteBuffer`/byte arrays via
`ByteUtils`. BYTE_STREAM_SPLIT creates a new four/eight-byte array and wrapper for each value. V2 page reading copies
repetition levels, definition levels, and compressed values into separate buffers. `FileChunkReader` allocates for each
request and serializes shared-position seek/read operations. Nested MAP decoding repeatedly discovers/builds
dictionaries.

**Change:** write little-endian primitives directly into reusable page buffers; use `Float.intBitsToFloat`/
`Double.longBitsToDouble` on assembled bits rather than per-value wrappers. Slice validated V2 buffers instead of
copying sections, cache each dictionary once per column, and use bulk run/bit unpacking with checked tails. Consider
positional `FileChannel` reads into caller-owned buffers, complete-read loops, and bounded read-ahead/coalescing;
preserve the `ChunkReader` abstraction for alternate storage. Remove unconditional hot-path debug printing or put
diagnostics behind a disabled-by-default logger.

**Validate:** preserve independent golden bytes and compare results before/after. Profile allocation, GC,
system/read-call counts, and throughput. Test heap/direct/read-only buffer behavior and concurrency ownership before
introducing shared buffers; memory mapping is an optional measured strategy, not an assumed win.

References: `src/main/java/io/github/aloksingh/parquet/util/ByteUtils.java:8-33`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:926-961`,
`src/main/java/io/github/aloksingh/parquet/ByteStreamSplitDecoder.java:47-106`,
`src/main/java/io/github/aloksingh/parquet/PageReader.java:137-170`,
`src/main/java/io/github/aloksingh/parquet/FileChunkReader.java:73-97`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:1830-1839`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:2263-2335`.

## 14. Preserve logical annotations and align logical/physical row interfaces

**Status: COMPLETE (verified 2026-10-05).** PrimitiveLogicalType annotations (
STRING/ENUM/JSON/BSON/DECIMAL/DATE/TIME/TIMESTAMP/INTEGER/UUID with parameters; modern LogicalType + legacy
ConvertedType) now flow through ParquetMetadataReader into every ColumnDescriptor and are emitted by the writer into
footer SchemaElements. A path-qualified schema tree is reconstructed annotation-first (LIST 3-level plus legacy
2-level/repeated-element variants, MAP 3-level with required key, MAP_KEY_VALUE wrapper, exact key_value/key/value names
only as a last-resort legacy heuristic, otherwise STRUCT), so unannotated MAP-like child names no longer misclassify;
leaf indexes derive centrally from the tree (case-sensitive, duplicate-aware). Row values are annotation-aware: only
STRING/ENUM/JSON decode as UTF-8 text; unannotated BYTE_ARRAY/BSON keep raw bytes (ff0080 round-trips byte-exact);
FIXED_LEN_BYTE_ARRAY returns its fixed bytes; DECIMAL returns exact scaled BigDecimal; INTEGER widens to checked
carriers; INT96 and unsupported conversions throw instead of returning null lists. getColumns()/getColumnCount() now
match logical value count/order, with getPhysicalColumns() for physical leaves. Covered by
LogicalAnnotationReadPathTest (17 tests: binary/string/decimal/timestamp/INT96 fixtures, lists, nested MAPs,
primitive-MAP-primitive alignment, MAP-misclassification guard, index resolution) plus the updated corpus, row, filter,
and writer tests. Multi-level repeated leaves surface as one flattened per-row item list (baseline-equivalent
flattening).

**Evidence: reproduced and static.** Physical descriptors discard STRING/DECIMAL/date/time/unsigned annotations and
their parameters. The row API turns every BYTE_ARRAY into text: raw bytes `ff0080` became UTF-8 replacement bytes
`efbfbd00efbfbd`, although low-level byte access preserved the original. FIXED_LEN_BYTE_ARRAY row access returned null
for a present four-byte value. Schema construction recognizes MAP naming patterns but does not build a complete
annotated LIST/STRUCT tree. Row counts/values are logically indexed while `getColumns()` exposes physical descriptors.

**Change:** preserve primitive annotations separately from container shape, reconstruct a path-qualified schema tree
from modern/legacy annotations, and derive every leaf index centrally. Expose explicit raw physical access and
annotation-aware logical values. Decode only genuine STRING data as UTF-8; preserve binary bytes and decimal
precision/scale/timestamp units. Reject unsupported row conversions instead of returning null. Make row descriptors
match logical value count/order, with separate physical-leaf interfaces.

**Validate:** compare external schemas and exact logical/raw values for binary, string, decimals, timestamps, LISTs,
nested MAPs, and primitive–MAP–primitive rows. Include unannotated structures with MAP-like child names to prevent false
classification.

References: `src/main/java/io/github/aloksingh/parquet/model/ColumnDescriptor.java:19-21`,
`src/main/java/io/github/aloksingh/parquet/ParquetMetadataReader.java:291-299`,
`src/main/java/io/github/aloksingh/parquet/ParquetMetadataReader.java:350-413`,
`src/main/java/io/github/aloksingh/parquet/ParquetRowIterator.java:257-290`,
`src/main/java/io/github/aloksingh/parquet/model/SimpleRowColumnGroup.java:68-69`,
`src/main/java/io/github/aloksingh/parquet/model/SimpleRowColumnGroup.java:103-157`.

## 15. Add reproducible performance benchmarks and allocation profiling

**Status: COMPLETE (verified 2026-10-05).** A JMH 1.37 benchmark suite runs behind a `benchmarks` Maven profile (
src/jmh/java compiled only under -Pbenchmarks; shade builds target/benchmarks.jar with Main-Class org.openjdk.jmh.Main),
so JMH/jopt-simple/commons-math3 never enter the library's dependency graph (verified via dependency:tree) and the
default jar contains no benchmark classes after a clean build. Benchmarks cover row iteration vs columnar-batch vs
boxed-list access, projections, selective filters (0/10/50/100% pass rates x pruning on/off), time-to-first-row, and
full-file writes across all six codecs and conditional-compression thresholds (minRatio 0/0.5/0.9), over 8 dataset
kinds (narrow/wide x required/nullable, MAP-repeated, high-cardinality strings, compressible/incompressible) x 6 codecs
x 2 page/group targets with a fixed seed. Every scan asserts an exact row count plus per-leaf value checksum per op and
per iteration (a guard self-test proves the guard trips on tampered expectations), and write benchmarks assert non-empty
output and re-read row counts at trial teardown. Results report rows/s, MB/s, output size, and allocation per row via
JMH `-prof gc`, with JFR capture commands documented. Commands, matrix, and guard design in benchmarks/README.md; real
smoke-run output in benchmarks/RESULTS.md (clearly labeled indicative, full-run command given for publishable numbers).
Per the Validate bullet: no regression thresholds are added until repeatability is established, and the parquet-java
comparison is explicitly out of scope. Release builds must never use -Pbenchmarks (documented).

**Evidence: static gap.** Fixture correctness tests and small writer examples do not measure production throughput, time
to first row, peak memory, or allocation. There is no JMH benchmark suite in the inspected source tree.

**Change:** add a separate benchmark profile/module so JMH does not become a runtime dependency. Benchmark column versus
row access, projections, selective filters, narrow/wide schemas, nullable/repeated fields, high-cardinality strings,
compressible/incompressible input, multiple page/group targets, and each codec. Compare equivalent correct output
against parquet-java where useful. Use JFR/allocation profiling and report rows/s, MB/s, allocation per row, peak
heap/GC, first-row latency, read counts, and output size; separate warm-cache from storage-limited tests.

**Validate:** checksum/count benchmark output so a broken empty/partial scan cannot look fast. Use warmup/forks and
stable generated seeds, publish commands/results, and add regression thresholds only after establishing repeatability.
Do not select SIMD, parallel decompression, or new encodings on intuition alone.

Starting points: `pom.xml:1`, `src/main/java/io/github/aloksingh/parquet/ParquetRowIterator.java:57`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:189`,
`src/test/java/io/github/aloksingh/parquet/ConditionalCompressionTest.java:1`.

## 16. Split monolithic decoding/writing into shared, testable components

**Evidence: static.** `ColumnValues` is 2,523 lines and repeats V1/V2/type/encoding branches; the null behavior
divergences above demonstrate the maintenance cost. `ParquetFileWriter` combines schema serialization, row validation,
primitive encoding, MAP flattening, compression policy, statistics, I/O, and lifecycle bookkeeping. There are
overlapping nested decoding paths and stale heuristics/debug comments.

**Change:** separate validated page parsing, level streams, value encodings, logical assembly, statistics accumulation,
and file lifecycle. Normalize V1/V2 once, then use specialized typed decoders rather than duplicating whole pipelines.
Give writer page/column builders responsibility for paired values/levels/statistics. Remove obsolete/placeholder APIs or
make unsupported behavior explicit; retain specialized inner loops rather than adding reflection-heavy abstractions. Use
named schema paths/level constants and comments explaining format invariants, not obvious statements.

**Validate:** extract behind current interfaces in small steps after strengthening tests; require identical supported
output and malformed-input behavior. Avoid a large rewrite whose correctness and performance cannot be isolated.

References: `src/main/java/io/github/aloksingh/parquet/model/ColumnValues.java:1`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:1`,
`src/main/java/io/github/aloksingh/parquet/NestedStructureReader.java:1`,
`src/main/java/io/github/aloksingh/parquet/PageReader.java:209-220`.

## 17. Clarify conditional compression and expand encoding choices selectively

**Evidence: static behavior; performance opportunity.** A zero compression threshold still creates/invokes the
compressor, then discards its result. The finite 0–1 range is not validated. The threshold actually limits
compressed/uncompressed size, yet is named a minimum ratio; strict `<` means one does not literally retain every
compressed result. Values are always written PLAIN, even when dictionary or delta encoding could fit a workload better.

**Change:** bypass compressor creation/work when disabled, validate options, and document or rename the ratio contract
without silently breaking callers. Expose explicit codec/policy choices. After benchmarking, add bounded dictionary
encoding with fallback for low-cardinality columns, delta encoding for suitable integer sequences, and BYTE_STREAM_SPLIT
where useful for floats. Sampling compression/encoding decisions is optional; keep metadata and chunk/page semantics
correct.

**Validate:** instrument compressor invocation, test zero/one/boundary/NaN/infinite thresholds, and cover incompressible
data rather than only favorable fixtures. Compare CPU, file size, and external readability for each new policy/encoding.
Coordinate multi-page behavior with recommendation 10.

References: `src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:133-151`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:754-789`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:852`,
`src/test/java/io/github/aloksingh/parquet/ConditionalCompressionTest.java:98-132`.

## 18. Remove build tooling and unused dependencies from the library graph

**Evidence: dependency-tree verification and static imports.** `central-publishing-maven-plugin:0.10.0` is a compile
dependency, pulling Guava, Jackson, Plexus, and commons-io into consumers even though publishing is build-time work.
Source uses `shaded.parquet.org.apache.thrift`, not the separately declared unshaded libthrift API.

**Change:** remove the publishing plugin from `<dependencies>` and keep one deliberate build-plugin version. Remove
unshaded libthrift after testing that the shaded structures runtime suffices. Consider moving JSON/inspection utilities
and optional codecs into separate artifacts only if consumer needs justify that extra packaging complexity.

**Validate:** compare runtime dependency trees and run a minimal downstream reader/writer application. Measure
artifact/classpath changes rather than claim an unmeasured startup or scan-speed gain.

References: `pom.xml:56-60`, `pom.xml:100-104`, `pom.xml:154-160`,
`src/main/java/io/github/aloksingh/parquet/ParquetMetadataReader.java:29-31`.

## 19. Make schema identity, ownership, and index caches stable

**Evidence: reproduced and static.** Independently constructed equal-content physical/logical descriptors compare
unequal; a filter bound to one does not apply to its equivalent counterpart. Schema lists/path arrays and filter lists
can be mutated despite immutable-descriptor expectations, while applicability/index caches assume stable content. Reused
writer rows also demonstrate unclear snapshot versus borrowing semantics.

**Change:** defensively own immutable schema lists/paths and define structural descriptor equality/hash codes, including
array contents and annotation parameters. Resolve names and physical/logical indexes once, with clear
duplicate/case-sensitive rules. Bind filters to stable schema/index identities instead of accidental object identity;
immutable filter sets keep caches valid. Document snapshot semantics, or expose an explicitly borrowed batch/view API
with a lifetime contract.

**Validate:** mutate constructor inputs/accessor results and assert internal schema/cache stability. Test equivalent
independently read schemas, duplicate names, list/MAP reordering, and reusable row buffers. Keep compatibility adapters
where changing descriptor equality/indexing affects public APIs.

References: `src/main/java/io/github/aloksingh/parquet/model/SchemaDescriptor.java:17-69`,
`src/main/java/io/github/aloksingh/parquet/model/ColumnDescriptor.java:19-21`,
`src/main/java/io/github/aloksingh/parquet/model/LogicalColumnDescriptor.java:7-15`,
`src/main/java/io/github/aloksingh/parquet/util/filter/RowColumnGroupFilterSet.java:18-32`,
`src/main/java/io/github/aloksingh/parquet/util/filter/ColumnEqualFilter.java:86-88`.

## 20. Provide options/builders, compilable examples, and clear contracts

**Evidence: static.** The writer Javadoc example calls nonexistent schema/row builders, `ColumnDescriptor.primitive`,
and a nonexistent three-argument constructor. Filtering examples also reference nonexistent APIs. Constructors contain
several positional tuning arguments; codec/type support and ownership are not easy to discover. The writer interface
requests thread safety or an explicit guarantee, but the mutable implementation does not provide a clear single-threaded
contract. `created_by` names `java-parquet-rs` rather than this project.

**Change:** fix examples to use real APIs immediately; optionally add validated reader/writer options and builders.
Document Maven coordinates, prerequisites, writable/readable feature matrices, raw versus logical values, null behavior,
projection/filter examples, thread confinement, and input/output ownership. Make iterator close/EOF behavior match
`closeOnComplete` wording. Narrow interface close exceptions deliberately and set accurate writer/version metadata.

**Validate:** compile/run documentation snippets in CI and test every advertised factory/type/codec. Inspect generated
metadata and exercise ownership/close cases rather than relying on prose. Favor short examples and honest capability
limits over broad unsupported claims.

References: `src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:48-73`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:102-139`,
`src/main/java/io/github/aloksingh/parquet/ParquetFileWriter.java:1209`,
`src/main/java/io/github/aloksingh/parquet/ParquetWriter.java:12-38`,
`src/main/java/io/github/aloksingh/parquet/FilteringParquetRowIterator.java:24-31`, `README.md:35-115`.

## 21. Pin build tooling, validate an LTS minimum, and align release configuration

**Evidence: static configuration plus executed compilation/verification.** The project specifies release 23 without
prominently documenting prerequisites. Compiler/Surefire versions depend on Maven defaults. Java 21 language/API
compilation and all tests passed using JDK 26, making an LTS minimum worth evaluating; an actual JDK 21 runtime remains
untested. Current CI stops at `test`, while credential-free `verify` succeeded locally with Javadoc warnings. Publishing
configuration uses differing server IDs/targets across setup-java, distribution management, and the Central extension.

**Change:** pin compiler/Surefire and a consistent Maven version/wrapper; fail on zero tests and retain reports.
Validate the chosen minimum on its actual runtime plus newer JDKs before lowering release. Add credential-free
packaging/Javadoc verification and focused static-analysis/style gates. Separate release targets/profiles, align
authentication IDs, validate tag/version agreement, and configure signing securely only in release jobs.

**Validate:** run the supported JDK matrix and a downstream consumer; check packaging without credentials. Validate
release configuration/staging only with explicit authorization. This review confirms configuration inconsistencies, not
an observed failed or successful remote deployment. Do not misdiagnose the working JUnit baseline as “tests never
execute.”

References: `pom.xml:28-33`, `pom.xml:82-91`, `pom.xml:107-169`, `.github/workflows/maven-ci.yml:18-31`,
`.github/workflows/maven-publish.yml:20-34`.

## 22. Stream JSON export and make CLI output machine-safe

**Evidence: static.** `extractRows()` builds a complete `JsonArray`, adding another full-file object graph on top of
eager row-group decoding. The CLI prints a success message to stdout even when stdout is the JSON destination. Its owned
file writer is not explicitly closed, and all `byte[]` values are assumed UTF-8 text.

**Change:** use Gson's streaming writer for rows while preserving the metadata envelope, and optionally offer JSON Lines
and a preview/row limit. Send diagnostics/status to stderr; reserve stdout for data. Close owned files using
try-with-resources with explicit UTF-8, while leaving caller-supplied writers under their ownership contract. Encode
arbitrary binary as documented Base64/hex rather than lossy text; use logical annotations for genuine strings.

**Validate:** parse stdout as a single complete JSON document with no surrounding messages, compare streamed output to
expected nested values, verify exact binary round trips, and export a large file under a constrained heap. Metadata-only
operations should avoid constructing a row iterator.

References: `src/main/java/io/github/aloksingh/parquet/util/ParquetToJsonConverter.java:228-252`,
`src/main/java/io/github/aloksingh/parquet/util/ParquetToJsonConverter.java:266-305`,
`src/main/java/io/github/aloksingh/parquet/util/ParquetToJsonConverter.java:317-341`.

## Suggested starting point

Start with the localized safety/regression work in 1–8, particularly BOOLEAN packing, logical index mapping, nullable V2
decoding, failure propagation, NaN/statistics totals, and assertions that cannot pass with zero rows. Then establish the
benchmark harness before undertaking the larger read/write pipeline work in 9–13. Dependency cleanup and accurate
documentation are low-cost improvements that can proceed independently. Do not enable pruning until its contracts and
metadata are corrected.

## Review evidence and format references

Temporary execution artifacts are under `/home/alok/.hermes/cache/scratch/parquet4j-review/` and may be pruned by the
environment. They are not committed project files:

- `baseline-mvn-test.log`, `release21-mvn-test.log`, `mvn-verify.log`
- `dependency-tree.log`
- `ReviewProbes.java` / `probe-results.json`
- `ReviewEdgeProbes.java` / `edge-probe-results.json`
- `ReviewFinalProbes.java` / `final-probe-results.json`

The probes record expected versus observed behavior; their successful process exits mean the investigation ran, **not**
that the deliberately exposed library defects passed regression assertions. Convert the cases into failing project tests
before implementing fixes.

Authoritative format references used to check encoding, framing, logical types, and metadata contracts:

- [Apache Parquet encodings](https://raw.githubusercontent.com/apache/parquet-format/master/Encodings.md)
- [Apache Parquet compression](https://raw.githubusercontent.com/apache/parquet-format/master/Compression.md)
- [Apache Parquet Thrift definitions](https://raw.githubusercontent.com/apache/parquet-format/master/src/main/thrift/parquet.thrift)
- [Apache Parquet logical types](https://raw.githubusercontent.com/apache/parquet-format/master/LogicalTypes.md)
- [Hadoop block compression framing](https://raw.githubusercontent.com/apache/hadoop/trunk/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/BlockCompressorStream.java)
