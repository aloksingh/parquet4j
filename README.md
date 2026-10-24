# parquet4j

A minimal Java implementation of a Parquet file reader and writer. The implementation uses
[arrow-rs/parquet](https://docs.rs/parquet/latest/parquet/) and
[parquet-java](https://github.com/apache/parquet-java) as references. Claude Code was used
extensively to generate encoder/decoder classes and test cases.

## Requirements

- Java 23 or newer
- Maven 3.x

## Overview

The library supports:

- Reading file metadata, schemas, row groups, column chunks, and Data Page V1/V2 pages
- Boxed column values, row iterators, and primitive columnar batches
- Lazy page decoding, column projection, row limits, and bounded row batches
- Typed row filters with statistics-based and Bloom-filter-based row-group pruning
- Optional page-checksum verification and configurable page/header safety limits
- Annotation-aware values for strings, binary data, decimals, dates, times, timestamps,
  signed/unsigned integers, UUIDs, JSON, BSON, and enums
- Reading LIST, MAP, STRUCT, and nested combinations, including containers spanning pages
- Writing required/optional top-level primitive columns and simple top-level MAP columns
- Byte-targeted page and row-group flushing, column statistics, and transactional file publication
- Optional dictionary encoding and per-column, per-row-group Bloom filters when writing
- UNCOMPRESSED, GZIP, SNAPPY, ZSTD, legacy Hadoop-framed LZ4, and LZ4_RAW compression
- JSON export and file-inspection utilities

## Format support

| Feature | Reading | Writing |
| --- | --- | --- |
| Physical types | BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY; INT96 as raw bytes | Same, except INT96 |
| Value encodings | PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, and RLE BOOLEAN | PLAIN; optional RLE_DICTIONARY with PLAIN fallback (except BOOLEAN) |
| Level encodings | RLE and BIT_PACKED | RLE |
| Data pages | V1 and V2 | V2 |
| Nested data | LIST, MAP, STRUCT, repeated fields, and nested combinations | Top-level primitives and canonical top-level MAPs with primitive keys and values |
| Logical annotations | STRING, ENUM, JSON, BSON, DECIMAL, DATE, TIME, TIMESTAMP, INTEGER, and UUID | Preserved for writable columns |
| Statistics | Column statistics and conservative row-group pruning | Page/column statistics and file/row-group totals |
| Bloom filters | Split-block Bloom filters using XXH64; equality-pruning integration | Optional per-column configuration using expected NDV and target FPP |
| Compression | UNCOMPRESSED, GZIP, SNAPPY, ZSTD, LZ4, and LZ4_RAW | Same |
| Page checksums | Optional CRC verification | Not emitted |
| Column/offset indexes | Not supported | Not supported |
| Encryption | Not supported | Not supported |

### Conditional compression

For compressed Data Page V2 output, the writer retains compression only when
`compressedSize / uncompressedSize < minCompressionRatio`. Otherwise, that page's values are
stored uncompressed and its page header records the actual storage choice. The default ratio is
`0.90`; setting it to `0` disables data-page compression. This policy was inspired by the
conditional-compression discussion in [arXiv:2602.17335](https://arxiv.org/abs/2602.17335).

## Usage

### Reading metadata

```java
try (ParquetFileReader reader = new ParquetFileReader("data.parquet")) {
    reader.printMetadata();

    SchemaDescriptor schema = reader.getSchema();
    System.out.println("Number of physical columns: " + schema.getNumColumns());
    System.out.println("Total rows: " + reader.getTotalRowCount());

    ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
}
```

### Reading column data

```java
try (ParquetFileReader reader = new ParquetFileReader("data.parquet")) {
    ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

    ColumnValues boolColumn = rowGroup.readColumn(0);
    List<Boolean> boolValues = boolColumn.decodeAsBoolean();

    ColumnValues intColumn = rowGroup.readColumn(1);
    List<Integer> intValues = intColumn.decodeAsInt32();

    ColumnValues longColumn = rowGroup.readColumn(2);
    List<Long> longValues = longColumn.decodeAsInt64();

    ColumnValues floatColumn = rowGroup.readColumn(3);
    List<Float> floatValues = floatColumn.decodeAsFloat();

    ColumnValues doubleColumn = rowGroup.readColumn(4);
    List<Double> doubleValues = doubleColumn.decodeAsDouble();

    ColumnValues stringColumn = rowGroup.readColumn(5);
    List<String> strings = stringColumn.decodeAsString();
}
```

`decodeAsString()` is intended for STRING-annotated BYTE_ARRAY columns. Unannotated BYTE_ARRAY
and BSON values remain raw bytes; FIXED_LEN_BYTE_ARRAY values retain their fixed-width bytes.

### Reading primitive column batches

```java
try (ParquetFileReader reader = new ParquetFileReader("data.parquet")) {
    ColumnBatch batch = reader.getRowGroup(0).readColumnBatch("id");

    for (int row = 0; row < batch.size(); row++) {
        if (!batch.isNull(row)) {
            long id = batch.getLong(row);
            System.out.println(id);
        }
    }
}
```

Batches own their data and remain valid after the reader closes. Numeric buffers are read-only,
binary values share an offsets/payload representation, and dictionary indexes can remain
unmaterialized until values are requested.

### Projected, filtered, and bounded row scans

```java
try (ParquetFileReader reader = new ParquetFileReader("data.parquet")) {
    ColumnFilter ageFilter = new ColumnFilters().createFilter(
        reader.getSchema(), new BaseQueryParser().parse("age=gte(18)"));
    RowColumnGroupFilter filter = new RowColumnGroupFilterSet(FilterJoinType.All, ageFilter);

    ReadOptions options = ReadOptions.builder()
        .project("id", "name", "age")
        .filter(filter)
        .batchSize(1024)
        .maxBatchBytes(8 * 1024 * 1024)
        .limit(10_000)
        .verifyChecksums(true)
        .build();

    try (ParquetRowIterator rows = reader.rowIterator(options)) {
        while (rows.hasNext()) {
            RowColumnGroup row = rows.next();
            System.out.println(row.getColumnValue("id"));
        }
    }
}
```

Projection uses logical column names. Columns needed only by the predicate are read internally
but omitted from result rows. Statistics and Bloom-filter pruning are enabled by default when a
filter is supplied; use `.pruning(false)` to disable them. `getDroppedRowGroupCount()` reports
how many groups were skipped.

### Writing a file

```java
LogicalColumnDescriptor id = new LogicalColumnDescriptor(
    "id", LogicalType.PRIMITIVE, Type.INT64,
    new ColumnDescriptor(Type.INT64, new String[]{"id"}, 0, 0, 0));
LogicalColumnDescriptor name = new LogicalColumnDescriptor(
    "name", LogicalType.PRIMITIVE, Type.BYTE_ARRAY,
    new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"name"}, 1, 0, 0,
        PrimitiveLogicalType.string()));
SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("example", List.of(id, name));

WriteOptions options = WriteOptions.builder()
    .compressionCodec(CompressionCodec.SNAPPY)
    .pageSize(1024 * 1024)
    .rowGroupSize(128 * 1024 * 1024)
    .minCompressionRatio(0.90)
    .bloomFilter("name", 10_000, 0.01)
    .build();

try (ParquetFileWriter writer = new ParquetFileWriter(
    Path.of("output.parquet"), schema, options, DictionaryOptions.enabled())) {
    writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{1L, "Ada"}));
    writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{2L, null}));
}
```

The writer is single-threaded. Closing it flushes the final row group and footer, then atomically
replaces the destination; a failed write does not publish a partial output file. Dictionary
encoding is disabled by default and can be enabled with `DictionaryOptions.enabled()` or a custom
byte budget.

See [MapColumnExample](src/main/java/io/github/aloksingh/parquet/util/MapColumnExample.java) for
MAP access and [ParquetWriterMapTest](src/test/java/io/github/aloksingh/parquet/ParquetWriterMapTest.java)
for MAP writing.

### Reading Bloom filters directly

```java
try (ParquetFileReader reader = new ParquetFileReader("data.parquet")) {
    SplitBlockBloomFilter bloom = reader.getRowGroup(0).getBloomFilter(0);
    if (bloom != null) {
        boolean mightContain = bloom.mightContain(plainEncodedValueBytes);
    }
}
```

The input to `mightContain` is the PLAIN-encoded value. BYTE_ARRAY input excludes its four-byte
length prefix.

## Current limitations

- The writer does not support LIST, STRUCT, arbitrary nested schemas, repeated primitives, or INT96.
- INT96 is available through low-level/raw physical access, but logical row conversion is unsupported.
- Multi-level repeated leaves are flattened into one per-row item list rather than preserving every
  intermediate nested-list shape.
- DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, and BYTE_STREAM_SPLIT are read-only.
- Column indexes and offset indexes are not read or written.
- Page CRCs can be verified while reading but are not generated while writing.
- LZO and BROTLI compression and Parquet encryption are unsupported.

## Building and testing

```bash
# Compile
mvn compile

# Run tests
mvn test

# Create JAR
mvn package
```

The test suite includes Apache Parquet corpus fixtures and DuckDB interoperability checks.

## Benchmarks

Reproducible JMH benchmarks with checksum-guarded scans live in [benchmarks/](benchmarks/).
Commands, the parameter matrix, guard design, and profiling notes are in
[benchmarks/README.md](benchmarks/README.md); measured numbers are in
[benchmarks/RESULTS.md](benchmarks/RESULTS.md). Benchmarks are built behind the Maven profile
`benchmarks` and do not enter the library's normal dependency graph. **Never build a release with
`-Pbenchmarks`.**

```bash
mvn -Pbenchmarks package -DskipTests
java -jar target/benchmarks.jar -l     # list benchmarks
```

## Dependencies

Runtime dependencies:

- Apache Parquet Format Structures 1.13.1: Parquet Thrift metadata definitions
- Apache Thrift 0.19.0: Thrift metadata support
- Gson 2.11.0: JSON export and inspection utilities
- Snappy Java 1.1.10.5: SNAPPY support
- lz4-java 1.10.3: LZ4 and LZ4_RAW support
- zstd-jni 1.5.7-6: ZSTD support

GZIP uses the JDK and needs no external codec library. The third-party codec dependencies can be
excluded only when the corresponding codecs are not read or written. JUnit 5 and DuckDB JDBC are
test-scoped dependencies.

## Future enhancements

- Preserve fully nested repeated structures without flattening
- Write LIST, STRUCT, and arbitrarily nested schemas
- Add delta and BYTE_STREAM_SPLIT writer encodings
- Read column and offset indexes for page-level predicate pushdown
- Generate page checksums while writing
- Add encryption support

## License

[Apache 2](./LICENSE)

## References

- [Apache Parquet Format Specification](https://github.com/apache/parquet-format)
- [Apache Arrow Rust Parquet Implementation](https://github.com/apache/arrow-rs/tree/master/parquet)
- [Parquet Documentation](https://parquet.apache.org/)
