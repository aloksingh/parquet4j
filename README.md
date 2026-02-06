# A minimal Java Parquet Reader/Writer Implementation

A Java implementation of a Parquet file reader, with very minimal set of dependencies. 
This implementation used [rust parquet](https://docs.rs/parquet/latest/parquet/)
and [parquet-java](https://github.com/apache/parquet-java) projects
for references.
Claude code was used extensively to generate majority of the implementation code and test cases. 

## Overview

The library provides a simple way to

- Read and parse Parquet file metadata and data
- Decode Parquet file schemas
- Read row groups and column chunks
- Access data using column indices or row based iterators
- Decode PLAIN-encoded data for basic types (INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY)
- Basic support for simple Map<key, value> columns
- GZIP support works without any external dependencies
- SNAPPY, ZSTD, LZ4 codecs are supported using third-party libraries.

## Current Limitations

- INT96 (can be read as raw bytes)
- FIXED_LEN_BYTE_ARRAY (not yet fully tested)
- Nested lists (max_repetition_level > 1) require additional handling
- Deeply nested structures (nested lists of lists, etc.)
- Column indexes and offset indexes
- Bloom filters
- Encryption support

## Usage

### Reading Metadata

```java
try(ParquetFileReader reader = new ParquetFileReader("data.parquet")){
    // Print file metadata
    reader.printMetadata();

    // Get schema
    SchemaDescriptor schema = reader.getSchema();
    System.out.println("Number of columns: " + schema.getNumColumns());

    // Access row groups
    int numRowGroups = reader.getNumRowGroups();
ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);
}
```

### Reading Column Data

```java
try(ParquetFileReader reader = new ParquetFileReader("data.parquet")){
ParquetFileReader.RowGroupReader rowGroup = reader.getRowGroup(0);

    // Read BOOLEAN column
    ColumnValues boolColumn = rowGroup.readColumn(0);
    List<Boolean> boolValues = boolColumn.decodeAsBoolean();

    // Read INT32 column
    ColumnValues intColumn = rowGroup.readColumn(1);
    List<Integer> intValues = intColumn.decodeAsInt32();

    // Read INT64 column
    ColumnValues longColumn = rowGroup.readColumn(2);
    List<Long> longValues = longColumn.decodeAsInt64();

    // Read FLOAT column
    ColumnValues floatColumn = rowGroup.readColumn(3);
    List<Float> floatValues = floatColumn.decodeAsFloat();

    // Read DOUBLE column
    ColumnValues doubleColumn = rowGroup.readColumn(4);
    List<Double> doubleValues = doubleColumn.decodeAsDouble();

    // Read BYTE_ARRAY column as strings
    ColumnValues stringColumn = rowGroup.readColumn(5);
    List<String> strings = stringColumn.decodeAsString();
}
```

### Reading Simple Map columns
- See [MapColumnExample](src/main/java/io/github/aloksingh/parquet/util/MapColumnExample.java)

## Building and Testing

```bash
# Compile
mvn compile

# Run tests
mvn test

# Create JAR
mvn package
```

## Dependencies

- **Apache Parquet Format Structures** (1.13.1): Provides Thrift definitions for Parquet metadata
- **Apache Thrift** (0.19.0): For reading Thrift-encoded metadata
- **Gson 2.11**: Provides support for exporting data in JSON format
- **JUnit 5**: For testing
- Compression Codec dependencies: Can be excluded if you are not reading/writing files with these codecs
- **Snappy Java** (1.1.10.5): For Snappy codex support
- **lz4-java**: For LZ4 codec support
- **zstd-jni**: For ZSTD codec support

## Future Enhancements

- **Deeply nested structures** (lists of lists, maps of lists, etc.)
- **Page Index** and **Offset Index** reading for predicate pushdown
- **Bloom filter** support

## License

[Apache 2](./LICENSE)

## References

- [Apache Parquet Format Specification](https://github.com/apache/parquet-format)
- [Apache Arrow Rust Parquet Implementation](https://github.com/apache/arrow-rs/tree/master/parquet)
- [Parquet Documentation](https://parquet.apache.org/)
