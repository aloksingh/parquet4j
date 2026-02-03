# Parquet Format in Dogma

This document describes the Apache Parquet columnar storage format using the Dogma metalanguage.

## About This Specification

The file `parquet_format.dogma` contains a Dogma specification for the Apache Parquet format, based on the Java
implementation in this repository (`src/main/java/org/parquet/`).

## What is Parquet?

Apache Parquet is a columnar storage format optimized for use with big data processing frameworks. Unlike row-oriented
formats (like CSV), Parquet stores data by columns, which provides several advantages:

- **Better compression**: Similar data is stored together
- **Efficient querying**: Can read only the columns needed
- **Schema evolution**: Supports adding/removing columns
- **Multiple encodings**: Different encoding schemes for different data types

## File Structure Overview

A Parquet file has the following structure:

```
+----------------+
| File Header    | (4 bytes: "PAR1")
+----------------+
| Row Group 1    |
|   Column Chunk |
|     Page 1     |
|     Page 2     |
|     ...        |
|   Column Chunk |
|     ...        |
+----------------+
| Row Group 2    |
|   ...          |
+----------------+
| ...            |
+----------------+
| File Metadata  | (Thrift encoded)
+----------------+
| Footer Length  | (4 bytes, little-endian)
+----------------+
| File Footer    | (4 bytes: "PAR1")
+----------------+
```

### Key Concepts

#### Row Groups

A row group is a horizontal partitioning of the data. Each row group contains one column chunk per column. Row groups
are the unit of data that can be read in parallel.

#### Column Chunks

A column chunk contains all values for a particular column within a row group. Column chunks are composed of one or more
pages.

#### Pages

Pages are the unit of compression and encoding. There are three types of pages:

1. **Dictionary Page** (optional): Contains dictionary values for dictionary encoding
2. **Data Page V1**: Contains column values with embedded repetition/definition levels
3. **Data Page V2**: Contains column values with separate uncompressed levels

## Page Structure Details

### Data Page V1

After decompression, a Data Page V1 contains:

```
+---------------------------+
| Repetition Level Length   | (4 bytes LE, if max_rep_level > 0)
| Repetition Levels (RLE)   | (variable length)
+---------------------------+
| Definition Level Length   | (4 bytes LE, if max_def_level > 0)
| Definition Levels (RLE)   | (variable length)
+---------------------------+
| Encoded Values            | (encoding specified in page header)
+---------------------------+
```

### Data Page V2

A Data Page V2 has a different structure where levels are stored uncompressed:

```
+---------------------------+
| Repetition Levels (RLE)   | (uncompressed, length in header)
+---------------------------+
| Definition Levels (RLE)   | (uncompressed, length in header)
+---------------------------+
| Encoded Values            | (may be compressed)
+---------------------------+
```

## Encoding Types

Parquet supports multiple encoding schemes:

### PLAIN (0)

Raw values stored without encoding. Fixed-width types are stored as little-endian.

### RLE (3)

Run-Length Encoding for repeated values. Used for repetition/definition levels.

### DELTA_BINARY_PACKED (5)

Delta encoding with bit-packing. Stores differences between consecutive values using variable-width bit-packing.

### DELTA_LENGTH_BYTE_ARRAY (6)

For byte arrays: stores lengths using delta encoding, followed by concatenated data.

### DELTA_BYTE_ARRAY (7)

For byte arrays: stores prefix lengths and suffixes.

### RLE_DICTIONARY (8)

Dictionary encoding where values are replaced with dictionary indices encoded using RLE.

### BYTE_STREAM_SPLIT (9)

Splits multi-byte values into separate streams by byte position. Good for floating-point data.

## Physical Types

Parquet supports these physical storage types:

- **BOOLEAN** (0): Single bit values
- **INT32** (1): 32-bit signed integers
- **INT64** (2): 64-bit signed integers
- **INT96** (3): 96-bit values (deprecated, used for legacy timestamps)
- **FLOAT** (4): IEEE 754 single-precision (32-bit) floats
- **DOUBLE** (5): IEEE 754 double-precision (64-bit) floats
- **BYTE_ARRAY** (6): Variable-length byte arrays
- **FIXED_LEN_BYTE_ARRAY** (7): Fixed-length byte arrays

## Compression Codecs

Parquet supports multiple compression algorithms:

- **UNCOMPRESSED** (0): No compression
- **SNAPPY** (1): Fast compression/decompression
- **GZIP** (2): Better compression ratio, slower
- **LZO** (3): Fast compression
- **BROTLI** (4): High compression ratio
- **LZ4** (5): Very fast compression
- **ZSTD** (6): Good balance of speed and compression
- **LZ4_RAW** (7): LZ4 without framing

## Repetition and Definition Levels

Parquet uses two concepts to represent nested and optional data:

### Definition Levels

Indicates how many optional fields in the path to this column are defined.

- Used to distinguish null values from missing nested structures
- Max definition level = number of optional/repeated fields in path

### Repetition Levels

Indicates at what repeated field in the path the value has repeated.

- Used to reconstruct nested structures
- Max repetition level = number of repeated fields in path

Example:

```
message Document {
  optional group Person {
    repeated string phoneNumbers;
  }
}
```

- `phoneNumbers` has max definition level = 2 (Person + phoneNumbers both optional/repeated)
- `phoneNumbers` has max repetition level = 1 (one repeated field)

## Metadata Structure

The file footer contains Thrift-encoded metadata with:

### FileMetaData

- **version**: Parquet format version
- **schema**: List of SchemaElement describing the column schema
- **num_rows**: Total number of rows
- **row_groups**: List of RowGroup metadata
- **key_value_metadata**: Optional custom key-value pairs
- **created_by**: String identifying the writer
- **column_orders**: Column sort orders (optional)

### RowGroup

- **columns**: List of ColumnChunk metadata
- **total_byte_size**: Total size of all column chunks
- **num_rows**: Number of rows in this row group

### ColumnChunk

- **file_path**: Path to column chunk data (usually null)
- **file_offset**: Byte offset of column chunk
- **meta_data**: ColumnMetaData

### ColumnMetaData

- **type**: Physical type (BOOLEAN, INT32, etc.)
- **encodings**: List of encodings used
- **path_in_schema**: Column path in the schema
- **codec**: Compression codec used
- **num_values**: Number of values in this column chunk
- **total_uncompressed_size**: Total uncompressed byte size
- **total_compressed_size**: Total compressed byte size
- **key_value_metadata**: Optional column-specific metadata
- **data_page_offset**: Offset of first data page
- **index_page_offset**: Offset of column index page (optional)
- **dictionary_page_offset**: Offset of dictionary page (optional)
- **statistics**: Min/max/null_count statistics (optional)

## Encoding Details

### Variable-length Integer (varint)

Varints encode integers using 1-10 bytes. Each byte uses 7 bits for data and 1 bit (MSB) as a continuation flag:

- If MSB = 1, more bytes follow
- If MSB = 0, this is the last byte
- Bytes are in little-endian order for data bits

Example encoding 300:

```
300 = 0x012C = 0b00000001_00101100
Varint: [0xAC, 0x02] = [0b10101100, 0b00000010]
                         ^-------^  ^-------^
                         data+cont  data+end
```

### RLE/Bit-Packed Hybrid

Used for repetition and definition levels. The encoding alternates between RLE runs and bit-packed runs.

Header format (varint):

- Bit 0: Mode (1 = RLE, 0 = bit-packed)
- Remaining bits: Length information

**RLE run**: `header = (count << 1) | 1` followed by single value
**Bit-packed run**: `header = (byte_count << 1) | 0` followed by bit-packed values

The bit width for values is: `ceil(log2(max_level + 1))`

### Delta Binary Packed

Encodes integers as differences (deltas) from previous values, using variable-width bit-packing.

Format:

1. Block header (5 varints):
    - block_size: number of values in this block
    - miniblocks_in_block: number of miniblocks
    - total_value_count: total values in this page
    - first_value: the first value (zigzag encoded)

2. For each miniblock:
    - min_delta: minimum delta in this miniblock (zigzag varint)
    - bit_width: bits per delta value
    - deltas: bit-packed delta values

## Java Implementation Reference

This Dogma specification is based on the Java implementation in this repository. Key source files:

- [`ParquetMetadataReader.java`](../src/main/java/io/github/aloksingh/parquet/ParquetMetadataReader.java): Reads file metadata from
  footer
- [`PageReader.java`](../src/main/java/io/github/aloksingh/parquet/PageReader.java): Reads pages from column chunks
- [`SerializedFileReader.java`](../src/main/java/io/github/aloksingh/parquet/SerializedFileReader.java): Main entry point for reading
  Parquet files
- [`model/Type.java`](../src/main/java/io/github/aloksingh/parquet/model/Type.java): Physical type enumeration
- [`model/Encoding.java`](../src/main/java/io/github/aloksingh/parquet/model/Encoding.java): Encoding type enumeration
- [`model/CompressionCodec.java`](../src/main/java/io/github/aloksingh/parquet/model/CompressionCodec.java): Compression codec
  enumeration
- [`model/Page.java`](../src/main/java/io/github/aloksingh/parquet/model/Page.java): Page data structures

## Using the Dogma Specification

The `parquet_format.dogma` file describes the binary structure of Parquet files. You can use it to:

1. **Understand the format**: Read through the documented structure
2. **Implement parsers**: Use as a reference when writing Parquet readers
3. **Generate parsers**: Use with Dogma-based parser generators (if available)
4. **Document extensions**: Fork and extend for custom Parquet variants

## References

- [Apache Parquet Documentation](https://parquet.apache.org/docs/)
- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Thrift Compact Protocol](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
- [Dremel Paper](https://research.google/pubs/pub36632/) (Original paper describing columnar nested data)
- [Dogma Specification](../dogma/v1/dogma_v1.0.md)

## License

This Dogma specification is provided as documentation and follows the same license as the parent project.
