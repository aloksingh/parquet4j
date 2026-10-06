package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.*;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads pages from a column chunk in a Parquet file.
 *
 * <p>This class is responsible for parsing page headers, decompressing page data,
 * and creating appropriate Page objects based on the page type (DATA_PAGE, DATA_PAGE_V2,
 * or DICTIONARY_PAGE). It handles the complexities of different page formats and
 * compression schemes.</p>
 *
 * <p>The reader maintains an internal offset to track the current position within
 * the column chunk and supports sequential reading of pages via {@link #readNextPage()}
 * or batch reading via {@link #readAllPages()}.</p>
 */
public class PageReader {
  private final ChunkReader chunkReader;
  private final ParquetMetadata.ColumnChunkMetadata columnMeta;
  private final Decompressor decompressor;
  private final ColumnDescriptor columnDescriptor;
  private final PageReadOptions options;
  private long currentOffset;
  private final long endOffset;

  /**
   * Creates a new PageReader for reading pages from a column chunk.
   *
   * @param chunkReader the chunk reader used to read raw bytes from the file
   * @param columnMeta metadata for the column chunk being read
   * @param columnDescriptor descriptor containing schema information about the column
   */
  public PageReader(ChunkReader chunkReader,
                    ParquetMetadata.ColumnChunkMetadata columnMeta,
                    ColumnDescriptor columnDescriptor) {
    this(chunkReader, columnMeta, columnDescriptor, PageReadOptions.DEFAULT);
  }

  /**
   * Creates a page reader with explicit resource limits and checksum policy.
   * @param chunkReader the source of positional reads
   * @param columnMeta column chunk metadata
   * @param columnDescriptor the column's schema descriptor
   * @param options resource limits and checksum policy
   */
  public PageReader(ChunkReader chunkReader,
                    ParquetMetadata.ColumnChunkMetadata columnMeta,
                    ColumnDescriptor columnDescriptor,
                    PageReadOptions options) {
    this.chunkReader = java.util.Objects.requireNonNull(chunkReader, "chunkReader");
    this.columnMeta = java.util.Objects.requireNonNull(columnMeta, "columnMeta");
    this.columnDescriptor = java.util.Objects.requireNonNull(columnDescriptor, "columnDescriptor");
    this.options = java.util.Objects.requireNonNull(options, "options");
    long dataOffset = columnMeta.dataPageOffset();
    long dictionaryOffset = columnMeta.dictionaryPageOffset();
    long size = columnMeta.totalCompressedSize();
    // The existing metadata adapter uses -1 for an absent dictionary offset.
    // It is not a file address; all offsets that will actually be read are nonnegative.
    if (dataOffset < 0 || dictionaryOffset < -1 || size < 0
        || columnMeta.totalUncompressedSize() < 0 || columnMeta.numValues() < 0) {
      throw new ParquetException("Negative column chunk offset, size, or value count");
    }
    // Empty dictionary-only chunks have no data page and may use a zero data offset.
    boolean hasDataOffset = columnMeta.numValues() != 0 || dataOffset != 0;
    if (hasDataOffset && dictionaryOffset > 0 && dictionaryOffset > dataOffset) {
      throw new ParquetException("Dictionary page offset follows the first data page");
    }
    long start = columnMeta.getFirstDataPageOffset();
    long fileLength;
    try {
      fileLength = chunkReader.length();
    } catch (IOException e) {
      throw new ParquetException("Failed to determine column chunk source length", e);
    }
    // Subtraction validates both EOF and addition overflow without wrapping offsets.
    if (fileLength < 0 || start > fileLength || size > fileLength - start) {
      throw new ParquetException("Column chunk range exceeds source length: offset=" + start
          + ", size=" + size + ", sourceLength=" + fileLength);
    }
    this.currentOffset = start;
    this.endOffset = start + size;
    if (dataOffset > endOffset) {
      throw new ParquetException("First data page offset lies outside the column chunk");
    }
    this.decompressor = Decompressor.create(columnMeta.codec(), options);
  }

  /**
   * Reads all pages from this column chunk sequentially.
   *
   * <p>This method repeatedly calls {@link #readNextPage()} until no more pages
   * are available in the column chunk.</p>
   *
   * @return a list of all pages in this column chunk
   * @throws IOException if an I/O error occurs while reading page data
   */
  public List<Page> readAllPages() throws IOException {
    List<Page> pages = new ArrayList<>();
    Page page;
    while ((page = readNextPage()) != null) {
      pages.add(page);
    }
    return pages;
  }

  /**
   * Reads the next page from the column chunk.
   *
   * <p>This method performs the following operations:</p>
   * <ol>
   *   <li>Reads and parses the Thrift-encoded page header</li>
   *   <li>Reads the compressed page data</li>
   *   <li>Decompresses the data if necessary (handling differs by page type)</li>
   *   <li>Creates the appropriate Page object (DataPage, DataPageV2, or DictionaryPage)</li>
   * </ol>
   *
   * <p><b>Special handling for DATA_PAGE_V2:</b> In V2 pages, repetition and definition
   * levels are stored uncompressed at the beginning of the page, followed by the
   * (possibly compressed) data. This method extracts the levels separately before
   * decompressing the data portion.</p>
   *
   * <p><b>Special handling for DATA_PAGE (V1):</b> In V1 pages, the entire page is
   * decompressed first, then repetition and definition levels are extracted from
   * the beginning of the decompressed data. Each level section starts with a 4-byte
   * little-endian length field.</p>
   *
   * @return the next Page object, or {@code null} if there are no more pages in the chunk
   * @throws IOException if an I/O error occurs while reading page data
   * @throws ParquetException if the page header cannot be parsed or an unsupported
   *         page type is encountered
   */
  public Page readNextPage() throws IOException {
    if (currentOffset >= endOffset) {
      return null;
    }

    long pageOffset = currentOffset;
    try {
      int headerLimit = (int) Math.min((long) options.maxHeaderBytes(), endOffset - currentOffset);
      PageHeaderInputStream input = new PageHeaderInputStream(chunkReader, currentOffset, headerLimit);
      var configuration = new shaded.parquet.org.apache.thrift.TConfiguration(headerLimit, headerLimit, 100);
      TIOStreamTransport transport = new TIOStreamTransport(configuration, input);
      TCompactProtocol protocol = new TCompactProtocol(transport, headerLimit, headerLimit);
      PageHeader pageHeader = new PageHeader();
      pageHeader.read(protocol);
      int headerSize = input.bytesConsumed();

      // Move offset past header
      currentOffset += headerSize;

      // Read compressed page data
      int compressedSize = pageHeader.getCompressed_page_size();
      int uncompressedSize = pageHeader.getUncompressed_page_size();
      validateSize("compressed page size", compressedSize, options.maxCompressedPageBytes());
      validateSize("uncompressed page size", uncompressedSize, options.maxUncompressedPageBytes());
      if (compressedSize > endOffset - currentOffset) {
        throw new ParquetException("Page body exceeds column chunk boundary");
      }
      validatePageCounts(pageHeader);

      // Create appropriate page type based on page type
      // NOTE: For DATA_PAGE_V2, we must NOT decompress here because the levels are uncompressed
      if (pageHeader.getType() == PageType.DATA_PAGE_V2) {
        // Handle DATA_PAGE_V2 separately - levels are uncompressed, data may be compressed
        ByteBuffer allPageData = readPageBody(compressedSize);
        verifyChecksum(pageHeader, allPageData);
        currentOffset += compressedSize;

        var dataPageV2Header = pageHeader.getData_page_header_v2();

        int defLevelsByteLen = dataPageV2Header.getDefinition_levels_byte_length();
        int repLevelsByteLen = dataPageV2Header.getRepetition_levels_byte_length();
        // The pinned Parquet Thrift definition makes an omitted flag true.
        boolean isCompressed = !dataPageV2Header.isSetIs_compressed() || dataPageV2Header.isIs_compressed();

        // Validated level and data ranges share the stored body, without per-byte copies.
        ByteBuffer repetitionLevels = allPageData.slice(0, repLevelsByteLen).asReadOnlyBuffer();
        ByteBuffer definitionLevels = allPageData.slice(repLevelsByteLen, defLevelsByteLen).asReadOnlyBuffer();
        int levelBytes = repLevelsByteLen + defLevelsByteLen;
        int dataSize = compressedSize - levelBytes;
        ByteBuffer compressedDataBuf = allPageData.slice(levelBytes, dataSize).asReadOnlyBuffer();

        // Decompress data if needed
        ByteBuffer decompressedData;
        if (isCompressed) {
          int uncompressedDataSize = uncompressedSize - repLevelsByteLen - defLevelsByteLen;
          decompressedData = decompressor.decompress(compressedDataBuf, uncompressedDataSize);
        } else {
          decompressedData = compressedDataBuf;
        }

        Encoding encoding = Encoding.fromValue(dataPageV2Header.getEncoding().getValue());

        return new Page.DataPageV2(
            decompressedData,
            dataPageV2Header.getNum_values(),
            dataPageV2Header.getNum_nulls(),
            dataPageV2Header.getNum_rows(),
            encoding,
            definitionLevels,
            repetitionLevels,
            isCompressed
        );
      }

      // For other page types, read and decompress the whole page
      ByteBuffer compressedData = readPageBody(compressedSize);
      verifyChecksum(pageHeader, compressedData);
      currentOffset += compressedSize;

      // Decompress if needed
      ByteBuffer pageData = decompressor.decompress(compressedData, uncompressedSize);

      if (pageHeader.getType() == PageType.DICTIONARY_PAGE) {
        Encoding encoding = Encoding.fromValue(
            pageHeader.getDictionary_page_header().getEncoding().getValue());

        return new Page.DictionaryPage(
            pageData,
            pageHeader.getDictionary_page_header().getNum_values(),
            encoding
        );
      } else if (pageHeader.getType() == PageType.DATA_PAGE) {
        Encoding encoding = Encoding.fromValue(
            pageHeader.getData_page_header().getEncoding().getValue());

        // V1 RLE level sections carry their own little-endian byte-length prefix
        // (Parquet encodings spec). The prefixed sections are passed through unread;
        // the page decoder validates and parses the framing itself.
        pageData.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int repLevelLen = columnDescriptor.maxRepetitionLevel() > 0
            ? v1LevelLength(pageData, 0, "repetition levels") : 0;
        int defLevelLen = columnDescriptor.maxDefinitionLevel() > 0
            ? v1LevelLength(pageData, repLevelLen, "definition levels") : 0;

        return new Page.DataPage(
            pageData,
            pageHeader.getData_page_header().getNum_values(),
            encoding,
            defLevelLen,
            repLevelLen
        );
      } else {
        throw new ParquetException("Unsupported page type: " + pageHeader.getType());
      }

    } catch (TException e) {
      currentOffset = pageOffset;
      throw new ParquetException("Failed to parse page header at " + pageOffset, e);
    } catch (IOException | RuntimeException e) {
      currentOffset = pageOffset;
      throw e;
    }
  }

  private void verifyChecksum(PageHeader header, ByteBuffer body) {
    if (options.verifyChecksums() && header.isSetCrc()) {
      var crc = new java.util.zip.CRC32();
      crc.update(body.duplicate());
      if ((int) crc.getValue() != header.getCrc()) {
        throw new ParquetException("Page CRC32 mismatch at body offset " + currentOffset);
      }
    }
  }

  private static int v1LevelLength(ByteBuffer data, int offset, String field) {
    int available = data.remaining() - offset;
    if (available < Integer.BYTES) {
      throw new ParquetException("Missing length prefix for " + field);
    }
    int length = data.getInt(data.position() + offset);
    if (length < 0 || length > available - Integer.BYTES) {
      throw new ParquetException("Invalid byte length for " + field + ": " + length);
    }
    return Integer.BYTES + length;
  }

  private void validatePageCounts(PageHeader header) {
    switch (header.getType()) {
      case DATA_PAGE -> {
        var data = header.getData_page_header();
        if (data == null) throw new ParquetException("Missing DATA_PAGE header");
        validateSize("page value count", data.getNum_values(), options.maxValuesPerPage());
      }
      case DICTIONARY_PAGE -> {
        var dictionary = header.getDictionary_page_header();
        if (dictionary == null) throw new ParquetException("Missing DICTIONARY_PAGE header");
        validateSize("dictionary value count", dictionary.getNum_values(), options.maxValuesPerPage());
      }
      case DATA_PAGE_V2 -> {
        var data = header.getData_page_header_v2();
        if (data == null) throw new ParquetException("Missing DATA_PAGE_V2 header");
        validateSize("page value count", data.getNum_values(), options.maxValuesPerPage());
        validateSize("page null count", data.getNum_nulls(), data.getNum_values());
        validateSize("page row count", data.getNum_rows(), data.getNum_values());
        int storedSize = header.getCompressed_page_size();
        int decodedSize = header.getUncompressed_page_size();
        int repetition = data.getRepetition_levels_byte_length();
        int definition = data.getDefinition_levels_byte_length();
        validateSize("V2 repetition level byte length", repetition, Math.min(storedSize, decodedSize));
        validateSize("V2 definition level byte length", definition, Math.min(storedSize, decodedSize));
        long levelBytes = (long) repetition + definition;
        if (levelBytes > storedSize || levelBytes > decodedSize) {
          throw new ParquetException("V2 level byte lengths exceed the stored or decoded page size");
        }
        if (data.isSetIs_compressed() && !data.isIs_compressed() && storedSize != decodedSize) {
          throw new ParquetException("Uncompressed V2 page size mismatch");
        }
      }
      default -> throw new ParquetException("Unsupported page type: " + header.getType());
    }
  }

  private static void validateSize(String field, int value, int limit) {
    if (value < 0 || value > limit) {
      throw new ParquetException("Invalid " + field + ": " + value + " (limit " + limit + ")");
    }
  }

  private ByteBuffer readPageBody(int size) throws IOException {
    ByteBuffer body = ByteBuffer.allocate(size);
    chunkReader.readInto(currentOffset, body);
    return body.flip();
  }
}
