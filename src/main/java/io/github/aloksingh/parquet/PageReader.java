package io.github.aloksingh.parquet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.ParquetMetadata;
import shaded.parquet.org.apache.thrift.TException;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

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
    this.chunkReader = chunkReader;
    this.columnMeta = columnMeta;
    this.columnDescriptor = columnDescriptor;
    this.decompressor = Decompressor.create(columnMeta.codec());

    // Start reading from the first page offset
    this.currentOffset = columnMeta.getFirstDataPageOffset();
    this.endOffset = currentOffset + columnMeta.totalCompressedSize();
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

    try {
      // Read page header (we don't know the size, so read a reasonable amount)
      // Page headers are typically small (< 100 bytes)
      ByteBuffer headerBuffer = chunkReader.readBytes(currentOffset, 256);
      byte[] headerBytes = new byte[headerBuffer.remaining()];
      headerBuffer.get(headerBytes);

      // Parse page header
      ByteArrayInputStream bais = new ByteArrayInputStream(headerBytes);
      TIOStreamTransport transport = new TIOStreamTransport(bais);
      TCompactProtocol protocol = new TCompactProtocol(transport);

      PageHeader pageHeader = new PageHeader();
      pageHeader.read(protocol);

      // Calculate header size by tracking how much was consumed from the input stream
      int headerSize = headerBytes.length - bais.available();

      // Move offset past header
      currentOffset += headerSize;

      // Read compressed page data
      int compressedSize = pageHeader.getCompressed_page_size();
      int uncompressedSize = pageHeader.getUncompressed_page_size();

      // Create appropriate page type based on page type
      // NOTE: For DATA_PAGE_V2, we must NOT decompress here because the levels are uncompressed
      if (pageHeader.getType() == PageType.DATA_PAGE_V2) {
        // Handle DATA_PAGE_V2 separately - levels are uncompressed, data may be compressed
        ByteBuffer allPageData = chunkReader.readBytes(currentOffset, compressedSize);
        currentOffset += compressedSize;

        var dataPageV2Header = pageHeader.getData_page_header_v2();

        int defLevelsByteLen = dataPageV2Header.getDefinition_levels_byte_length();
        int repLevelsByteLen = dataPageV2Header.getRepetition_levels_byte_length();
        boolean isCompressed = dataPageV2Header.isIs_compressed();

        // Extract repetition levels (uncompressed)
        ByteBuffer repetitionLevels = ByteBuffer.allocate(repLevelsByteLen);
        for (int i = 0; i < repLevelsByteLen; i++) {
          repetitionLevels.put(allPageData.get());
        }
        repetitionLevels.flip();

        // Extract definition levels (uncompressed)
        ByteBuffer definitionLevels = ByteBuffer.allocate(defLevelsByteLen);
        for (int i = 0; i < defLevelsByteLen; i++) {
          definitionLevels.put(allPageData.get());
        }
        definitionLevels.flip();

        // Extract data (may be compressed)
        int dataSize = compressedSize - repLevelsByteLen - defLevelsByteLen;
        byte[] compressedDataBytes = new byte[dataSize];
        allPageData.get(compressedDataBytes);
        ByteBuffer compressedDataBuf = ByteBuffer.wrap(compressedDataBytes);

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
      ByteBuffer compressedData = chunkReader.readBytes(currentOffset, compressedSize);
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

        // For Data Page V1, definition and repetition levels are stored at the beginning
        // of the page data. Each section starts with a 4-byte length field (little-endian)
        // indicating the number of bytes of RLE-encoded level data.
        //
        // Format: [def_level_length][def_level_data][values]
        // OR:     [rep_level_length][rep_level_data][def_level_length][def_level_data][values]
        //
        // Note: If max_repetition_level = 0, repetition levels are omitted entirely
        // If max_definition_level = 0, definition levels are omitted entirely
        //
        // TODO: Properly determine if levels exist by checking max levels from schema
        // For now, we use a heuristic: try to read the length, and if it seems valid, use it

        int repLevelLen = 0;
        int defLevelLen = 0;

        pageData.order(java.nio.ByteOrder.LITTLE_ENDIAN);

        int offset = 0;  // Track total bytes consumed by levels

        // Check if we need to read repetition levels
        // Repetition levels exist when max_repetition_level > 0
        if (columnDescriptor.maxRepetitionLevel() > 0) {
          if (pageData.remaining() >= 4) {
            int savedPos = pageData.position();
            int repLevelDataLen = pageData.getInt();
            repLevelLen = 4 + repLevelDataLen;
            pageData.position(savedPos);  // Reset position for ColumnValues to read
            offset += repLevelLen;
          }
        }

        // Check if we need to read definition levels
        // Definition levels exist when max_definition_level > 0
        if (columnDescriptor.maxDefinitionLevel() > 0) {
          if (pageData.remaining() >= offset + 4) {
            int savedPos = pageData.position();
            pageData.position(savedPos + offset);  // Skip past rep levels
            int defLevelDataLen = pageData.getInt();
            defLevelLen = 4 + defLevelDataLen;
            pageData.position(savedPos);  // Reset position for ColumnValues to read
          }
        }

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
      throw new ParquetException("Failed to parse page header", e);
    }
  }
}
