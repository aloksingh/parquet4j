package io.github.aloksingh.parquet;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import shaded.parquet.org.apache.thrift.protocol.TCompactProtocol;
import shaded.parquet.org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

final class WriterTestSupport {
    private WriterTestSupport() {
    }

    static FileMetaData footer(Path path) throws Exception {
        byte[] bytes = Files.readAllBytes(path);
        int length = ByteBuffer.wrap(bytes, bytes.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        FileMetaData footer = new FileMetaData();
        footer.read(new TCompactProtocol(new TIOStreamTransport(
                new ByteArrayInputStream(bytes, bytes.length - 8 - length, length))));
        return footer;
    }

    record PageData(PageHeader header, int headerSize, byte[] payload) {
    }

    static java.util.List<PageData> pages(Path path, int group, int column) throws Exception {
        var metadata = footer(path).getRow_groups().get(group).getColumns().get(column).getMeta_data();
        byte[] bytes = Files.readAllBytes(path);
        int start = Math.toIntExact(metadata.isSetDictionary_page_offset()
                ? metadata.getDictionary_page_offset() : metadata.getData_page_offset());
        int length = Math.toIntExact(metadata.getTotal_compressed_size());
        ByteArrayInputStream input = new ByteArrayInputStream(bytes, start, length);
        java.util.List<PageData> pages = new java.util.ArrayList<>();
        while (input.available() > 0) {
            int before = input.available();
            PageHeader header = new PageHeader();
            header.read(new TCompactProtocol(new TIOStreamTransport(input)));
            int headerSize = before - input.available();
            byte[] payload = input.readNBytes(header.getCompressed_page_size());
            org.junit.jupiter.api.Assertions.assertEquals(header.getCompressed_page_size(), payload.length);
            pages.add(new PageData(header, headerSize, payload));
        }
        return pages;
    }

    /**
     * Small independent hybrid-RLE decoder for level-section assertions, not production decoding.
     */
    static int[] decodeLevels(byte[] raw, int bitWidth, int count) {
        int[] levels = new int[count];
        int offset = 0;
        int observed = 0;
        while (observed < count) {
            int header = 0;
            int shift = 0;
            int next;
            do {
                org.junit.jupiter.api.Assertions.assertTrue(offset < raw.length, "Truncated level run");
                next = raw[offset++] & 0xff;
                header |= (next & 0x7f) << shift;
                shift += 7;
            } while ((next & 0x80) != 0);
            if ((header & 1) == 0) {
                int run = header >>> 1;
                int value = 0;
                for (int i = 0; i < (bitWidth + 7) / 8; i++) value |= (raw[offset++] & 0xff) << (i * 8);
                int take = Math.min(run, count - observed);
                Arrays.fill(levels, observed, observed + take, value);
                observed += take;
            } else {
                int run = (header >>> 1) * 8;
                int take = Math.min(run, count - observed);
                for (int i = 0; i < take; i++) {
                    int value = 0;
                    for (int bit = 0; bit < bitWidth; bit++) {
                        int position = i * bitWidth + bit;
                        value |= ((raw[offset + (position >>> 3)] >>> (position & 7)) & 1) << bit;
                    }
                    levels[observed++] = value;
                }
                offset += (run * bitWidth + 7) / 8;
            }
        }
        return levels;
    }

    static byte[] firstPageValues(Path path, int column, int maxRepetition, int maxDefinition)
            throws Exception {
        FileMetaData footer = footer(path);
        byte[] bytes = Files.readAllBytes(path);
        int offset = Math.toIntExact(footer.getRow_groups().getFirst().getColumns().get(column)
                .getMeta_data().getData_page_offset());
        ByteArrayInputStream input = new ByteArrayInputStream(bytes, offset, bytes.length - offset);
        PageHeader page = new PageHeader();
        page.read(new TCompactProtocol(new TIOStreamTransport(input)));
        byte[] payload = input.readNBytes(page.getCompressed_page_size());
        int levels = 0;
        if (page.isSetData_page_header_v2()) {
            levels = page.getData_page_header_v2().getRepetition_levels_byte_length()
                    + page.getData_page_header_v2().getDefinition_levels_byte_length();
        } else {
            ByteBuffer buffer = ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN);
            if (maxRepetition > 0) {
                int length = buffer.getInt();
                buffer.position(buffer.position() + length);
            }
            if (maxDefinition > 0) {
                int length = buffer.getInt();
                buffer.position(buffer.position() + length);
            }
            levels = buffer.position();
        }
        return java.util.Arrays.copyOfRange(payload, levels, payload.length);
    }
}
