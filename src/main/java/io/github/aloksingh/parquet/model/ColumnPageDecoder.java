package io.github.aloksingh.parquet.model;

import io.github.aloksingh.parquet.DecodeChecks;
import io.github.aloksingh.parquet.RleDecoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Stateful physical decoder for one column chunk, operating on decompressed pages.
 */
public final class ColumnPageDecoder {
    private final ColumnDescriptor descriptor;
    private Page.DictionaryPage dictionaryPage;
    private Object[] dictionary;
    private boolean dataDecoded;

    public ColumnPageDecoder(ColumnDescriptor descriptor) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        if (descriptor.physicalType() == null || descriptor.maxDefinitionLevel() < 0
                || descriptor.maxRepetitionLevel() < 0
                || (descriptor.physicalType() == Type.FIXED_LEN_BYTE_ARRAY && descriptor.typeLength() <= 0)) {
            throw new ParquetException("Invalid physical column descriptor");
        }
    }

    public void setDictionary(Page.DictionaryPage page) {
        Objects.requireNonNull(page, "dictionary page");
        if (page == dictionaryPage) return;
        if (dictionaryPage != null) {
            throw new ParquetException("Multiple dictionary pages for " + descriptor.getPathString());
        }
        if (dataDecoded) throw new ParquetException("Dictionary page must precede data pages");
        if (page.encoding() != Encoding.PLAIN && page.encoding() != Encoding.PLAIN_DICTIONARY) {
            throw new ParquetException("Unsupported dictionary encoding " + page.encoding());
        }
        if (page.numValues() < 0) throw new ParquetException("Negative dictionary value count: " + page.numValues());
        ByteBuffer data = physicalBuffer(page.data());
        Object values = PlainValueDecoder.decode(descriptor, data, page.numValues());
        DecodeChecks.requireConsumed(data, "dictionary values");
        Object[] entries = new Object[page.numValues()];
        for (int i = 0; i < entries.length; i++) {
            entries[i] = values instanceof BinaryValues binary ? binary.bytesAt(i)
                    : java.lang.reflect.Array.get(values, i);
        }
        dictionaryPage = page;
        dictionary = entries;
    }

    public DecodedPage decode(Page page) {
        ByteBuffer data;
        int count;
        Encoding encoding;
        int[] definitions;
        int[] repetitions;
        int maxDefinition = descriptor.maxDefinitionLevel();
        if (page instanceof Page.DataPage dataPage) {
            data = physicalBuffer(dataPage.data());
            count = dataPage.numValues();
            if (count < 0) throw new ParquetException("Negative page value count: " + count);
            encoding = dataPage.encoding();
            repetitions = LevelStreams.readV1Levels(data, dataPage.repetitionLevelByteLen(), count,
                    descriptor.maxRepetitionLevel(), "repetition");
            definitions = LevelStreams.readV1Levels(data, dataPage.definitionLevelByteLen(), count, maxDefinition, "definition");
        } else if (page instanceof Page.DataPageV2 dataPage) {
            data = physicalBuffer(dataPage.data());
            count = dataPage.numValues();
            if (count < 0) throw new ParquetException("Negative page value count: " + count);
            encoding = dataPage.encoding();
            repetitions = LevelStreams.readLevels(dataPage.repetitionLevels(), count, descriptor.maxRepetitionLevel(), "repetition");
            definitions = LevelStreams.readLevels(dataPage.definitionLevels(), count, maxDefinition, "definition");
        } else {
            throw new ParquetException("Expected a data page for " + descriptor.getPathString());
        }
        int present = 0;
        for (int i = 0; i < count; i++) {
            if (definitions == null || definitions[i] == maxDefinition) {
                present++;
            }
        }
        if (page instanceof Page.DataPageV2 v2 && (v2.numRows() < 0 || v2.numRows() > count
                || (descriptor.maxRepetitionLevel() == 0 && v2.numRows() != count))) {
            throw new ParquetException("Invalid V2 row count: " + v2.numRows());
        }
        if (page instanceof Page.DataPageV2 v2 && v2.numNulls() != count - present) {
            throw new ParquetException("V2 null count does not match definition levels: " + v2.numNulls()
                    + ", expected " + (count - present));
        }
        if (encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY) {
            if (dictionary == null) {
                throw new ParquetException("Dictionary page not found for " + descriptor.getPathString());
            }
            if (present > 0) DecodeChecks.requireBytes(data, 1, "dictionary bit width");
            int width = present == 0 && !data.hasRemaining() ? 0 : data.get() & 0xff;
            LevelStreams.validateHybrid(data, width, present, "dictionary indexes");
            int[] indices = new RleDecoder(data, width, present).readAll();
            for (int index : indices) {
                if (index < 0 || index >= dictionary.length) {
                    throw new ParquetException("Dictionary index out of bounds: " + index + " of " + dictionary.length);
                }
            }
            dataDecoded = true;
            return new DecodedPage(count, present, maxDefinition, definitions, repetitions, indices, indices, dictionary);
        }
        Object values;
        if (encoding == Encoding.PLAIN) {
            values = PlainValueDecoder.decode(descriptor, data, present);
        } else if (encoding == Encoding.DELTA_BYTE_ARRAY && (descriptor.physicalType() == Type.BYTE_ARRAY
                || descriptor.physicalType() == Type.FIXED_LEN_BYTE_ARRAY)) {
            values = present == 0 && !data.hasRemaining()
                    ? new BinaryValues(new int[]{0}, data)
                    : DeltaByteArrayDecoder.decode(data, present,
                        descriptor.physicalType() == Type.FIXED_LEN_BYTE_ARRAY ? descriptor.typeLength() : 0);
        } else if (encoding == Encoding.DELTA_LENGTH_BYTE_ARRAY && descriptor.physicalType() == Type.BYTE_ARRAY) {
            values = DeltaLengthByteArrayDecoder.decode(data, present);
        } else if (encoding == Encoding.DELTA_BINARY_PACKED) {
            values = DeltaBinaryValueDecoder.decode(descriptor, data, present);
        } else if (encoding == Encoding.BYTE_STREAM_SPLIT) {
            values = readByteStreamSplit(data, present);
        } else if (encoding == Encoding.RLE && descriptor.physicalType() == Type.BOOLEAN) {
            if (present > 0 || data.hasRemaining()) DecodeChecks.requireBytes(data, 4, "boolean RLE length");
            int length = present == 0 && !data.hasRemaining() ? 0 : data.getInt();
            DecodeChecks.requireBytes(data, length, "boolean RLE payload");
            ByteBuffer rle = data.slice().order(ByteOrder.LITTLE_ENDIAN);
            rle.limit(length);
            data.position(data.position() + length);
            LevelStreams.validateHybrid(rle, 1, present, "boolean values");
            int[] integers = new RleDecoder(rle, 1, present).readAll();
            boolean[] booleans = new boolean[present];
            for (int i = 0; i < present; i++) booleans[i] = integers[i] == 1;
            values = booleans;
        } else {
            throw new ParquetException("Unsupported encoding " + encoding + " for " + descriptor.physicalType());
        }
        DecodeChecks.requireConsumed(data, "physical values");
        dataDecoded = true;
        return new DecodedPage(count, present, maxDefinition, definitions, repetitions, values);
    }

    private static ByteBuffer physicalBuffer(ByteBuffer source) {
        if (source == null) throw new ParquetException("Missing physical page buffer");
        return source.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    }

    private Object readByteStreamSplit(ByteBuffer data, int count) {
        int width = switch (descriptor.physicalType()) {
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            case FIXED_LEN_BYTE_ARRAY -> descriptor.typeLength();
            default -> throw new ParquetException("Unsupported BYTE_STREAM_SPLIT type " + descriptor.physicalType());
        };
        DecodeChecks.requireBytes(data, count * (long) width, "BYTE_STREAM_SPLIT values");
        // Fixed-width primitives share one plane-reassembly implementation. The page-level
        // truncation check above keeps this path's error messages independent of it.
        return switch (descriptor.physicalType()) {
            case INT32 -> new io.github.aloksingh.parquet.ByteStreamSplitDecoder(data, count, 4).decodeInt32();
            case INT64 -> new io.github.aloksingh.parquet.ByteStreamSplitDecoder(data, count, 8).decodeInt64();
            case FLOAT -> new io.github.aloksingh.parquet.ByteStreamSplitDecoder(data, count, 4).decodeFloat();
            case DOUBLE -> new io.github.aloksingh.parquet.ByteStreamSplitDecoder(data, count, 8).decodeDouble();
            case FIXED_LEN_BYTE_ARRAY -> {
                int start = data.position();
                int[] offsets = new int[count + 1];
                ByteBuffer payload = ByteBuffer.allocate(DecodeChecks.checkedSize(count * (long) width, "fixed binary payload"));
                for (int i = 0; i < count; i++) {
                    offsets[i + 1] = offsets[i] + width;
                    for (int lane = 0; lane < width; lane++)
                        payload.put(i * width + lane, data.get(start + lane * count + i));
                }
                data.position(start + count * width);
                yield new BinaryValues(offsets, payload);
            }
            default -> throw new ParquetException("Unsupported BYTE_STREAM_SPLIT type " + descriptor.physicalType());
        };
    }

}
