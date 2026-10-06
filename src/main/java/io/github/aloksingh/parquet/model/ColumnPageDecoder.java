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
        Object values = readPlain(data, page.numValues());
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
            repetitions = readV1Levels(data, dataPage.repetitionLevelByteLen(), count,
                    descriptor.maxRepetitionLevel(), "repetition");
            definitions = readV1Levels(data, dataPage.definitionLevelByteLen(), count, maxDefinition, "definition");
        } else if (page instanceof Page.DataPageV2 dataPage) {
            data = physicalBuffer(dataPage.data());
            count = dataPage.numValues();
            if (count < 0) throw new ParquetException("Negative page value count: " + count);
            encoding = dataPage.encoding();
            repetitions = readLevels(dataPage.repetitionLevels(), count, descriptor.maxRepetitionLevel(), "repetition");
            definitions = readLevels(dataPage.definitionLevels(), count, maxDefinition, "definition");
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
            validateHybrid(data, width, present, "dictionary indexes");
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
            values = readPlain(data, present);
        } else if (encoding == Encoding.DELTA_BYTE_ARRAY && (descriptor.physicalType() == Type.BYTE_ARRAY
                || descriptor.physicalType() == Type.FIXED_LEN_BYTE_ARRAY)) {
            values = present == 0 && !data.hasRemaining()
                    ? new BinaryValues(new int[]{0}, data)
                    : DeltaByteArrayDecoder.decode(data, present,
                        descriptor.physicalType() == Type.FIXED_LEN_BYTE_ARRAY ? descriptor.typeLength() : 0);
        } else if (encoding == Encoding.DELTA_LENGTH_BYTE_ARRAY && descriptor.physicalType() == Type.BYTE_ARRAY) {
            values = DeltaLengthByteArrayDecoder.decode(data, present);
        } else if (encoding == Encoding.DELTA_BINARY_PACKED) {
            io.github.aloksingh.parquet.DeltaBinaryPackedDecoder delta = present == 0 && !data.hasRemaining() ? null
                    : DecodeChecks.validatedDelta(data, present, descriptor.physicalType() == Type.INT64);
            values = switch (descriptor.physicalType()) {
                case INT32 -> delta == null ? new int[0] : delta.decodeInt32(present);
                case INT64 -> delta == null ? new long[0] : delta.decodeInt64(present);
                default ->
                        throw new ParquetException("Unsupported DELTA_BINARY_PACKED type " + descriptor.physicalType());
            };
        } else if (encoding == Encoding.BYTE_STREAM_SPLIT) {
            values = readByteStreamSplit(data, present);
        } else if (encoding == Encoding.RLE && descriptor.physicalType() == Type.BOOLEAN) {
            if (present > 0 || data.hasRemaining()) DecodeChecks.requireBytes(data, 4, "boolean RLE length");
            int length = present == 0 && !data.hasRemaining() ? 0 : data.getInt();
            DecodeChecks.requireBytes(data, length, "boolean RLE payload");
            ByteBuffer rle = data.slice().order(ByteOrder.LITTLE_ENDIAN);
            rle.limit(length);
            data.position(data.position() + length);
            validateHybrid(rle, 1, present, "boolean values");
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

    private Object readPlain(ByteBuffer data, int count) {
        long required = switch (descriptor.physicalType()) {
            case BOOLEAN -> (count + 7L) / 8;
            case INT32, FLOAT, BYTE_ARRAY -> count * 4L;
            case INT64, DOUBLE -> count * 8L;
            case INT96 -> count * 12L;
            case FIXED_LEN_BYTE_ARRAY -> count * (long) descriptor.typeLength();
        };
        DecodeChecks.requireBytes(data, required, "PLAIN " + descriptor.physicalType());
        return switch (descriptor.physicalType()) {
            case INT32 -> {
                int[] values = new int[count];
                for (int i = 0; i < count; i++) values[i] = data.getInt();
                yield values;
            }
            case INT64 -> {
                long[] values = new long[count];
                for (int i = 0; i < count; i++) values[i] = data.getLong();
                yield values;
            }
            case FLOAT -> {
                float[] values = new float[count];
                for (int i = 0; i < count; i++) values[i] = data.getFloat();
                yield values;
            }
            case DOUBLE -> {
                double[] values = new double[count];
                for (int i = 0; i < count; i++) values[i] = data.getDouble();
                yield values;
            }
            case BOOLEAN -> {
                boolean[] values = new boolean[count];
                int packed = 0;
                for (int i = 0; i < count; i++) {
                    if ((i & 7) == 0) packed = data.get() & 0xff;
                    values[i] = (packed & (1 << (i & 7))) != 0;
                }
                yield values;
            }
            case FIXED_LEN_BYTE_ARRAY, INT96 -> {
                int width = descriptor.physicalType() == Type.INT96 ? 12 : descriptor.typeLength();
                int[] offsets = new int[count + 1];
                for (int i = 0; i < count; i++) offsets[i + 1] = Math.addExact(offsets[i], width);
                ByteBuffer payload = data.slice();
                payload.limit(offsets[count]);
                data.position(data.position() + offsets[count]);
                yield new BinaryValues(offsets, payload);
            }
            case BYTE_ARRAY -> readPlainBinary(data, count);
            default -> throw new ParquetException("Unsupported PLAIN type " + descriptor.physicalType());
        };
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

    private static BinaryValues readPlainBinary(ByteBuffer data, int count) {
        int[] offsets = new int[count + 1];
        ByteBuffer scan = data.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            DecodeChecks.requireBytes(scan, 4, "binary length");
            int length = scan.getInt();
            DecodeChecks.requireBytes(scan, length, "binary value");
            offsets[i + 1] = Math.addExact(offsets[i], length);
            scan.position(scan.position() + length);
        }
        ByteBuffer payload = ByteBuffer.allocate(offsets[count]);
        for (int i = 0; i < count; i++) {
            int length = data.getInt();
            payload.put(offsets[i], data, data.position(), length);
            data.position(data.position() + length);
        }
        return new BinaryValues(offsets, payload);
    }





    // Validate framing before handing a bounded stream to the reusable RLE reader.
    // In particular, a short packed run must not become zero-filled values.
    private static void validateHybrid(ByteBuffer source, int width, int count, String kind) {
        if (width < 0 || width > 32 || count < 0) {
            throw new ParquetException("Invalid " + kind + " width/count: " + width + "/" + count);
        }
        ByteBuffer data = source.duplicate();
        int decoded = 0;
        while (decoded < count) {
            long header = DecodeChecks.unsignedVarInt(data, kind);
            long run = header >>> 1;
            if (run == 0) throw new ParquetException("Zero-length " + kind + " run");
            if ((header & 1) == 0) {
                int bytes = (width + 7) / 8;
                DecodeChecks.requireBytes(data, bytes, kind);
                long value = 0;
                for (int i = 0; i < bytes; i++) value |= (data.get() & 0xffL) << (8 * i);
                if ((value >>> width) != 0) throw new ParquetException("Invalid " + kind + " value for width " + width);
                if (run > count - decoded) throw new ParquetException("Too many " + kind + " values");
                decoded += (int) run;
            } else {
                long values = run * 8;
                long bytes = run * width;
                DecodeChecks.requireBytes(data, bytes, kind);
                if (values > (long) count - decoded + 7) throw new ParquetException("Too many " + kind + " values");
                data.position(data.position() + (int) bytes);
                decoded += (int) Math.min(values, count - decoded);
            }
        }
        if (data.hasRemaining()) throw new ParquetException("Trailing " + kind + " bytes");
    }

    private static int[] readV1Levels(ByteBuffer data, int byteLength, int count, int maxLevel, String kind) {
        if (maxLevel == 0) {
            if (byteLength != 0) throw new ParquetException("Unexpected " + kind + " level section");
            return null;
        }
        if (count == 0 && byteLength == 0) return new int[0];
        if (byteLength < 4) throw new ParquetException("Missing " + kind + " level section");
        DecodeChecks.requireBytes(data, byteLength, kind + " level section");
        int length = data.getInt();
        if (length < 0 || 4L + length != byteLength) {
            throw new ParquetException("Invalid " + kind + " level section length: " + length + "/" + byteLength);
        }
        DecodeChecks.requireBytes(data, length, kind + " levels");
        ByteBuffer levels = data.slice().order(ByteOrder.LITTLE_ENDIAN);
        levels.limit(length);
        data.position(data.position() + length);
        return readLevels(levels, count, maxLevel, kind);
    }

    private static int[] readLevels(ByteBuffer data, int count, int maxLevel, String kind) {
        if (maxLevel == 0) {
            // Some legacy V2 writers explicitly encode the implicit zero levels.
            // Validate their count/framing rather than rejecting a correct width-zero stream.
            if (data != null && data.hasRemaining()) validateHybrid(data, 0, count, kind + " levels");
            return null;
        }
        if (data == null || !data.hasRemaining()) {
            if (count == 0) return new int[0];
            throw new ParquetException("Missing " + kind + " levels");
        }
        int width = 32 - Integer.numberOfLeadingZeros(maxLevel);
        validateHybrid(data, width, count, kind + " levels");
        int[] levels = new RleDecoder(data, width, count).readAll();
        for (int level : levels) {
            if (level < 0 || level > maxLevel) {
                throw new ParquetException("Invalid " + kind + " level " + level + ", maximum " + maxLevel);
            }
        }
        return levels;
    }
}
