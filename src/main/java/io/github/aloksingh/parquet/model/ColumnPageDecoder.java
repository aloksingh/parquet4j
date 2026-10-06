package io.github.aloksingh.parquet.model;

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
        requireConsumed(data, "dictionary values");
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
            if (present > 0) requireBytes(data, 1, "dictionary bit width");
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
                    ? new BinaryValues(new int[]{0}, data) : readDeltaBinary(data, present);
        } else if (encoding == Encoding.DELTA_LENGTH_BYTE_ARRAY && descriptor.physicalType() == Type.BYTE_ARRAY) {
            int[] lengths = present == 0 && !data.hasRemaining() ? new int[0]
                    : deltaDecoder(data, present, false).decodeInt32(present);
            int[] offsets = new int[present + 1];
            for (int i = 0; i < present; i++) {
                if (lengths[i] < 0) throw new ParquetException("Negative binary length: " + lengths[i]);
                offsets[i + 1] = checkedSize((long) offsets[i] + lengths[i], "binary payload");
            }
            requireBytes(data, offsets[present], "binary payload");
            ByteBuffer payload = data.slice();
            payload.limit(offsets[present]);
            data.position(data.position() + offsets[present]);
            values = new BinaryValues(offsets, payload);
        } else if (encoding == Encoding.DELTA_BINARY_PACKED) {
            io.github.aloksingh.parquet.DeltaBinaryPackedDecoder delta = present == 0 && !data.hasRemaining() ? null
                    : deltaDecoder(data, present, descriptor.physicalType() == Type.INT64);
            values = switch (descriptor.physicalType()) {
                case INT32 -> delta == null ? new int[0] : delta.decodeInt32(present);
                case INT64 -> delta == null ? new long[0] : delta.decodeInt64(present);
                default ->
                        throw new ParquetException("Unsupported DELTA_BINARY_PACKED type " + descriptor.physicalType());
            };
        } else if (encoding == Encoding.BYTE_STREAM_SPLIT) {
            values = readByteStreamSplit(data, present);
        } else if (encoding == Encoding.RLE && descriptor.physicalType() == Type.BOOLEAN) {
            if (present > 0 || data.hasRemaining()) requireBytes(data, 4, "boolean RLE length");
            int length = present == 0 && !data.hasRemaining() ? 0 : data.getInt();
            requireBytes(data, length, "boolean RLE payload");
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
        requireConsumed(data, "physical values");
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
        requireBytes(data, required, "PLAIN " + descriptor.physicalType());
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

    private static io.github.aloksingh.parquet.DeltaBinaryPackedDecoder deltaDecoder(
            ByteBuffer source, int expected, boolean is64Bit) {
        ByteBuffer data = source.duplicate();
        long block = unsignedVarInt(data, "delta block size");
        long minis = unsignedVarInt(data, "delta miniblock count");
        long encoded = unsignedVarInt(data, "delta value count");
        if (block == 0 || block > Integer.MAX_VALUE || block % 128 != 0 || minis == 0
                || minis > block || block % minis != 0 || (block / minis) % 32 != 0) {
            throw new ParquetException("Invalid delta block/miniblock size: " + block + "/" + minis);
        }
        if (encoded != expected) {
            throw new ParquetException("Delta value count " + encoded + " does not match present count " + expected);
        }
        zigzagVarLong(data, is64Bit, "delta first value");
        int decoded = expected == 0 ? 0 : 1;
        while (decoded < expected) {
            zigzagVarLong(data, is64Bit, "minimum delta");
            requireBytes(data, minis, "delta miniblock widths");
            int widths = data.position();
            data.position(widths + (int) minis);
            for (int mini = 0; mini < minis && decoded < expected; mini++) {
                int width = data.get(widths + mini) & 0xff;
                if (width > (is64Bit ? 64 : 32)) throw new ParquetException("Invalid delta bit width: " + width);
                long bytes = (block / minis) * width / 8;
                requireBytes(data, bytes, "delta miniblock");
                data.position(data.position() + (int) bytes);
                decoded += (int) Math.min(block / minis, expected - decoded);
            }
        }
        return new io.github.aloksingh.parquet.DeltaBinaryPackedDecoder(source, is64Bit);
    }

    private static long zigzagVarLong(ByteBuffer data, boolean is64Bit, String kind) {
        long encoded;
        if (!is64Bit) {
            encoded = unsignedVarInt(data, kind);
        } else {
            encoded = 0;
            boolean complete = false;
            for (int shift = 0; shift <= 63; shift += 7) {
                requireBytes(data, 1, kind + " varint");
                int value = data.get() & 0xff;
                if (shift == 63 && (value & 0xfe) != 0) throw new ParquetException("Invalid " + kind + " varint");
                encoded |= (long) (value & 0x7f) << shift;
                if ((value & 0x80) == 0) {
                    complete = true;
                    break;
                }
            }
            if (!complete) throw new ParquetException("Invalid " + kind + " varint");
        }
        return (encoded >>> 1) ^ -(encoded & 1);
    }

    private BinaryValues readDeltaBinary(ByteBuffer data, int count) {
        int[] prefixes = deltaDecoder(data, count, false).decodeInt32(count);
        int[] lengths = deltaDecoder(data, count, false).decodeInt32(count);
        int[] offsets = new int[count + 1];
        long suffixBytes = 0;
        for (int i = 0; i < count; i++) {
            int previous = i == 0 ? 0 : offsets[i] - offsets[i - 1];
            if (prefixes[i] < 0 || prefixes[i] > previous) {
                throw new ParquetException("Invalid binary prefix " + prefixes[i] + ", previous length " + previous);
            }
            if (lengths[i] < 0) throw new ParquetException("Negative binary suffix length: " + lengths[i]);
            if (descriptor.physicalType() == Type.FIXED_LEN_BYTE_ARRAY
                    && (long) prefixes[i] + lengths[i] != descriptor.typeLength()) {
                throw new ParquetException("Fixed binary value width does not match " + descriptor.typeLength());
            }
            offsets[i + 1] = checkedSize((long) offsets[i] + prefixes[i] + lengths[i], "binary payload");
            suffixBytes += lengths[i];
        }
        requireBytes(data, suffixBytes, "binary suffixes");
        ByteBuffer payload = ByteBuffer.allocate(offsets[count]);
        for (int i = 0; i < count; i++) {
            int prefix = prefixes[i];
            if (prefix > 0) {
                for (int j = 0; j < prefix; j++) payload.put(offsets[i] + j, payload.get(offsets[i - 1] + j));
            }
            payload.put(offsets[i] + prefix, data, data.position(), lengths[i]);
            data.position(data.position() + lengths[i]);
        }
        return new BinaryValues(offsets, payload);
    }

    private Object readByteStreamSplit(ByteBuffer data, int count) {
        int width = switch (descriptor.physicalType()) {
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            case FIXED_LEN_BYTE_ARRAY -> descriptor.typeLength();
            default -> throw new ParquetException("Unsupported BYTE_STREAM_SPLIT type " + descriptor.physicalType());
        };
        requireBytes(data, count * (long) width, "BYTE_STREAM_SPLIT values");
        int start = data.position();
        Object result = switch (descriptor.physicalType()) {
            case INT32 -> {
                int[] values = new int[count];
                for (int i = 0; i < count; i++) values[i] = (int) splitBits(data, start, count, 4, i);
                yield values;
            }
            case INT64 -> {
                long[] values = new long[count];
                for (int i = 0; i < count; i++) values[i] = splitBits(data, start, count, 8, i);
                yield values;
            }
            case FLOAT -> {
                float[] values = new float[count];
                for (int i = 0; i < count; i++)
                    values[i] = Float.intBitsToFloat((int) splitBits(data, start, count, 4, i));
                yield values;
            }
            case DOUBLE -> {
                double[] values = new double[count];
                for (int i = 0; i < count; i++)
                    values[i] = Double.longBitsToDouble(splitBits(data, start, count, 8, i));
                yield values;
            }
            case FIXED_LEN_BYTE_ARRAY -> {
                int[] offsets = new int[count + 1];
                ByteBuffer payload = ByteBuffer.allocate(checkedSize(count * (long) width, "fixed binary payload"));
                for (int i = 0; i < count; i++) {
                    offsets[i + 1] = offsets[i] + width;
                    for (int lane = 0; lane < width; lane++)
                        payload.put(i * width + lane, data.get(start + lane * count + i));
                }
                yield new BinaryValues(offsets, payload);
            }
            default -> throw new ParquetException("Unsupported BYTE_STREAM_SPLIT type " + descriptor.physicalType());
        };
        data.position(start + count * width);
        return result;
    }

    private static long splitBits(ByteBuffer data, int start, int count, int width, int index) {
        long bits = 0;
        for (int lane = 0; lane < width; lane++) {
            bits |= (data.get(start + lane * count + index) & 0xffL) << (lane * 8);
        }
        return bits;
    }

    private static BinaryValues readPlainBinary(ByteBuffer data, int count) {
        int[] offsets = new int[count + 1];
        ByteBuffer scan = data.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            requireBytes(scan, 4, "binary length");
            int length = scan.getInt();
            requireBytes(scan, length, "binary value");
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

    private static int checkedSize(long size, String kind) {
        if (size < 0 || size > Integer.MAX_VALUE) throw new ParquetException("Invalid " + kind + " size: " + size);
        return (int) size;
    }

    private static void requireConsumed(ByteBuffer data, String kind) {
        if (data.hasRemaining()) throw new ParquetException("Trailing " + kind + " bytes: " + data.remaining());
    }

    private static void requireBytes(ByteBuffer data, long count, String kind) {
        if (count < 0 || count > data.remaining()) {
            throw new ParquetException("Truncated " + kind + ": need " + count + " bytes, remaining " + data.remaining());
        }
    }

    private static long unsignedVarInt(ByteBuffer data, String kind) {
        long value = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            requireBytes(data, 1, kind + " varint");
            int b = data.get() & 0xff;
            if (shift == 28 && (b & 0xf0) != 0) {
                throw new ParquetException("Invalid " + kind + " varint");
            }
            value |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) return value;
        }
        throw new ParquetException("Invalid " + kind + " varint");
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
            long header = unsignedVarInt(data, kind);
            long run = header >>> 1;
            if (run == 0) throw new ParquetException("Zero-length " + kind + " run");
            if ((header & 1) == 0) {
                int bytes = (width + 7) / 8;
                requireBytes(data, bytes, kind);
                long value = 0;
                for (int i = 0; i < bytes; i++) value |= (data.get() & 0xffL) << (8 * i);
                if ((value >>> width) != 0) throw new ParquetException("Invalid " + kind + " value for width " + width);
                if (run > count - decoded) throw new ParquetException("Too many " + kind + " values");
                decoded += (int) run;
            } else {
                long values = run * 8;
                long bytes = run * width;
                requireBytes(data, bytes, kind);
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
        requireBytes(data, byteLength, kind + " level section");
        int length = data.getInt();
        if (length < 0 || 4L + length != byteLength) {
            throw new ParquetException("Invalid " + kind + " level section length: " + length + "/" + byteLength);
        }
        requireBytes(data, length, kind + " levels");
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
