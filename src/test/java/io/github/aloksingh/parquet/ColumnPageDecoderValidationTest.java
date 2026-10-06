package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnPageDecoder;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

class ColumnPageDecoderValidationTest {
    @Test
    void dictionaryIndexesAreBoundedEvenForEmptyDictionaries() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 0, 0);
        for (int size : new int[]{0, 2}) {
            ColumnPageDecoder decoder = new ColumnPageDecoder(descriptor);
            decoder.setDictionary(new Page.DictionaryPage(size == 0 ? ByteBuffer.allocate(0)
                    : DecodingTestSupport.plain(Type.INT32, 17, 23), size, Encoding.PLAIN));
            ByteBuffer indexes = size == 0 ? ByteBuffer.wrap(new byte[]{0, 2})
                    : ByteBuffer.wrap(new byte[]{2, 2, 2});
            ParquetException error = assertThrows(ParquetException.class, () -> decoder.decode(
                    new Page.DataPage(indexes, 1, Encoding.RLE_DICTIONARY, 0, 0)));
            assertTrue(error.getMessage().contains("bounds"));
        }
    }

    @Test
    void unsupportedPhysicalEncodingsNeverBecomeNullOnlySuccess() {
        for (Type type : Type.values()) {
            ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 1, 0);
            for (Encoding encoding : Encoding.values()) {
                boolean supported = switch (encoding) {
                    case PLAIN, PLAIN_DICTIONARY, RLE_DICTIONARY -> true;
                    case RLE -> type == Type.BOOLEAN;
                    case DELTA_BINARY_PACKED -> type == Type.INT32 || type == Type.INT64;
                    case DELTA_LENGTH_BYTE_ARRAY -> type == Type.BYTE_ARRAY;
                    case DELTA_BYTE_ARRAY -> type == Type.BYTE_ARRAY || type == Type.FIXED_LEN_BYTE_ARRAY;
                    case BYTE_STREAM_SPLIT -> type == Type.INT32 || type == Type.INT64 || type == Type.FLOAT
                            || type == Type.DOUBLE || type == Type.FIXED_LEN_BYTE_ARRAY;
                    case BIT_PACKED -> false;
                };
                if (supported) continue;
                for (boolean v2 : new boolean[]{false, true}) {
                    Page page = DecodingTestSupport.page(v2, descriptor, encoding,
                            new int[]{0}, new int[]{0}, ByteBuffer.allocate(0));
                    assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(page),
                            type + ":" + encoding);
                }
            }
        }
    }

    @Test
    void missingPhysicalBuffersProduceParquetErrors() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 0, 0);
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(
                new Page.DataPage(null, 1, Encoding.PLAIN, 0, 0)));
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(
                new Page.DataPageV2(null, 1, 0, 1, Encoding.PLAIN,
                        ByteBuffer.allocate(0), ByteBuffer.allocate(0), false)));
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).setDictionary(
                new Page.DictionaryPage(null, 1, Encoding.PLAIN)));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Type.class)
    void negativeDictionaryCountsFailBeforeAllocation(Type type) {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 0, 0);
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).setDictionary(
                new Page.DictionaryPage(ByteBuffer.allocate(0), -1, Encoding.PLAIN)));
    }

    @Test
    void missingDictionaryWidthsAndMalformedBooleanFramesFailWithParquetErrors() {
        ColumnDescriptor integers = DecodingTestSupport.descriptor(Type.INT32, 0, 0);
        ColumnPageDecoder dictionary = new ColumnPageDecoder(integers);
        dictionary.setDictionary(new Page.DictionaryPage(DecodingTestSupport.plain(Type.INT32, 17), 1, Encoding.PLAIN));
        assertThrows(ParquetException.class, () -> dictionary.decode(
                new Page.DataPage(ByteBuffer.allocate(0), 1, Encoding.RLE_DICTIONARY, 0, 0)));
        ColumnDescriptor booleans = DecodingTestSupport.descriptor(Type.BOOLEAN, 0, 0);
        ByteBuffer[] invalidFrames = {
                ByteBuffer.wrap(new byte[]{1, 2, 3}),
                ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(-1).flip(),
                ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(1).flip(),
                ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(Integer.MAX_VALUE).flip()
        };
        for (ByteBuffer frame : invalidFrames) {
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(booleans).decode(
                    new Page.DataPage(frame, 1, Encoding.RLE, 0, 0)));
        }
    }

    @Test
    void negativePageCountsAndInvalidFlatRowCountsAreRejected() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 0, 0);
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(
                new Page.DataPage(ByteBuffer.allocate(0), -1, Encoding.PLAIN, 0, 0)));
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(
                new Page.DataPageV2(ByteBuffer.allocate(0), -1, 0, 0, Encoding.PLAIN,
                        ByteBuffer.allocate(0), ByteBuffer.allocate(0), false)));
        for (int rows : new int[]{-1, 0, 2}) {
            Page page = new Page.DataPageV2(DecodingTestSupport.plain(Type.INT32, 17), 1, 0, rows,
                    Encoding.PLAIN, ByteBuffer.allocate(0), ByteBuffer.allocate(0), false);
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(page));
        }
    }

    @Test
    void invalidColumnLevelsAndFixedWidthsAreRejectedAtConstruction() {
        ColumnDescriptor[] invalid = {
                new ColumnDescriptor(Type.INT32, new String[]{"c"}, -1, 0, 0),
                new ColumnDescriptor(Type.INT32, new String[]{"c"}, 0, -1, 0),
                new ColumnDescriptor(Type.FIXED_LEN_BYTE_ARRAY, new String[]{"c"}, 0, 0, 0),
                new ColumnDescriptor(Type.FIXED_LEN_BYTE_ARRAY, new String[]{"c"}, 0, 0, -3)
        };
        for (ColumnDescriptor descriptor : invalid) {
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor));
        }
    }

    @Test
    void fixedDeltaValuesMustHaveTheDeclaredWidth() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.FIXED_LEN_BYTE_ARRAY, 0, 0);
        ByteBuffer prefix = DecodingTestSupport.delta(0);
        ByteBuffer suffix = DecodingTestSupport.delta(2);
        ByteBuffer data = ByteBuffer.allocate(prefix.remaining() + suffix.remaining() + 2);
        data.put(prefix).put(suffix).put(new byte[]{1, 2}).flip();
        Page page = new Page.DataPage(data, 1, Encoding.DELTA_BYTE_ARRAY, 0, 0);
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(page));
    }

    @Test
    void binaryDeltaRejectsNegativeLengthsAndPrefixesLongerThanThePreviousValue() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.BYTE_ARRAY, 0, 0);
        ByteBuffer negativeLength = DecodingTestSupport.delta(-1);
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(
                new Page.DataPage(negativeLength, 1, Encoding.DELTA_LENGTH_BYTE_ARRAY, 0, 0)));
        int[][] invalidPrefixes = {{1}, {0, 2}, {0, -1}};
        int[][] lengths = {{0}, {1, 0}, {1, 1}};
        for (int index = 0; index < invalidPrefixes.length; index++) {
            ByteBuffer prefix = DecodingTestSupport.delta(invalidPrefixes[index]);
            ByteBuffer length = DecodingTestSupport.delta(lengths[index]);
            int suffixes = java.util.Arrays.stream(lengths[index]).sum();
            ByteBuffer data = ByteBuffer.allocate(prefix.remaining() + length.remaining() + suffixes);
            data.put(prefix).put(length).put(new byte[suffixes]).flip();
            Page page = new Page.DataPage(data, invalidPrefixes[index].length, Encoding.DELTA_BYTE_ARRAY, 0, 0);
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(page),
                    "Invalid prefix case " + index);
        }
    }

    @Test
    void deltaHeadersAndBodiesAreValidatedBeforeDecoding() {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 0, 0);
        byte[][] malformed = {
                {0, 0, 1, 0},
                {4, 1, 1, 0},
                {-128, 1, 0, 1, 0},
                {-128, 1, 3, 1, 0},
                {-128, 1, 8, 1, 0},
                {-128, 1, 4, 1, -128},
                {-128, 1, 4, 2, 0, 0, 1, 0, 0, 0},
                {-128, 1, 4, 2, 0, 0, 33, 0, 0, 0}
        };
        for (int index = 0; index < malformed.length; index++) {
            int count = index >= 6 ? 2 : 1;
            Page source = new Page.DataPage(ByteBuffer.wrap(malformed[index]), count,
                    Encoding.DELTA_BINARY_PACKED, 0, 0);
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(source),
                    "Malformed delta case " + index);
        }
    }

    static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> deltaEncodings() {
        return java.util.stream.Stream.of(
                org.junit.jupiter.params.provider.Arguments.of(Type.INT32, Encoding.DELTA_BINARY_PACKED),
                org.junit.jupiter.params.provider.Arguments.of(Type.INT64, Encoding.DELTA_BINARY_PACKED),
                org.junit.jupiter.params.provider.Arguments.of(Type.BYTE_ARRAY, Encoding.DELTA_LENGTH_BYTE_ARRAY),
                org.junit.jupiter.params.provider.Arguments.of(Type.BYTE_ARRAY, Encoding.DELTA_BYTE_ARRAY));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.MethodSource("deltaEncodings")
    void deltaCountsMustMatchPresentEventsRatherThanInventingLastValues(Type type, Encoding encoding) {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 0, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            ByteBuffer data;
            if (encoding == Encoding.DELTA_BINARY_PACKED) {
                data = DecodingTestSupport.consecutiveDelta(100, 1, 2);
            } else {
                ByteBuffer prefixes = encoding == Encoding.DELTA_BYTE_ARRAY
                        ? DecodingTestSupport.delta(0, 0) : ByteBuffer.allocate(0);
                ByteBuffer lengths = DecodingTestSupport.delta(1, 1);
                data = ByteBuffer.allocate(prefixes.remaining() + lengths.remaining() + 2);
                data.put(prefixes).put(lengths).put(new byte[]{'a', 'b'}).flip();
            }
            Page source = DecodingTestSupport.page(v2, descriptor, encoding, new int[3], new int[3], data);
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(source));
        }
    }

    @Test
    void v2LevelBuffersArePresentExactlyWhenTheDescriptorRequiresThem() {
        ColumnDescriptor required = DecodingTestSupport.descriptor(Type.INT32, 0, 0);
        Page extraDefinitions = new Page.DataPageV2(DecodingTestSupport.plain(Type.INT32, 17),
                1, 0, 1, Encoding.PLAIN, ByteBuffer.wrap(new byte[]{2, 0}), ByteBuffer.allocate(0), false);
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(required).decode(extraDefinitions));
        Page extraRepetitions = new Page.DataPageV2(DecodingTestSupport.plain(Type.INT32, 17),
                1, 0, 1, Encoding.PLAIN, ByteBuffer.allocate(0), ByteBuffer.wrap(new byte[]{2, 0}), false);
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(required).decode(extraRepetitions));
        ColumnDescriptor optional = DecodingTestSupport.descriptor(Type.INT32, 1, 0);
        for (ByteBuffer missing : new ByteBuffer[]{null, ByteBuffer.allocate(0)}) {
            Page missingDefinitions = new Page.DataPageV2(DecodingTestSupport.plain(Type.INT32, 17),
                    1, 0, 1, Encoding.PLAIN, missing, ByteBuffer.allocate(0), false);
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(optional).decode(missingDefinitions));
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {-1, 0, 5, 7})
    void v1LevelSectionLengthMustAgreeWithItsPrefix(int declaredBytes) {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 1, 0);
        ByteBuffer data = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(2).put((byte) 2).put((byte) 1).putInt(17).flip();
        Page page = new Page.DataPage(data, 1, Encoding.PLAIN, declaredBytes, 0);
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(page));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints = {-1, 0, 2, 4})
    void v2NullCountsMustMatchTheDefinitionStream(int declaredNulls) {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(Type.INT32, 1, 0);
        Page page = new Page.DataPageV2(DecodingTestSupport.plain(Type.INT32, 17, 23), 3,
                declaredNulls, 3, Encoding.PLAIN, DecodingTestSupport.levels(1, 1, 0, 1),
                ByteBuffer.allocate(0), false);
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(page));
    }

    static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> emptyEncodings() {
        java.util.List<org.junit.jupiter.params.provider.Arguments> cases = new java.util.ArrayList<>();
        for (Type type : Type.values()) {
            cases.add(org.junit.jupiter.params.provider.Arguments.of(type, Encoding.PLAIN));
            cases.add(org.junit.jupiter.params.provider.Arguments.of(type, Encoding.RLE_DICTIONARY));
        }
        for (Type type : new Type[]{Type.INT32, Type.INT64}) {
            cases.add(org.junit.jupiter.params.provider.Arguments.of(type, Encoding.DELTA_BINARY_PACKED));
        }
        for (Type type : new Type[]{Type.INT32, Type.INT64, Type.FLOAT, Type.DOUBLE}) {
            cases.add(org.junit.jupiter.params.provider.Arguments.of(type, Encoding.BYTE_STREAM_SPLIT));
        }
        cases.add(org.junit.jupiter.params.provider.Arguments.of(Type.BOOLEAN, Encoding.RLE));
        cases.add(org.junit.jupiter.params.provider.Arguments.of(Type.BYTE_ARRAY, Encoding.DELTA_LENGTH_BYTE_ARRAY));
        cases.add(org.junit.jupiter.params.provider.Arguments.of(Type.BYTE_ARRAY, Encoding.DELTA_BYTE_ARRAY));
        return cases.stream();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.MethodSource("emptyEncodings")
    void allAbsentEventsNeedNoPhysicalPayloadForSupportedEncodings(Type type, Encoding encoding) {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 2, 0);
        for (boolean v2 : new boolean[]{false, true}) {
            ColumnPageDecoder decoder = new ColumnPageDecoder(descriptor);
            if (encoding == Encoding.RLE_DICTIONARY) {
                decoder.setDictionary(new Page.DictionaryPage(ByteBuffer.allocate(0), 0, Encoding.PLAIN));
            }
            io.github.aloksingh.parquet.model.DecodedPage page = decoder.decode(DecodingTestSupport.page(
                    v2, descriptor, encoding, new int[]{0, 1, 0}, new int[3], ByteBuffer.allocate(0)));
            assertEquals(3, page.numValues());
            assertEquals(0, page.nonNullCount());
            assertThrows(IndexOutOfBoundsException.class, () -> page.physicalValue(0));
            if (encoding == Encoding.RLE_DICTIONARY) assertArrayEquals(new int[0], page.dictionaryIndices());
            else if (page.values() instanceof io.github.aloksingh.parquet.model.BinaryValues binary) {
                assertEquals(0, binary.size());
                assertArrayEquals(new int[]{0}, binary.offsets());
            } else assertEquals(0, java.lang.reflect.Array.getLength(page.values()));
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Type.class)
    void extraPlainValuesAreNotSilentlyIgnored(Type type) {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 0, 0);
        Object first = switch (type) {
            case BOOLEAN -> true;
            case INT32 -> 17;
            case INT64 -> 17L;
            case FLOAT -> 1.25f;
            case DOUBLE -> 1.25;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> new byte[]{1, 2, 3};
            case INT96 -> new byte[12];
        };
        Object[] values = type == Type.BOOLEAN
                ? new Object[]{true, true, true, true, true, true, true, true, true}
                : new Object[]{first, first};
        for (boolean v2 : new boolean[]{false, true}) {
            Page source = DecodingTestSupport.page(v2, descriptor, Encoding.PLAIN,
                    new int[]{0}, new int[]{0}, DecodingTestSupport.plain(type, values));
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).decode(source));
        }
        assertThrows(ParquetException.class, () -> new ColumnPageDecoder(descriptor).setDictionary(
                new Page.DictionaryPage(DecodingTestSupport.plain(type, values), 1, Encoding.PLAIN)));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(Type.class)
    void shortPlainPayloadsThrowParquetErrorsInsteadOfInventingValues(Type type) {
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 0, 0);
        Object value = switch (type) {
            case BOOLEAN -> true;
            case INT32 -> 17;
            case INT64 -> 17L;
            case FLOAT -> 1.25f;
            case DOUBLE -> 1.25;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> new byte[]{1, 2, 3};
            case INT96 -> new byte[12];
        };
        int count = type == Type.BOOLEAN ? 9 : 2;
        for (boolean v2 : new boolean[]{false, true}) {
            Page source = DecodingTestSupport.page(v2, descriptor, Encoding.PLAIN,
                    new int[count], new int[count], DecodingTestSupport.plain(type, value));
            ParquetException error = assertThrows(ParquetException.class,
                    () -> new ColumnPageDecoder(descriptor).decode(source));
            assertTrue(error.getMessage().contains("Truncated"), error::getMessage);
        }
    }

    @Test
    void truncatedPackedLevelIndexAndBooleanRunsNeverProduceZeroFilledValues() {
        for (boolean v2 : new boolean[]{false, true}) {
            ColumnDescriptor optional = DecodingTestSupport.descriptor(Type.INT32, 1, 0);
            Page levels;
            if (v2) {
                levels = new Page.DataPageV2(ByteBuffer.allocate(0), 1, 1, 1, Encoding.PLAIN,
                        ByteBuffer.wrap(new byte[]{3}), ByteBuffer.allocate(0), false);
            } else {
                ByteBuffer data = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(1).put((byte) 3).flip();
                levels = new Page.DataPage(data, 1, Encoding.PLAIN, 5, 0);
            }
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(optional).decode(levels));

            ColumnDescriptor required = DecodingTestSupport.descriptor(Type.INT32, 0, 0);
            ColumnPageDecoder dictionary = new ColumnPageDecoder(required);
            dictionary.setDictionary(new Page.DictionaryPage(DecodingTestSupport.plain(Type.INT32, 17, 23),
                    2, Encoding.PLAIN));
            Page indexes = DecodingTestSupport.page(v2, required, Encoding.RLE_DICTIONARY,
                    new int[]{0}, new int[]{0}, ByteBuffer.wrap(new byte[]{1, 3}));
            assertThrows(ParquetException.class, () -> dictionary.decode(indexes));

            ColumnDescriptor bool = DecodingTestSupport.descriptor(Type.BOOLEAN, 0, 0);
            ByteBuffer data = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(1).put((byte) 3).flip();
            Page booleans = DecodingTestSupport.page(v2, bool, Encoding.RLE,
                    new int[]{0}, new int[]{0}, data);
            assertThrows(ParquetException.class, () -> new ColumnPageDecoder(bool).decode(booleans));
        }
    }

    @Test
    void definitionAndRepetitionLevelsAboveTheDescriptorMaximumAreRejected() {
        for (boolean v2 : new boolean[]{false, true}) {
            ColumnDescriptor definitions = DecodingTestSupport.descriptor(Type.INT32, 2, 0);
            Page invalidDefinition = DecodingTestSupport.page(v2, definitions, Encoding.PLAIN,
                    new int[]{3}, new int[]{0}, ByteBuffer.allocate(0));
            ParquetException definitionError = assertThrows(ParquetException.class,
                    () -> new ColumnPageDecoder(definitions).decode(invalidDefinition));
            assertTrue(definitionError.getMessage().contains("definition"));

            ColumnDescriptor repetitions = DecodingTestSupport.descriptor(Type.INT32, 0, 2);
            Page invalidRepetition = DecodingTestSupport.page(v2, repetitions, Encoding.PLAIN,
                    new int[]{0}, new int[]{3}, DecodingTestSupport.plain(Type.INT32, 17));
            ParquetException repetitionError = assertThrows(ParquetException.class,
                    () -> new ColumnPageDecoder(repetitions).decode(invalidRepetition));
            assertTrue(repetitionError.getMessage().contains("repetition"));
        }
    }
}
