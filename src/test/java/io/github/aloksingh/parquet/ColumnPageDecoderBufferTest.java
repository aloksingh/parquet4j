package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnPageDecoder;
import io.github.aloksingh.parquet.model.DecodedPage;
import io.github.aloksingh.parquet.model.Encoding;
import io.github.aloksingh.parquet.model.Page;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ColumnPageDecoderBufferTest {
    @ParameterizedTest
    @EnumSource(Type.class)
    void plainPagesAcceptHeapDirectReadOnlyAndPositionedBuffersWithoutChangingInput(Type type) {
        Object first = switch (type) {
            case BOOLEAN -> true;
            case INT32 -> -17;
            case INT64 -> -9_000_000_000L;
            case FLOAT -> Float.intBitsToFloat(0x7fc12345);
            case DOUBLE -> -0.0;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> new byte[]{0, -1, 66};
            case INT96 -> new byte[]{0, -1, 66, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        };
        ColumnDescriptor descriptor = DecodingTestSupport.descriptor(type, 2, 2);
        for (boolean v2 : new boolean[]{false, true}) {
            for (boolean direct : new boolean[]{false, true}) {
                for (boolean readOnly : new boolean[]{false, true}) {
                    Page source = DecodingTestSupport.page(v2, descriptor, Encoding.PLAIN,
                            new int[]{2, 1, 2}, new int[]{0, 1, 0}, DecodingTestSupport.plain(type, first, first));
                    ByteBuffer values;
                    ByteBuffer definitions = null;
                    ByteBuffer repetitions = null;
                    if (source instanceof Page.DataPage page) {
                        values = positioned(page.data(), direct, readOnly);
                        source = new Page.DataPage(values, page.numValues(), page.encoding(),
                                page.definitionLevelByteLen(), page.repetitionLevelByteLen());
                    } else {
                        Page.DataPageV2 page = (Page.DataPageV2) source;
                        values = positioned(page.data(), direct, readOnly);
                        definitions = positioned(page.definitionLevels(), direct, readOnly);
                        repetitions = positioned(page.repetitionLevels(), direct, readOnly);
                        source = new Page.DataPageV2(values, page.numValues(), page.numNulls(), page.numRows(),
                                page.encoding(), definitions, repetitions, false);
                    }
                    int position = values.position();
                    int limit = values.limit();
                    values.mark();
                    DecodedPage decoded = new ColumnPageDecoder(descriptor).decode(source);
                    assertEquals(position, values.position());
                    assertEquals(limit, values.limit());
                    assertEquals(position, values.reset().position());
                    assertEquals(ByteOrder.BIG_ENDIAN, values.order());
                    if (definitions != null) {
                        assertEquals(3, definitions.position());
                        assertEquals(3, repetitions.position());
                    }
                    assertEquals(3, decoded.numValues());
                    assertEquals(2, decoded.nonNullCount());
                    for (int physical = 0; physical < 2; physical++) {
                        if (first instanceof byte[] bytes)
                            assertArrayEquals(bytes, (byte[]) decoded.physicalValue(physical));
                        else if (first instanceof Float value) assertEquals(Float.floatToRawIntBits(value),
                                Float.floatToRawIntBits((Float) decoded.physicalValue(physical)));
                        else if (first instanceof Double value) assertEquals(Double.doubleToRawLongBits(value),
                                Double.doubleToRawLongBits((Double) decoded.physicalValue(physical)));
                        else assertEquals(first, decoded.physicalValue(physical));
                    }
                }
            }
        }
    }

    private static ByteBuffer positioned(ByteBuffer source, boolean direct, boolean readOnly) {
        int length = source.remaining();
        ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(length + 8) : ByteBuffer.allocate(length + 8);
        buffer.position(3).put(source.duplicate()).limit(3 + length).position(3);
        return (readOnly ? buffer.asReadOnlyBuffer() : buffer).order(ByteOrder.BIG_ENDIAN);
    }
}
