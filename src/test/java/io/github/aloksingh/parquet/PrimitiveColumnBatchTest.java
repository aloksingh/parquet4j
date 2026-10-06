package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.lang.reflect.Array;
import java.nio.Buffer;
import java.util.BitSet;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PrimitiveColumnBatchTest {
    static Stream<Arguments> numericValues() {
        return Stream.of(
                Arguments.of(Type.INT32, new int[]{10, 0, 20}, 10, "intValues"),
                Arguments.of(Type.INT64, new long[]{10, 0, 20}, 10L, "longValues"),
                Arguments.of(Type.FLOAT, new float[]{1.25f, 0, 2.5f}, 1.25f, "floatValues"),
                Arguments.of(Type.DOUBLE, new double[]{1.25, 0, 2.5}, 1.25, "doubleValues"),
                Arguments.of(Type.BOOLEAN, new boolean[]{false, false, true}, false, "booleanValues"));
    }

    @ParameterizedTest
    @MethodSource("numericValues")
    void primitiveBatchesPreserveValidityAndOwnTheirBuffers(Type type, Object values,
                                                            Object first, String accessor) throws Exception {
        ColumnDescriptor descriptor = new ColumnDescriptor(type, new String[]{"v"}, 1, 0, 0);
        BitSet present = new BitSet();
        present.set(0);
        present.set(2);
        Class<?> batchClass;
        try {
            batchClass = Class.forName("io.github.aloksingh.parquet.model.ColumnBatch");
        } catch (ClassNotFoundException e) {
            org.junit.jupiter.api.Assertions.fail("Primitive column batches are missing", e);
            return;
        }
        Object batch = batchClass.getMethod("of", ColumnDescriptor.class, Object.class, BitSet.class)
                .invoke(null, descriptor, values, present);
        assertEquals(3, batchClass.getMethod("size").invoke(batch));
        assertEquals(first, batchClass.getMethod("getObject", int.class).invoke(batch, 0));
        assertNull(batchClass.getMethod("getObject", int.class).invoke(batch, 1));
        present.clear();
        Array.set(values, 0, type == Type.BOOLEAN ? true : zero(type));
        assertEquals(first, batchClass.getMethod("getObject", int.class).invoke(batch, 0));
        Object nativeValues = batchClass.getMethod(accessor).invoke(batch);
        if (nativeValues instanceof Buffer buffer) {
            assertTrue(buffer.isReadOnly());
        } else {
            boolean[] copied = (boolean[]) nativeValues;
            copied[0] = true;
            assertEquals(first, batchClass.getMethod("getObject", int.class).invoke(batch, 0));
        }
    }

    private static Object zero(Type type) {
        return switch (type) {
            case INT32 -> 0;
            case INT64 -> 0L;
            case FLOAT -> 0f;
            case DOUBLE -> 0d;
            default -> throw new IllegalArgumentException();
        };
    }
}
