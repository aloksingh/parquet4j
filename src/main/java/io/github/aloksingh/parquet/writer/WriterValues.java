package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Internal physical-value validation shared by primitive and MAP leaves.
 */
public final class WriterValues {
    private WriterValues() {
    }

    public static void validate(ColumnDescriptor column, Object value, boolean nullable) {
        if (value == null) {
            if (!nullable) throw invalid(column, "null is not allowed");
            return;
        }
        switch (column.physicalType()) {
            case BOOLEAN -> {
                if (!(value instanceof Boolean)) throw invalid(column, "expected Boolean");
            }
            case INT32 -> {
                long integer = integer(column, value);
                if (integer < Integer.MIN_VALUE || integer > Integer.MAX_VALUE) {
                    throw invalid(column, "integer outside INT32 range");
                }
            }
            case INT64 -> integer(column, value);
            case FLOAT, DOUBLE -> {
                if (!isNumber(value)) throw invalid(column, "expected a numeric value");
                Number number = (Number) value;
                double doubleValue = number.doubleValue();
                if ((value instanceof BigDecimal || value instanceof BigInteger)
                        && Double.isInfinite(doubleValue)) {
                    throw invalid(column, "numeric value outside floating-point range");
                }
                if (doubleValue == 0.0 && value instanceof BigDecimal decimal && decimal.signum() != 0) {
                    throw invalid(column, "nonzero numeric value underflows DOUBLE range");
                }
                if (column.physicalType() == Type.FLOAT && Double.isFinite(doubleValue)
                        && (Float.isInfinite(number.floatValue())
                        || (doubleValue != 0.0 && number.floatValue() == 0.0f))) {
                    throw invalid(column, "numeric value outside FLOAT range");
                }
            }
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                int length;
                if (value instanceof byte[] bytes) length = bytes.length;
                else if (value instanceof ByteBuffer buffer) length = buffer.remaining();
                else if (value instanceof String string) length = string.getBytes(StandardCharsets.UTF_8).length;
                else throw invalid(column, "expected byte[], ByteBuffer, or String");
                if (column.physicalType() == Type.FIXED_LEN_BYTE_ARRAY && length != column.typeLength()) {
                    throw invalid(column, "expected exactly " + column.typeLength() + " bytes, got " + length);
                }
            }
            case INT96 -> throw invalid(column, "INT96 writing is not supported");
        }
    }

    /**
     * Validate and detach all mutable value representations before acceptance.
     */
    public static Object snapshot(ColumnDescriptor column, Object value, boolean nullable) {
        validate(column, value, nullable);
        if (value == null) return null;
        return switch (column.physicalType()) {
            case BOOLEAN -> value;
            case INT32 -> (int) integer(column, value);
            case INT64 -> integer(column, value);
            case FLOAT -> ((Number) value).floatValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                if (value instanceof byte[] bytes) yield bytes.clone();
                if (value instanceof String string) yield string.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ((ByteBuffer) value).duplicate();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                yield bytes;
            }
            case INT96 -> throw invalid(column, "INT96 writing is not supported");
        };
    }

    private static boolean isNumber(Object value) {
        return value instanceof Byte || value instanceof Short || value instanceof Integer
                || value instanceof Long || value instanceof Float || value instanceof Double
                || value instanceof BigInteger || value instanceof BigDecimal;
    }

    private static long integer(ColumnDescriptor column, Object value) {
        try {
            if (value instanceof Byte || value instanceof Short || value instanceof Integer
                    || value instanceof Long) return ((Number) value).longValue();
            if (value instanceof BigInteger integer) return integer.longValueExact();
            if (value instanceof BigDecimal decimal) return decimal.longValueExact();
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException(column.getPathString() + ": integer outside INT64 range or fractional", exception);
        }
        throw invalid(column, "expected an exact integral value");
    }

    private static IllegalArgumentException invalid(ColumnDescriptor column, String reason) {
        return new IllegalArgumentException(column.getPathString() + ": " + reason);
    }
}
