package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.parquet.format.Statistics;

/**
 * Exact bounds/null counts and bounded, exact-or-omitted distinct counts.
 */
public final class WriterStatistics {
    private static final int DISTINCT_LIMIT = 4096;
    private static final long DISTINCT_BYTE_LIMIT = 1024 * 1024;
    private final Type type;
    private Object min;
    private Object max;
    private long nullCount;
    private Set<Object> distinct = new HashSet<>();
    private long distinctBytes;

    public WriterStatistics(Type type) {
        this.type = type;
    }

    public void addNull() {
        nullCount = Math.addExact(nullCount, 1);
    }

    /**
     * Values are already checked against their physical descriptor.
     */
    public void add(Object value) {
        if (value == null) {
            addNull();
            return;
        }
        Object canonical = canonical(value);
        if (distinct != null && distinct.add(canonical instanceof byte[] bytes
                ? ByteBuffer.wrap(bytes).asReadOnlyBuffer() : canonical)) {
            distinctBytes += canonical instanceof byte[] bytes ? bytes.length : 8;
            if (distinct.size() > DISTINCT_LIMIT || distinctBytes > DISTINCT_BYTE_LIMIT) {
                distinct = null;
                distinctBytes = 0;
            }
        }
        if ((type == Type.FLOAT && Float.isNaN((Float) canonical))
                || (type == Type.DOUBLE && Double.isNaN((Double) canonical))) return;
        if (min == null || compare(canonical, min) < 0) min = canonical;
        if (max == null || compare(canonical, max) > 0) max = canonical;
    }

    public void clear() {
        min = null;
        max = null;
        nullCount = 0;
        if (distinct == null) distinct = new HashSet<>();
        else distinct.clear();
        distinctBytes = 0;
    }

    public void merge(WriterStatistics row) {
        if (type != row.type) throw new IllegalArgumentException("Cannot merge statistics of different physical types");
        nullCount = Math.addExact(nullCount, row.nullCount);
        if (row.min != null && (min == null || compare(row.min, min) < 0)) min = row.min;
        if (row.max != null && (max == null || compare(row.max, max) > 0)) max = row.max;
        if (distinct != null) {
            if (row.distinct == null) {
                distinct = null;
                distinctBytes = 0;
            } else {
                for (Object value : row.distinct) {
                    if (distinct.add(value)) distinctBytes += value instanceof ByteBuffer bytes ? bytes.remaining() : 8;
                    if (distinct.size() > DISTINCT_LIMIT || distinctBytes > DISTINCT_BYTE_LIMIT) {
                        distinct = null;
                        distinctBytes = 0;
                        break;
                    }
                }
            }
        }
    }

    public long nullCount() {
        return nullCount;
    }

    public Statistics toParquet() {
        Statistics result = new Statistics();
        result.setNull_count(nullCount);
        if (distinct != null) result.setDistinct_count(distinct.size());
        boolean signedOrder = type != Type.BYTE_ARRAY && type != Type.FIXED_LEN_BYTE_ARRAY;
        if (min != null) {
            byte[] bytes = encode(zeroBound(min, true));
            result.setMin_value(bytes);
            if (signedOrder) result.setMin(bytes);
        }
        if (max != null) {
            byte[] bytes = encode(zeroBound(max, false));
            result.setMax_value(bytes);
            if (signedOrder) result.setMax(bytes);
        }
        return result;
    }

    /**
     * parquet.thrift TypeDefinedOrder: when a computed FLOAT/DOUBLE bound is zero (either sign),
     * the min field must carry -0.0 and the max field must carry +0.0, so signed comparisons in
     * readers always see a correct bound regardless of the zero signs present in the data.
     */
    private Object zeroBound(Object value, boolean minimum) {
        if (type == Type.FLOAT && (Float) value == 0.0f) {
            return minimum ? (Object) (-0.0f) : (Object) 0.0f;
        }
        if (type == Type.DOUBLE && (Double) value == 0.0d) {
            return minimum ? (Object) (-0.0d) : (Object) 0.0d;
        }
        return value;
    }

    private Object canonical(Object value) {
        return switch (type) {
            case BOOLEAN -> (Boolean) value;
            case INT32 -> ((Number) value).intValue();
            case INT64 -> ((Number) value).longValue();
            case FLOAT -> ((Number) value).floatValue();
            case DOUBLE -> ((Number) value).doubleValue();
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                if (value instanceof byte[] bytes) yield bytes;
                if (value instanceof String string) yield string.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ((ByteBuffer) value).duplicate();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                yield bytes;
            }
            case INT96 -> throw new IllegalArgumentException("INT96 writing is not supported");
        };
    }

    private int compare(Object first, Object second) {
        return switch (type) {
            case BOOLEAN -> Boolean.compare((Boolean) first, (Boolean) second);
            case INT32 -> Integer.compare((Integer) first, (Integer) second);
            case INT64 -> Long.compare((Long) first, (Long) second);
            case FLOAT -> Float.compare((Float) first, (Float) second);
            case DOUBLE -> Double.compare((Double) first, (Double) second);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> Arrays.compareUnsigned((byte[]) first, (byte[]) second);
            case INT96 -> throw new IllegalArgumentException("INT96 writing is not supported");
        };
    }

    private byte[] encode(Object value) {
        return switch (type) {
            case BOOLEAN -> new byte[]{(byte) ((Boolean) value ? 1 : 0)};
            case INT32 -> ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt((Integer) value).array();
            case INT64 -> ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong((Long) value).array();
            case FLOAT -> ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat((Float) value).array();
            case DOUBLE -> ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble((Double) value).array();
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> (byte[]) value;
            case INT96 -> throw new IllegalArgumentException("INT96 writing is not supported");
        };
    }
}
