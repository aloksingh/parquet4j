package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.ColumnStatistics;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Strict statistics decoding for known signed primitive order; never interpret binary as UTF-8.
 */
final class PredicateStatistics {
    private PredicateStatistics() {
    }

    static boolean canDrop(LogicalColumnDescriptor column, FilterOperator operator, Object constant,
                           ColumnStatistics statistics, long numValues) {
        if (statistics == null || !column.isPrimitive() || numValues < -1) return false;
        var descriptor = column.getPhysicalDescriptor();
        if (descriptor == null || descriptor.maxRepetitionLevel() != 0) return false;
        Type type = column.getPhysicalType();
        if (type == null || type == Type.BYTE_ARRAY || type == Type.FIXED_LEN_BYTE_ARRAY
                || type == Type.INT96) return false;
        Long nulls = statistics.nullCount();
        Long distinct = statistics.distinctCount();
        if (nulls != null && (nulls < 0 || numValues >= 0 && nulls > numValues)) return false;
        if (distinct != null && (distinct < 0 || numValues >= 0 && distinct > numValues)) return false;
        boolean minPresent = statistics.min() != null;
        boolean maxPresent = statistics.max() != null;
        if (minPresent != maxPresent) return false;
        Object min = null;
        Object max = null;
        if (minPresent) {
            min = decode(statistics.min(), type);
            max = decode(statistics.max(), type);
            if (min == null || max == null || TypedColumnFilter.isNaN(min)
                    || TypedColumnFilter.isNaN(max) || TypedColumnFilter.compareValues(min, max) > 0) return false;
        }
        boolean allNull = numValues >= 0 && nulls != null && nulls == numValues;
        if (allNull && minPresent) return false; // Contradictory metadata cannot prove anything.
        if (operator == FilterOperator.isNull) return nulls != null && nulls == 0;
        if (operator == FilterOperator.isNotNull) return allNull;
        if (TypedColumnFilter.isNaN(constant)) return false;
        if (allNull) return true;
        // This format has no nan_count. Keep floating ordered/inequality predicates unless the
        // all-null count proved them impossible, independent of the residual NaN policy.
        if ((type == Type.FLOAT || type == Type.DOUBLE) && operator != FilterOperator.eq) return false;
        if (!minPresent) return false;
        int minCompare = TypedColumnFilter.compareValues(min, constant);
        int maxCompare = TypedColumnFilter.compareValues(max, constant);
        return switch (operator) {
            case eq -> minCompare > 0 || maxCompare < 0;
            // Parquet bounds may omit NaNs. NaNs match !=, so finite equal bounds are not enough.
            case neq -> type != Type.FLOAT && type != Type.DOUBLE && minCompare == 0 && maxCompare == 0;
            case lt -> minCompare >= 0;
            case lte -> minCompare > 0;
            case gt -> maxCompare <= 0;
            case gte -> maxCompare < 0;
            default -> false;
        };
    }

    private static Object decode(byte[] bytes, Type type) {
        int width = switch (type) {
            case BOOLEAN -> 1;
            case INT32, FLOAT -> 4;
            case INT64, DOUBLE -> 8;
            default -> -1;
        };
        if (bytes.length != width) return null;
        if (type == Type.BOOLEAN) {
            if (bytes[0] == 0) return Boolean.FALSE;
            if (bytes[0] == 1) return Boolean.TRUE;
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        return switch (type) {
            case INT32 -> buffer.getInt();
            case INT64 -> buffer.getLong();
            case FLOAT -> buffer.getFloat();
            case DOUBLE -> buffer.getDouble();
            default -> null;
        };
    }
}
