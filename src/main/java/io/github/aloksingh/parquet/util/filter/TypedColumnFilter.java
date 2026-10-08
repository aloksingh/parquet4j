package io.github.aloksingh.parquet.util.filter;

import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.PrimitiveLogicalType;
import io.github.aloksingh.parquet.model.Type;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Shared binding and evaluation for built-in predicates. Constants are converted once, before
 * iteration. Ordinary value comparisons never match a null selected value. For compatibility,
 * {@code eq(null)} is an alias for {@code isNull}, and {@code neq(null)} for {@code isNotNull}.
 * For keyed MAP predicates a null map, missing key and present-null value all select null.
 * FLOAT/DOUBLE use exact physical IEEE values: signed zeros compare equal, NaN never matches
 * equality or ordering, and inequality matches NaN (including NaN != NaN). Infinities are
 * supported. Binary comparison is by content, ordered unsigned lexicographically. Strings
 * use Java String order. Unsigned INTEGER(32/64) predicates use logical Long/BigInteger values.
 * There is no tolerance or decimal conversion during row evaluation.
 */
abstract class TypedColumnFilter implements ColumnFilter {
    protected final LogicalColumnDescriptor targetColumnDescriptor;
    protected final Object matchValue;
    protected final Optional<String> mapKey;
    protected final FilterOperator operator;
    protected final Type valueType;

    TypedColumnFilter(LogicalColumnDescriptor column, FilterOperator operator, Object constant,
                      Optional<String> mapKey) {
        if (column == null || operator == null || mapKey == null) {
            throw new IllegalArgumentException("Column, operator and mapKey must not be null");
        }
        if (mapKey.isPresent() && !column.isMap()) {
            throw new IllegalArgumentException("Column '" + column.getName() + "' is not a MAP");
        }
        this.targetColumnDescriptor = column;
        this.mapKey = mapKey;
        this.operator = constant == null && operator == FilterOperator.eq ? FilterOperator.isNull
                : constant == null && operator == FilterOperator.neq ? FilterOperator.isNotNull : operator;
        if (this.operator == FilterOperator.isNull || this.operator == FilterOperator.isNotNull) {
            if (constant != null) {
                throw invalid("null checks do not take a constant");
            }
            this.valueType = null;
            this.matchValue = null;
            return;
        }
        if (constant == null) {
            throw invalid("null constants require isNull() or isNotNull()");
        }
        boolean scalar = column.isPrimitive() || mapKey.isPresent();
        if (scalar) {
            if (mapKey.isPresent() && column.getMapMetadata() == null) {
                throw invalid("MAP value metadata is missing");
            }
            this.valueType = mapKey.isPresent() ? column.getMapMetadata().valueType()
                    : column.getPhysicalType();
            this.matchValue = ColumnFilterHelper.CFH.convertToColumnType(column, constant, mapKey);
        } else if (column.isList()) {
            if (operator == FilterOperator.eq || operator == FilterOperator.neq) {
                if (!(constant instanceof List<?>)) throw invalid("expected a LIST constant");
                this.valueType = null;
                this.matchValue = constant;
            } else if (operator == FilterOperator.contains && column.getListMetadata() != null) {
                this.valueType = column.getListMetadata().elementType();
                this.matchValue = ColumnFilterHelper.CFH.convertToClassType(javaClass(valueType), constant);
            } else {
                throw invalid("operator requires a scalar column or LIST element metadata");
            }
        } else if (column.isMap()) {
            if (operator == FilterOperator.eq || operator == FilterOperator.neq) {
                if (!(constant instanceof Map<?, ?>)) throw invalid("expected a MAP constant");
                this.valueType = null;
                this.matchValue = constant;
            } else if (operator == FilterOperator.contains && column.getMapMetadata() != null) {
                this.valueType = column.getMapMetadata().valueType();
                this.matchValue = ColumnFilterHelper.CFH.convertToClassType(javaClass(valueType), constant);
            } else {
                throw invalid("operator requires a MAP key or MAP value metadata");
            }
        } else {
            throw invalid("unsupported column type " + column.getLogicalType());
        }
        if (scalar && (operator == FilterOperator.contains || operator == FilterOperator.prefix
                || operator == FilterOperator.suffix)) {
            if (valueType != Type.BYTE_ARRAY || !(matchValue instanceof String)) {
                throw invalid("string operators require a BYTE_ARRAY string constant");
            }
        }
        if ((operator == FilterOperator.lt || operator == FilterOperator.lte
                || operator == FilterOperator.gt || operator == FilterOperator.gte)
                && (!scalar || !(matchValue instanceof Comparable<?> || matchValue instanceof byte[]))) {
            throw invalid("ordered comparisons require a comparable scalar constant");
        }
    }

    protected final IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("Invalid " + operator + " predicate for column '"
                + targetColumnDescriptor.getName() + "': " + message);
    }

    private static Class<?> javaClass(Type type) {
        if (type == null) throw new IllegalArgumentException("Column value type is missing");
        return switch (type) {
            case BOOLEAN -> Boolean.class;
            case INT32 -> Integer.class;
            case INT64 -> Long.class;
            case FLOAT -> Float.class;
            case DOUBLE -> Double.class;
            case BYTE_ARRAY -> String.class;
            case FIXED_LEN_BYTE_ARRAY -> byte[].class;
            case INT96 -> throw new IllegalArgumentException("INT96 predicates are unsupported");
        };
    }

    protected final Object selectedValue(Object colValue) {
        if (colValue == null) return null;
        if (targetColumnDescriptor.isMap()) {
            if (!(colValue instanceof Map<?, ?> map)) throw invalid("row value is not a MAP");
            return mapKey.isPresent() ? map.get(mapKey.get()) : map;
        }
        if (targetColumnDescriptor.isList() && !(colValue instanceof List<?>)) {
            throw invalid("row value is not a LIST");
        }
        return colValue;
    }

    protected final void validateScalar(Object value) {
        if (valueType == Type.BYTE_ARRAY && (value instanceof String || value instanceof byte[])) {
            if (matchValue instanceof String && !(value instanceof String)
                    || matchValue instanceof byte[] && !(value instanceof byte[])) {
                throw invalid("row and constant use different BYTE_ARRAY representations");
            }
            return;
        }
        if (valueType == Type.FIXED_LEN_BYTE_ARRAY && value instanceof byte[] bytes) {
            if (bytes.length != ((byte[]) matchValue).length) {
                throw invalid("row value has the wrong fixed binary length");
            }
            return;
        }
        var descriptor = mapKey.isPresent() ? targetColumnDescriptor.getMapMetadata().valueDescriptor()
                : targetColumnDescriptor.getPhysicalDescriptor();
        Class<?> expected = javaClass(valueType);
        if (descriptor != null && descriptor.annotation().kind() == PrimitiveLogicalType.Kind.INTEGER
                && !descriptor.annotation().isSigned()) {
            if (descriptor.annotation().bitWidth() == 32) expected = Long.class;
            if (descriptor.annotation().bitWidth() == 64) expected = BigInteger.class;
        }
        if (!expected.isInstance(value)) {
            throw invalid("expected " + expected.getSimpleName() + " row value, got "
                    + value.getClass().getName());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean apply(Object colValue) {
        Object value = selectedValue(colValue);
        if (operator == FilterOperator.isNull) return value == null;
        if (operator == FilterOperator.isNotNull) return value != null;
        if (value == null) return false;
        boolean scalar = targetColumnDescriptor.isPrimitive() || mapKey.isPresent();
        if (scalar) validateScalar(value);
        if ((operator == FilterOperator.lt || operator == FilterOperator.lte
                || operator == FilterOperator.gt || operator == FilterOperator.gte)
                && (isNaN(value) || isNaN(matchValue))) return false;
        return switch (operator) {
            case eq -> equalValues(value, matchValue);
            case neq -> !equalValues(value, matchValue);
            case lt -> compareValues(value, matchValue) < 0;
            case lte -> compareValues(value, matchValue) <= 0;
            case gt -> compareValues(value, matchValue) > 0;
            case gte -> compareValues(value, matchValue) >= 0;
            case contains -> scalar ? ((String) value).contains((String) matchValue)
                    : value instanceof List<?> list ? list.contains(matchValue)
                      : ((Map<?, ?>) value).containsValue(matchValue);
            case prefix -> ((String) value).startsWith((String) matchValue);
            case suffix -> ((String) value).endsWith((String) matchValue);
            case isNull, isNotNull -> throw new AssertionError("handled above");
        };
    }

    protected static boolean isNaN(Object value) {
        return value instanceof Float f && Float.isNaN(f)
                || value instanceof Double d && Double.isNaN(d);
    }

    private static boolean equalValues(Object left, Object right) {
        if (left instanceof Float a && right instanceof Float b) return a.floatValue() == b.floatValue();
        if (left instanceof Double a && right instanceof Double b) return a.doubleValue() == b.doubleValue();
        if (left instanceof byte[] a && right instanceof byte[] b) {
            return java.util.Arrays.equals(a, b);
        }
        return Objects.equals(left, right);
    }

    @SuppressWarnings("unchecked")
    protected static int compareValues(Object left, Object right) {
        if (left instanceof Float a && right instanceof Float b) {
            return a.floatValue() == b.floatValue() ? 0 : Float.compare(a, b);
        }
        if (left instanceof Double a && right instanceof Double b) {
            return a.doubleValue() == b.doubleValue() ? 0 : Double.compare(a, b);
        }
        if (left instanceof byte[] a && right instanceof byte[] b) {
            return java.util.Arrays.compareUnsigned(a, b);
        }
        return ((Comparable<Object>) left).compareTo(right);
    }

    @Override
    public boolean canDrop(io.github.aloksingh.parquet.model.ColumnStatistics statistics, long numValues) {
        // Key/value statistics cannot prove per-row key presence or absence, or null map semantics.
        if (mapKey.isPresent() || !targetColumnDescriptor.isPrimitive()) return false;
        return PredicateStatistics.canDrop(targetColumnDescriptor, operator, matchValue, statistics, numValues);
    }

    @Override
    public LogicalColumnDescriptor targetColumn() {
        return targetColumnDescriptor;
    }

    @Override
    public String expression() {
        StringBuilder text = new StringBuilder(targetColumnDescriptor.getName());
        mapKey.ifPresent(key -> text.append('[').append(key).append(']'));
        text.append(' ').append(operator);
        if (matchValue != null) {
            text.append(' ').append(matchValue instanceof byte[] bytes
                    ? java.util.Arrays.toString(bytes) : String.valueOf(matchValue));
        }
        return text.toString();
    }

    @Override
    public boolean isApplicable(LogicalColumnDescriptor columnDescriptor) {
        return targetColumnDescriptor.equals(columnDescriptor);
    }

    /**
     * The filter operator for bloom-filter pruning integration.
     */
    public FilterOperator operator() {
        return operator;
    }

    /**
     * The bound constant, or null for isNull/isNotNull.
     */
    public Object getConstant() {
        return matchValue;
    }
}
