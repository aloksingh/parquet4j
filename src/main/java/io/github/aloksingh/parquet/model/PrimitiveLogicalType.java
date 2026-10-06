package io.github.aloksingh.parquet.model;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.StringType;

/**
 * Immutable interpretation of a primitive Parquet column. NONE and unsupported annotations
 * preserve physical values. Mutable format structs are copied on input and on output.
 */
public final class PrimitiveLogicalType {
    public enum Kind {
        NONE, STRING, ENUM, JSON, BSON, DECIMAL, DATE, TIME, TIMESTAMP, INTEGER, UUID, UNKNOWN
    }

    /**
     * Immutable precision unit; never expose mutable Thrift TimeUnit unions.
     */
    public enum TimeUnit {
        MILLIS(1_000_000), MICROS(1_000), NANOS(1);
        private final long nanos;

        TimeUnit(long nanos) {
            this.nanos = nanos;
        }
    }

    private static final PrimitiveLogicalType NONE = new PrimitiveLogicalType(Kind.NONE, null, null, null, null);
    private static final PrimitiveLogicalType STRING = new PrimitiveLogicalType(Kind.STRING,
            org.apache.parquet.format.LogicalType.STRING(new StringType()), ConvertedType.UTF8, null, null);
    private final Kind kind;
    private final org.apache.parquet.format.LogicalType modern;
    private final ConvertedType converted;
    private final Integer legacyPrecision;
    private final Integer legacyScale;
    private final Type physicalType;
    private final int typeLength;

    private PrimitiveLogicalType(Kind kind, org.apache.parquet.format.LogicalType modern,
                                 ConvertedType converted, Integer legacyPrecision, Integer legacyScale) {
        this(kind, modern, converted, legacyPrecision, legacyScale, null, 0);
    }

    private PrimitiveLogicalType(Kind kind, org.apache.parquet.format.LogicalType modern,
                                 ConvertedType converted, Integer legacyPrecision, Integer legacyScale,
                                 Type physicalType, int typeLength) {
        this.kind = kind;
        this.modern = modern == null ? null : modern.deepCopy();
        this.converted = converted;
        this.legacyPrecision = legacyPrecision;
        this.legacyScale = legacyScale;
        this.physicalType = physicalType;
        this.typeLength = typeLength;
        validateDecimal();
        if (kind == Kind.INTEGER && bitWidth() != 8 && bitWidth() != 16 && bitWidth() != 32 && bitWidth() != 64) {
            throw new IllegalArgumentException("Unsupported INTEGER bit width: " + bitWidth());
        }
        if (physicalType != null && !supportsPhysicalType(physicalType, typeLength)) {
            throw new IllegalArgumentException(kind + " cannot annotate " + physicalType + "(" + typeLength + ")");
        }
    }

    public static PrimitiveLogicalType none() {
        return NONE;
    }

    public static PrimitiveLogicalType string() {
        return STRING;
    }

    public static PrimitiveLogicalType uuid() {
        return new PrimitiveLogicalType(Kind.UUID,
                org.apache.parquet.format.LogicalType.UUID(new org.apache.parquet.format.UUIDType()), null, null, null);
    }

    public static PrimitiveLogicalType unknown() {
        return new PrimitiveLogicalType(Kind.UNKNOWN,
                org.apache.parquet.format.LogicalType.UNKNOWN(new org.apache.parquet.format.NullType()), null, null, null);
    }

    public static PrimitiveLogicalType enumeration() {
        return new PrimitiveLogicalType(Kind.ENUM,
                org.apache.parquet.format.LogicalType.ENUM(new org.apache.parquet.format.EnumType()),
                ConvertedType.ENUM, null, null);
    }

    public static PrimitiveLogicalType json() {
        return new PrimitiveLogicalType(Kind.JSON,
                org.apache.parquet.format.LogicalType.JSON(new org.apache.parquet.format.JsonType()),
                ConvertedType.JSON, null, null);
    }

    public static PrimitiveLogicalType bson() {
        return new PrimitiveLogicalType(Kind.BSON,
                org.apache.parquet.format.LogicalType.BSON(new org.apache.parquet.format.BsonType()),
                ConvertedType.BSON, null, null);
    }

    public static PrimitiveLogicalType integer(int bitWidth, boolean isSigned) {
        ConvertedType legacy = switch (bitWidth) {
            case 8 -> isSigned ? ConvertedType.INT_8 : ConvertedType.UINT_8;
            case 16 -> isSigned ? ConvertedType.INT_16 : ConvertedType.UINT_16;
            case 32 -> isSigned ? ConvertedType.INT_32 : ConvertedType.UINT_32;
            case 64 -> isSigned ? ConvertedType.INT_64 : ConvertedType.UINT_64;
            default -> throw new IllegalArgumentException("Unsupported INTEGER bit width: " + bitWidth);
        };
        return new PrimitiveLogicalType(Kind.INTEGER,
                org.apache.parquet.format.LogicalType.INTEGER(new org.apache.parquet.format.IntType((byte) bitWidth, isSigned)),
                legacy, null, null);
    }

    public int bitWidth() {
        return modern != null && modern.isSetINTEGER() ? Byte.toUnsignedInt(modern.getINTEGER().getBitWidth())
                : legacyBitWidth(converted);
    }

    public boolean isSigned() {
        return modern != null && modern.isSetINTEGER() ? modern.getINTEGER().isIsSigned()
                : converted == ConvertedType.INT_8 || converted == ConvertedType.INT_16
                  || converted == ConvertedType.INT_32 || converted == ConvertedType.INT_64;
    }

    private static int legacyBitWidth(ConvertedType converted) {
        if (converted == null) {
            return 0;
        }
        return switch (converted) {
            case INT_8, UINT_8 -> 8;
            case INT_16, UINT_16 -> 16;
            case INT_32, UINT_32 -> 32;
            case INT_64, UINT_64 -> 64;
            default -> 0;
        };
    }

    public static PrimitiveLogicalType timestamp(TimeUnit unit, boolean adjustedToUTC) {
        Objects.requireNonNull(unit, "unit");
        ConvertedType legacy = unit == TimeUnit.MILLIS ? ConvertedType.TIMESTAMP_MILLIS
                : unit == TimeUnit.MICROS ? ConvertedType.TIMESTAMP_MICROS : null;
        return new PrimitiveLogicalType(Kind.TIMESTAMP,
                org.apache.parquet.format.LogicalType.TIMESTAMP(new org.apache.parquet.format.TimestampType(
                        adjustedToUTC, thriftUnit(unit))), legacy, null, null);
    }

    public static PrimitiveLogicalType timestamp(boolean adjustedToUTC, TimeUnit unit) {
        return timestamp(unit, adjustedToUTC);
    }

    public static PrimitiveLogicalType time(TimeUnit unit, boolean adjustedToUTC) {
        Objects.requireNonNull(unit, "unit");
        ConvertedType legacy = unit == TimeUnit.MILLIS ? ConvertedType.TIME_MILLIS
                : unit == TimeUnit.MICROS ? ConvertedType.TIME_MICROS : null;
        return new PrimitiveLogicalType(Kind.TIME,
                org.apache.parquet.format.LogicalType.TIME(new org.apache.parquet.format.TimeType(
                        adjustedToUTC, thriftUnit(unit))), legacy, null, null);
    }

    public static PrimitiveLogicalType time(boolean adjustedToUTC, TimeUnit unit) {
        return time(unit, adjustedToUTC);
    }

    public TimeUnit unit() {
        if (modern != null && modern.isSetTIME()) {
            return fromThriftUnit(modern.getTIME().getUnit());
        }
        if (modern != null && modern.isSetTIMESTAMP()) {
            return fromThriftUnit(modern.getTIMESTAMP().getUnit());
        }
        return converted == ConvertedType.TIME_MILLIS || converted == ConvertedType.TIMESTAMP_MILLIS ? TimeUnit.MILLIS
                : converted == ConvertedType.TIME_MICROS || converted == ConvertedType.TIMESTAMP_MICROS ? TimeUnit.MICROS : null;
    }

    public boolean adjustedToUTC() {
        if (modern != null && modern.isSetTIME()) {
            return modern.getTIME().isIsAdjustedToUTC();
        }
        if (modern != null && modern.isSetTIMESTAMP()) {
            return modern.getTIMESTAMP().isIsAdjustedToUTC();
        }
        return kind == Kind.TIME || kind == Kind.TIMESTAMP;
    }

    private static org.apache.parquet.format.TimeUnit thriftUnit(TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> org.apache.parquet.format.TimeUnit.MILLIS(new org.apache.parquet.format.MilliSeconds());
            case MICROS -> org.apache.parquet.format.TimeUnit.MICROS(new org.apache.parquet.format.MicroSeconds());
            case NANOS -> org.apache.parquet.format.TimeUnit.NANOS(new org.apache.parquet.format.NanoSeconds());
        };
    }

    private static TimeUnit fromThriftUnit(org.apache.parquet.format.TimeUnit unit) {
        return unit == null ? null : unit.isSetMILLIS() ? TimeUnit.MILLIS
                                     : unit.isSetMICROS() ? TimeUnit.MICROS : unit.isSetNANOS() ? TimeUnit.NANOS : null;
    }

    public static PrimitiveLogicalType date() {
        return new PrimitiveLogicalType(Kind.DATE,
                org.apache.parquet.format.LogicalType.DATE(new org.apache.parquet.format.DateType()),
                ConvertedType.DATE, null, null);
    }

    public static PrimitiveLogicalType decimal(int precision, int scale) {
        return new PrimitiveLogicalType(Kind.DECIMAL,
                org.apache.parquet.format.LogicalType.DECIMAL(
                        new org.apache.parquet.format.DecimalType(scale, precision)),
                ConvertedType.DECIMAL, precision, scale);
    }

    public int precision() {
        return kind == Kind.DECIMAL && modern != null && modern.isSetDECIMAL()
                ? modern.getDECIMAL().getPrecision() : legacyPrecision == null ? 0 : legacyPrecision;
    }

    public int scale() {
        return kind == Kind.DECIMAL && modern != null && modern.isSetDECIMAL()
                ? modern.getDECIMAL().getScale() : legacyScale == null ? 0 : legacyScale;
    }

    /**
     * Bind conversion output to the descriptor's native physical carrier.
     */
    PrimitiveLogicalType withPhysicalType(Type type, int length) {
        if (kind == Kind.NONE || physicalType == type && typeLength == length) {
            return this;
        }
        return new PrimitiveLogicalType(kind, modern, converted, legacyPrecision, legacyScale, type, length);
    }

    private void validateDecimal() {
        if (kind != Kind.DECIMAL) {
            return;
        }
        if (precision() <= 0 || scale() < 0 || scale() > precision()) {
            throw new IllegalArgumentException("Invalid DECIMAL precision/scale: " + precision() + "/" + scale());
        }
        if (physicalType == null) {
            return;
        }
        int maxPrecision = switch (physicalType) {
            case INT32 -> 9;
            case INT64 -> 18;
            case BYTE_ARRAY -> Integer.MAX_VALUE;
            case FIXED_LEN_BYTE_ARRAY -> {
                if (typeLength <= 0) {
                    throw new IllegalArgumentException("DECIMAL fixed length must be positive");
                }
                // The capacity is floor(log10(2^(8*n-1)-1)), not the digit count of that maximum.
                yield BigInteger.ONE.shiftLeft(Math.subtractExact(Math.multiplyExact(typeLength, 8), 1))
                        .subtract(BigInteger.ONE).toString().length() - 1;
            }
            default -> throw new IllegalArgumentException("DECIMAL cannot annotate " + physicalType);
        };
        if (precision() > maxPrecision) {
            throw new IllegalArgumentException("DECIMAL precision exceeds " + physicalType + " capacity");
        }
    }

    public Kind kind() {
        return kind;
    }

    public boolean isString() {
        return kind == Kind.STRING;
    }

    /**
     * Retain exactly the annotation fields representable by parquet-format-structures 1.13.1.
     */
    public static PrimitiveLogicalType fromSchemaElement(SchemaElement source) {
        Objects.requireNonNull(source, "source");
        org.apache.parquet.format.LogicalType logical = source.getLogicalType();
        ConvertedType legacy = source.getConverted_type();
        Kind kind = identifyKind(logical, legacy);
        Integer precision = source.isSetPrecision() ? source.getPrecision() : null;
        Integer scale = source.isSetScale() ? source.getScale() : null;
        Type physical = source.isSetType() ? Type.fromValue(source.getType().getValue()) : null;
        int length = source.isSetType_length() ? source.getType_length() : 0;
        PrimitiveLogicalType annotation = new PrimitiveLogicalType(kind, logical, legacy, precision, scale);
        if (physical != null && !annotation.supportsPhysicalType(physical, length)
                || (kind == Kind.TIME || kind == Kind.TIMESTAMP) && annotation.unit() == null) {
            // Unknown logical/physical combinations are read as raw values, not guessed conversions.
            return new PrimitiveLogicalType(Kind.UNKNOWN, logical, legacy, precision, scale, physical, length);
        }
        return physical == null ? annotation : annotation.withPhysicalType(physical, length);
    }

    private static Kind identifyKind(org.apache.parquet.format.LogicalType logical, ConvertedType legacy) {
        if (logical != null) {
            if (logical.getSetField() == null) {
                return Kind.UNKNOWN;
            }
            return switch (logical.getSetField()) {
                case STRING -> Kind.STRING;
                case ENUM -> Kind.ENUM;
                case JSON -> Kind.JSON;
                case BSON -> Kind.BSON;
                case DECIMAL -> Kind.DECIMAL;
                case DATE -> Kind.DATE;
                case TIME -> Kind.TIME;
                case TIMESTAMP -> Kind.TIMESTAMP;
                case INTEGER -> Kind.INTEGER;
                case UUID -> Kind.UUID;
                default -> Kind.UNKNOWN;
            };
        }
        if (legacy == null) {
            return Kind.NONE;
        }
        return switch (legacy) {
            case UTF8 -> Kind.STRING;
            case ENUM -> Kind.ENUM;
            case JSON -> Kind.JSON;
            case BSON -> Kind.BSON;
            case DECIMAL -> Kind.DECIMAL;
            case DATE -> Kind.DATE;
            case TIME_MILLIS, TIME_MICROS -> Kind.TIME;
            case TIMESTAMP_MILLIS, TIMESTAMP_MICROS -> Kind.TIMESTAMP;
            case INT_8, INT_16, INT_32, INT_64, UINT_8, UINT_16, UINT_32, UINT_64 -> Kind.INTEGER;
            default -> Kind.UNKNOWN;
        };
    }

    private boolean supportsPhysicalType(Type type, int length) {
        return switch (kind) {
            case STRING, ENUM, JSON, BSON -> type == Type.BYTE_ARRAY;
            case DATE -> type == Type.INT32;
            case UUID -> type == Type.FIXED_LEN_BYTE_ARRAY && length == 16;
            case INTEGER -> type == (bitWidth() == 64 ? Type.INT64 : Type.INT32);
            case TIME -> unit() != null && type == (unit() == TimeUnit.MILLIS ? Type.INT32 : Type.INT64);
            case TIMESTAMP -> unit() != null && type == Type.INT64;
            case DECIMAL -> type == Type.INT32 || type == Type.INT64 || type == Type.BYTE_ARRAY
                    || type == Type.FIXED_LEN_BYTE_ARRAY;
            default -> true;
        };
    }

    /**
     * Replace annotation fields only; do not change name, physical type, repetition, or field ID.
     */
    public SchemaElement applyTo(SchemaElement target) {
        Objects.requireNonNull(target, "target");
        target.unsetLogicalType();
        target.unsetConverted_type();
        target.unsetPrecision();
        target.unsetScale();
        if (modern != null) {
            target.setLogicalType(modern.deepCopy());
        }
        if (converted != null) {
            target.setConverted_type(converted);
        }
        if (legacyPrecision != null) {
            target.setPrecision(legacyPrecision);
        }
        if (legacyScale != null) {
            target.setScale(legacyScale);
        }
        return target;
    }

    public Object toLogicalValue(Object physical) {
        if (physical == null || kind == Kind.NONE || kind == Kind.UNKNOWN || kind == Kind.BSON) {
            return physical;
        }
        if (kind == Kind.UUID) {
            byte[] bytes = binaryBytes(physical);
            if (bytes.length != 16) {
                throw new IllegalArgumentException("UUID physical value must be exactly 16 bytes");
            }
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return new UUID(buffer.getLong(), buffer.getLong());
        }
        if (kind == Kind.INTEGER) {
            BigInteger value = integral(physical);
            if (!isSigned() && bitWidth() == 32) {
                return Integer.toUnsignedLong(value.intValueExact());
            }
            if (!isSigned() && bitWidth() == 64) {
                long bits = value.longValueExact();
                return bits < 0 ? BigInteger.valueOf(bits).add(BigInteger.ONE.shiftLeft(64)) : BigInteger.valueOf(bits);
            }
            checkIntegerRange(value);
            if (bitWidth() == 64) {
                return value.longValueExact();
            }
            return value.intValueExact();
        }
        if (kind == Kind.TIMESTAMP) {
            long count = integral(physical).longValueExact();
            long perSecond = 1_000_000_000L / unit().nanos;
            long seconds = Math.floorDiv(count, perSecond);
            int nanos = Math.toIntExact(Math.floorMod(count, perSecond) * unit().nanos);
            return adjustedToUTC() ? Instant.ofEpochSecond(seconds, nanos)
                    : LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC);
        }
        if (kind == Kind.TIME) {
            long count = integral(physical).longValueExact();
            if (count < 0 || count >= 86_400_000_000_000L / unit().nanos) {
                throw new IllegalArgumentException("TIME value outside one day");
            }
            return LocalTime.ofNanoOfDay(count * unit().nanos);
        }
        if (kind == Kind.DATE) {
            return LocalDate.ofEpochDay(integral(physical).intValueExact());
        }
        if (kind == Kind.DECIMAL) {
            BigInteger unscaled;
            if (physical instanceof byte[] || physical instanceof ByteBuffer) {
                byte[] bytes = binaryBytes(physical);
                if (bytes.length == 0 || physicalType == Type.FIXED_LEN_BYTE_ARRAY && bytes.length != typeLength) {
                    throw new IllegalArgumentException("Invalid DECIMAL binary length");
                }
                unscaled = new BigInteger(bytes);
            } else {
                unscaled = integral(physical);
            }
            BigDecimal value = new BigDecimal(unscaled, scale());
            checkPrecision(value);
            return value;
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(binaryBytes(physical))).toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException("Invalid UTF-8 physical value", invalid);
        }
    }

    public Object toPhysicalValue(Object logical) {
        if (logical == null || kind == Kind.NONE || kind == Kind.UNKNOWN || kind == Kind.BSON) {
            return logical;
        }
        if (kind == Kind.UUID) {
            UUID value = (UUID) logical;
            return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                    .putLong(value.getLeastSignificantBits()).array();
        }
        if (kind == Kind.INTEGER) {
            BigInteger value = integral(logical);
            checkIntegerRange(value);
            if (bitWidth() == 64) {
                return value.longValue();
            }
            return value.intValue();
        }
        if (kind == Kind.TIMESTAMP) {
            long seconds;
            int nanos;
            if (adjustedToUTC()) {
                Instant instant = (Instant) logical;
                seconds = instant.getEpochSecond();
                nanos = instant.getNano();
            } else {
                LocalDateTime local = (LocalDateTime) logical;
                seconds = local.toEpochSecond(ZoneOffset.UTC);
                nanos = local.getNano();
            }
            if (nanos % unit().nanos != 0) {
                throw new ArithmeticException("TIMESTAMP value has finer precision than " + unit());
            }
            // Use an exact wider intermediate: Long.MIN_VALUE must round-trip for every unit.
            return BigInteger.valueOf(seconds).multiply(BigInteger.valueOf(1_000_000_000L / unit().nanos))
                    .add(BigInteger.valueOf(nanos / unit().nanos)).longValueExact();
        }
        if (kind == Kind.TIME) {
            long nanos = ((LocalTime) logical).toNanoOfDay();
            if (nanos % unit().nanos != 0) {
                throw new ArithmeticException("TIME value has finer precision than " + unit());
            }
            long count = nanos / unit().nanos;
            if (unit() == TimeUnit.MILLIS) {
                return Math.toIntExact(count);
            }
            return count;
        }
        if (kind == Kind.DATE) {
            return Math.toIntExact(((LocalDate) logical).toEpochDay());
        }
        if (kind == Kind.DECIMAL) {
            BigDecimal value = ((BigDecimal) logical).setScale(scale(), RoundingMode.UNNECESSARY);
            checkPrecision(value);
            BigInteger unscaled = value.unscaledValue();
            if (physicalType == null) {
                return unscaled;
            }
            return switch (physicalType) {
                case INT32 -> unscaled.intValueExact();
                case INT64 -> unscaled.longValueExact();
                case BYTE_ARRAY -> unscaled.toByteArray();
                case FIXED_LEN_BYTE_ARRAY -> {
                    byte[] encoded = unscaled.toByteArray();
                    if (encoded.length > typeLength) {
                        throw new ArithmeticException("DECIMAL does not fit fixed physical length");
                    }
                    byte[] bytes = new byte[typeLength];
                    if (unscaled.signum() < 0) {
                        Arrays.fill(bytes, (byte) -1);
                    }
                    System.arraycopy(encoded, 0, bytes, typeLength - encoded.length, encoded.length);
                    yield bytes;
                }
                default -> throw new IllegalArgumentException("Invalid DECIMAL carrier: " + physicalType);
            };
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap((String) logical));
            return binaryBytes(encoded);
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException("Invalid UTF-8 logical value", invalid);
        }
    }

    private void checkIntegerRange(BigInteger value) {
        BigInteger minimum = isSigned() ? BigInteger.ONE.shiftLeft(bitWidth() - 1).negate() : BigInteger.ZERO;
        BigInteger maximum = BigInteger.ONE.shiftLeft(isSigned() ? bitWidth() - 1 : bitWidth()).subtract(BigInteger.ONE);
        if (value.compareTo(minimum) < 0 || value.compareTo(maximum) > 0) {
            throw new ArithmeticException("Value outside INTEGER(" + bitWidth() + ", " + isSigned() + ")");
        }
    }

    private void checkPrecision(BigDecimal value) {
        if (value.precision() > precision()) {
            throw new ArithmeticException("DECIMAL value exceeds precision " + precision());
        }
    }

    private static byte[] binaryBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer copy = buffer.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
        throw new IllegalArgumentException("Expected binary physical value");
    }

    private static BigInteger integral(Object value) {
        if (value instanceof BigInteger integer) {
            return integer;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return BigInteger.valueOf(((Number) value).longValue());
        }
        throw new IllegalArgumentException("Expected exact integral physical value");
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PrimitiveLogicalType that) || kind != that.kind) {
            return false;
        }
        if (kind == Kind.INTEGER) {
            return bitWidth() == that.bitWidth() && isSigned() == that.isSigned();
        }
        if (kind == Kind.TIME || kind == Kind.TIMESTAMP) {
            return unit() == that.unit() && adjustedToUTC() == that.adjustedToUTC();
        }
        if (kind == Kind.DECIMAL) {
            return precision() == that.precision() && scale() == that.scale();
        }
        return kind != Kind.UNKNOWN || Objects.equals(modern, that.modern)
                && converted == that.converted && Objects.equals(legacyPrecision, that.legacyPrecision)
                && Objects.equals(legacyScale, that.legacyScale);
    }

    @Override
    public int hashCode() {
        return switch (kind) {
            case DECIMAL -> Objects.hash(kind, precision(), scale());
            case TIME, TIMESTAMP -> Objects.hash(kind, unit(), adjustedToUTC());
            case INTEGER -> Objects.hash(kind, bitWidth(), isSigned());
            case UNKNOWN -> Objects.hash(kind, modern, converted, legacyPrecision, legacyScale);
            default -> kind.hashCode();
        };
    }

    @Override
    public String toString() {
        return kind.toString();
    }
}
