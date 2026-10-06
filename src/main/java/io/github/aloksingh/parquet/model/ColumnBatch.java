package io.github.aloksingh.parquet.model;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.BitSet;
import java.util.Objects;

/**
 * An owning, immutable column batch backed by primitive arrays and a validity bitmap.
 * Numeric buffer access is read-only; null slots must be checked with {@link #isNull(int)}.
 * Row/object access boxes only the value requested by the caller.
 */
public final class ColumnBatch {
    private final LogicalColumnDescriptor descriptor;
    private final Object values;
    private final BitSet present;
    private final int size;

    private ColumnBatch(LogicalColumnDescriptor descriptor, Object values, BitSet present, int size) {
        this.descriptor = descriptor;
        this.values = values;
        this.present = present;
        this.size = size;
    }

    /**
     * Copy typed primitive arrays and validity into an independently owned batch.
     */
    public static ColumnBatch of(ColumnDescriptor descriptor, Object values, BitSet present) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(values, "values");
        Object copied;
        int size;
        switch (descriptor.physicalType()) {
            case INT32 -> {
                int[] source = (int[]) values;
                copied = source.clone();
                size = source.length;
            }
            case INT64 -> {
                long[] source = (long[]) values;
                copied = source.clone();
                size = source.length;
            }
            case FLOAT -> {
                float[] source = (float[]) values;
                copied = source.clone();
                size = source.length;
            }
            case DOUBLE -> {
                double[] source = (double[]) values;
                copied = source.clone();
                size = source.length;
            }
            case BOOLEAN -> {
                boolean[] source = (boolean[]) values;
                copied = source.clone();
                size = source.length;
            }
            default ->
                    throw new IllegalArgumentException("Not a numeric primitive batch: " + descriptor.physicalType());
        }
        BitSet validity = present == null ? null : (BitSet) present.clone();
        if (validity != null && validity.length() > size) {
            throw new IllegalArgumentException("Validity bitmap exceeds the batch size");
        }
        if (descriptor.maxDefinitionLevel() == 0 && validity != null && validity.cardinality() != size) {
            throw new IllegalArgumentException("Required column batch contains nulls");
        }
        if (validity != null && validity.cardinality() == size) validity = null;
        LogicalColumnDescriptor logical = new LogicalColumnDescriptor(descriptor.getPathString(),
                LogicalType.PRIMITIVE, descriptor.physicalType(), descriptor);
        return new ColumnBatch(logical, copied, validity, size);
    }

    /**
     * Preserve encoded dictionary indexes; primitive materialization is lazy.
     */
    public static ColumnBatch dictionary(ColumnDescriptor descriptor, int[] indexes, Object[] dictionary, BitSet present) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(indexes, "indexes");
        Objects.requireNonNull(dictionary, "dictionary");
        int size = indexes.length;
        BitSet validity = present == null ? null : (BitSet) present.clone();
        if (validity != null && validity.length() > size)
            throw new IllegalArgumentException("Validity exceeds batch size");
        if (descriptor.maxDefinitionLevel() == 0 && validity != null && validity.cardinality() != size) {
            throw new IllegalArgumentException("Required column batch contains nulls");
        }
        Object[] owned = dictionary.clone();
        Type type = descriptor.physicalType();
        for (int i = 0; i < owned.length; i++) {
            Object value = Objects.requireNonNull(owned[i], "Null dictionary entry");
            boolean compatible = switch (type) {
                case INT32 -> value instanceof Integer;
                case INT64 -> value instanceof Long;
                case FLOAT -> value instanceof Float;
                case DOUBLE -> value instanceof Double;
                case BOOLEAN -> value instanceof Boolean;
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> value instanceof byte[];
            };
            if (!compatible) throw new IllegalArgumentException("Dictionary entry does not match " + type);
            if (value instanceof byte[] bytes) {
                int width = type == Type.INT96 ? 12 : descriptor.typeLength();
                if (type != Type.BYTE_ARRAY && bytes.length != width)
                    throw new IllegalArgumentException("Invalid dictionary fixed length");
                owned[i] = bytes.clone();
            }
        }
        int[] encoded = indexes.clone();
        for (int i = 0; i < size; i++) {
            if (validity != null && !validity.get(i)) encoded[i] = -1;
            else if (encoded[i] < 0 || encoded[i] >= owned.length)
                throw new IllegalArgumentException("Invalid dictionary index at row " + i);
        }
        if (validity != null && validity.cardinality() == size) validity = null;
        LogicalColumnDescriptor logical = new LogicalColumnDescriptor(descriptor.getPathString(), LogicalType.PRIMITIVE, type, descriptor);
        return new ColumnBatch(logical, new DictionaryStorage(encoded, owned), validity, size);
    }

    public boolean isDictionaryEncoded() {
        return values instanceof DictionaryStorage;
    }

    public IntBuffer dictionaryIndices() {
        if (!(values instanceof DictionaryStorage storage)) throw new IllegalStateException("Not a dictionary batch");
        return IntBuffer.wrap(storage.indexes).asReadOnlyBuffer();
    }

    /**
     * Copy arbitrary binary values using row offsets; null and empty remain distinct.
     */
    public static ColumnBatch binary(ColumnDescriptor descriptor, int[] offsets, ByteBuffer data, BitSet present) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(offsets, "offsets");
        Objects.requireNonNull(data, "data");
        Type type = descriptor.physicalType();
        if (type != Type.BYTE_ARRAY && type != Type.FIXED_LEN_BYTE_ARRAY && type != Type.INT96) {
            throw new IllegalArgumentException("Not a binary column: " + type);
        }
        if (offsets.length == 0 || offsets[0] != 0) throw new IllegalArgumentException("Offsets must begin at zero");
        int size = offsets.length - 1;
        BitSet validity = present == null ? null : (BitSet) present.clone();
        if (validity != null && validity.length() > size)
            throw new IllegalArgumentException("Validity exceeds batch size");
        if (descriptor.maxDefinitionLevel() == 0 && validity != null && validity.cardinality() != size) {
            throw new IllegalArgumentException("Required column batch contains nulls");
        }
        int width = type == Type.INT96 ? 12 : descriptor.typeLength();
        if (type == Type.FIXED_LEN_BYTE_ARRAY && width < 1) throw new IllegalArgumentException("Invalid fixed length");
        int bytes = data.remaining();
        for (int i = 0; i < size; i++) {
            if (offsets[i] < 0 || offsets[i + 1] < offsets[i] || offsets[i + 1] > bytes) {
                throw new IllegalArgumentException("Invalid binary offsets at row " + i);
            }
            int length = offsets[i + 1] - offsets[i];
            boolean exists = validity == null || validity.get(i);
            if (!exists && length != 0) throw new IllegalArgumentException("Null row has a binary payload");
            if (exists && type != Type.BYTE_ARRAY && length != width)
                throw new IllegalArgumentException("Incorrect fixed length at row " + i);
        }
        if (offsets[size] != bytes) throw new IllegalArgumentException("Offsets do not cover the binary buffer");
        byte[] copy = new byte[bytes];
        data.duplicate().get(copy);
        if (validity != null && validity.cardinality() == size) validity = null;
        LogicalColumnDescriptor logical = new LogicalColumnDescriptor(descriptor.getPathString(), LogicalType.PRIMITIVE, type, descriptor);
        return new ColumnBatch(logical, new BinaryStorage(offsets.clone(), ByteBuffer.wrap(copy).asReadOnlyBuffer()), validity, size);
    }

    public LogicalColumnDescriptor descriptor() {
        return descriptor;
    }

    public int size() {
        return size;
    }

    public Type physicalType() {
        return descriptor.getPhysicalType();
    }

    public boolean isNull(int row) {
        checkRow(row);
        return present != null && !present.get(row);
    }

    /**
     * Returns an independent validity bitmap; a set bit denotes a present value.
     */
    public BitSet validity() {
        if (present != null) return (BitSet) present.clone();
        BitSet all = new BitSet(size);
        all.set(0, size);
        return all;
    }

    public Object getObject(int row) {
        if (isNull(row)) return null;
        if (values instanceof DictionaryStorage storage) {
            Object value = storage.value(row);
            return value instanceof byte[] bytes ? bytes.clone() : value;
        }
        return switch (physicalType()) {
            case INT32 -> ((int[]) values)[row];
            case INT64 -> ((long[]) values)[row];
            case FLOAT -> ((float[]) values)[row];
            case DOUBLE -> ((double[]) values)[row];
            case BOOLEAN -> ((boolean[]) values)[row];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                ByteBuffer data = getBytes(row);
                byte[] result = new byte[data.remaining()];
                data.get(result);
                yield result;
            }
        };
    }

    public int getInt(int row) {
        require(Type.INT32, row);
        return values instanceof DictionaryStorage storage ? (Integer) storage.value(row) : ((int[]) values)[row];
    }

    public long getLong(int row) {
        require(Type.INT64, row);
        return values instanceof DictionaryStorage storage ? (Long) storage.value(row) : ((long[]) values)[row];
    }

    public float getFloat(int row) {
        require(Type.FLOAT, row);
        return values instanceof DictionaryStorage storage ? (Float) storage.value(row) : ((float[]) values)[row];
    }

    public double getDouble(int row) {
        require(Type.DOUBLE, row);
        return values instanceof DictionaryStorage storage ? (Double) storage.value(row) : ((double[]) values)[row];
    }

    public boolean getBoolean(int row) {
        require(Type.BOOLEAN, row);
        return values instanceof DictionaryStorage storage ? (Boolean) storage.value(row) : ((boolean[]) values)[row];
    }

    public IntBuffer intValues() {
        requireType(Type.INT32);
        return IntBuffer.wrap((int[]) denseValues()).asReadOnlyBuffer();
    }

    public LongBuffer longValues() {
        requireType(Type.INT64);
        return LongBuffer.wrap((long[]) denseValues()).asReadOnlyBuffer();
    }

    public FloatBuffer floatValues() {
        requireType(Type.FLOAT);
        return FloatBuffer.wrap((float[]) denseValues()).asReadOnlyBuffer();
    }

    public DoubleBuffer doubleValues() {
        requireType(Type.DOUBLE);
        return DoubleBuffer.wrap((double[]) denseValues()).asReadOnlyBuffer();
    }

    public boolean[] booleanValues() {
        requireType(Type.BOOLEAN);
        return ((boolean[]) denseValues()).clone();
    }

    /**
     * Read-only, zero-copy view of one binary value.
     */
    public ByteBuffer getBytes(int row) {
        if (isNull(row)) return null;
        Type type = physicalType();
        if (type != Type.BYTE_ARRAY && type != Type.FIXED_LEN_BYTE_ARRAY && type != Type.INT96)
            throw new IllegalStateException("Not a binary batch");
        if (values instanceof DictionaryStorage dictionary)
            return ByteBuffer.wrap((byte[]) dictionary.value(row)).asReadOnlyBuffer();
        if (!(values instanceof BinaryStorage storage)) throw new IllegalStateException("Not a binary batch");
        ByteBuffer view = storage.data().duplicate();
        view.position(storage.offsets()[row]);
        view.limit(storage.offsets()[row + 1]);
        return view.slice().asReadOnlyBuffer();
    }

    private Object denseValues() {
        return values instanceof DictionaryStorage storage ? storage.materialize(physicalType()) : values;
    }

    private static final class DictionaryStorage {
        private final int[] indexes;
        private final Object[] dictionary;
        private Object dense;

        private DictionaryStorage(int[] indexes, Object[] dictionary) {
            this.indexes = indexes;
            this.dictionary = dictionary;
        }

        private Object value(int row) {
            return dictionary[indexes[row]];
        }

        private synchronized Object materialize(Type type) {
            if (dense != null) return dense;
            int size = indexes.length;
            dense = switch (type) {
                case INT32 -> {
                    int[] data = new int[size];
                    for (int i = 0; i < size; i++) if (indexes[i] >= 0) data[i] = (Integer) value(i);
                    yield data;
                }
                case INT64 -> {
                    long[] data = new long[size];
                    for (int i = 0; i < size; i++) if (indexes[i] >= 0) data[i] = (Long) value(i);
                    yield data;
                }
                case FLOAT -> {
                    float[] data = new float[size];
                    for (int i = 0; i < size; i++) if (indexes[i] >= 0) data[i] = (Float) value(i);
                    yield data;
                }
                case DOUBLE -> {
                    double[] data = new double[size];
                    for (int i = 0; i < size; i++) if (indexes[i] >= 0) data[i] = (Double) value(i);
                    yield data;
                }
                case BOOLEAN -> {
                    boolean[] data = new boolean[size];
                    for (int i = 0; i < size; i++) if (indexes[i] >= 0) data[i] = (Boolean) value(i);
                    yield data;
                }
                default -> throw new IllegalStateException("Binary dictionaries use getBytes instead of numeric views");
            };
            return dense;
        }
    }

    private record BinaryStorage(int[] offsets, ByteBuffer data) {
    }

    private void require(Type type, int row) {
        requireType(type);
        if (isNull(row)) throw new IllegalStateException("Column value is null at row " + row);
    }

    private void requireType(Type type) {
        if (physicalType() != type) {
            throw new IllegalStateException("Expected " + type + ", found " + physicalType());
        }
    }

    private void checkRow(int row) {
        Objects.checkIndex(row, size);
    }
}
