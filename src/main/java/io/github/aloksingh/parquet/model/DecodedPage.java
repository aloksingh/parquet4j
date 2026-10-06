package io.github.aloksingh.parquet.model;

import java.lang.reflect.Array;
import java.util.Objects;

/**
 * One physical data page. Level indexes address events; physical indexes address only
 * present values. Calling {@link #physicalValue(int)} boxes a value for row adapters;
 * {@link #values()} exposes the primitive page storage without per-value boxing.
 */
public final class DecodedPage {
    private final int numValues;
    private final int nonNullCount;
    private final int maxDefinitionLevel;
    private final int[] definitionLevels;
    private final int[] repetitionLevels;
    private final Object values;
    private final int[] dictionaryIndices;
    private final Object[] dictionary;

    DecodedPage(int numValues, int nonNullCount, int maxDefinitionLevel,
                int[] definitionLevels, int[] repetitionLevels, Object values) {
        this(numValues, nonNullCount, maxDefinitionLevel, definitionLevels, repetitionLevels, values, null, null);
    }

    DecodedPage(int numValues, int nonNullCount, int maxDefinitionLevel,
                int[] definitionLevels, int[] repetitionLevels, Object values,
                int[] dictionaryIndices, Object[] dictionary) {
        this.dictionaryIndices = dictionaryIndices;
        this.dictionary = dictionary;
        this.numValues = numValues;
        this.nonNullCount = nonNullCount;
        this.maxDefinitionLevel = maxDefinitionLevel;
        this.definitionLevels = definitionLevels;
        this.repetitionLevels = repetitionLevels;
        this.values = values;
    }

    public int numValues() {
        return numValues;
    }

    public int nonNullCount() {
        return nonNullCount;
    }

    public int definitionLevel(int eventIndex) {
        Objects.checkIndex(eventIndex, numValues);
        return definitionLevels == null ? maxDefinitionLevel : definitionLevels[eventIndex];
    }

    public int repetitionLevel(int eventIndex) {
        Objects.checkIndex(eventIndex, numValues);
        return repetitionLevels == null ? 0 : repetitionLevels[eventIndex];
    }

    public Object physicalValue(int nonNullIndex) {
        Objects.checkIndex(nonNullIndex, nonNullCount);
        if (dictionaryIndices != null) {
            Object value = dictionary[dictionaryIndices[nonNullIndex]];
            return value instanceof byte[] binary ? binary.clone() : value;
        }
        return values instanceof BinaryValues binary
                ? binary.bytesAt(nonNullIndex) : Array.get(values, nonNullIndex);
    }

    public Object values() {
        return values;
    }

    public int[] dictionaryIndices() {
        return dictionaryIndices;
    }

    public Object[] dictionary() {
        return dictionary;
    }
}
