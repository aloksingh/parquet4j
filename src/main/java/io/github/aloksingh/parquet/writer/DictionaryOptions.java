package io.github.aloksingh.parquet.writer;

/**
 * Writer options for RLE_DICTIONARY value encoding.
 *
 * <p>When enabled, eligible columns (all physical types except BOOLEAN) are
 * dictionary encoded: one PLAIN dictionary page per column chunk precedes the
 * data pages, and dictionary-encoded data pages store RLE/bit-packed hybrid
 * index streams. If the dictionary would grow past {@code maxDictionaryBytes},
 * the writer falls back to PLAIN for the remaining pages of the chunk and the
 * chunk's {@code encodings} list reflects both. Columns whose pages hold no
 * present (non-null) values stay PLAIN with no dictionary page.</p>
 *
 * @param dictionaryEnabled  whether dictionary encoding is enabled
 * @param maxDictionaryBytes maximum total PLAIN size of the dictionary entries
 *                           (the uncompressed dictionary page body) before falling back to PLAIN
 */
public record DictionaryOptions(boolean dictionaryEnabled, int maxDictionaryBytes) {

    /**
     * Default dictionary budget: 1 MiB of PLAIN-encoded entries.
     */
    public static final int DEFAULT_MAX_DICTIONARY_BYTES = 1024 * 1024;

    /**
     * Validates the options.
     *
     * @throws IllegalArgumentException if the byte limit is not positive
     */
    public DictionaryOptions {
        if (maxDictionaryBytes <= 0) {
            throw new IllegalArgumentException("Dictionary byte limit must be positive");
        }
    }

    /**
     * Dictionary encoding disabled; values are written PLAIN (the default).
     *
     * @return disabled options
     */
    public static DictionaryOptions disabled() {
        return new DictionaryOptions(false, DEFAULT_MAX_DICTIONARY_BYTES);
    }

    /**
     * Dictionary encoding enabled with the default byte budget.
     *
     * @return enabled options
     */
    public static DictionaryOptions enabled() {
        return new DictionaryOptions(true, DEFAULT_MAX_DICTIONARY_BYTES);
    }

    /**
     * Dictionary encoding enabled with a custom byte budget.
     *
     * @param maxDictionaryBytes maximum total PLAIN size of dictionary entries
     * @return enabled options
     * @throws IllegalArgumentException if the byte limit is not positive
     */
    public static DictionaryOptions enabled(int maxDictionaryBytes) {
        return new DictionaryOptions(true, maxDictionaryBytes);
    }
}
