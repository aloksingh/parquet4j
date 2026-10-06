package io.github.aloksingh.parquet;

/**
 * Immutable resource and integrity policy for page reads. All limits are positive.
 * The defaults bound headers to 1 MiB, stored bodies to 64 MiB, decoded bodies to
 * 128 MiB, and page/dictionary counts to 16 Mi values. Limits are checked before
 * allocating a body or calling a codec, independently of checksum policy.
 *
 * <p>{@link #DEFAULT} deliberately leaves checksum verification off for compatibility
 * with legacy files containing incorrect CRC fields. {@link #STRICT} has identical
 * resource limits and verifies a CRC whenever the page header supplies one.
 *
 * @param maxHeaderBytes           maximum serialized Thrift header bytes
 * @param maxCompressedPageBytes   maximum stored page body bytes
 * @param maxUncompressedPageBytes maximum decoded page body bytes (including V2 levels)
 * @param maxValuesPerPage         maximum value count for a data or dictionary page
 * @param verifyChecksums          whether to verify a present CRC32 over the stored page body
 */
public record PageReadOptions(int maxHeaderBytes, int maxCompressedPageBytes,
                              int maxUncompressedPageBytes, int maxValuesPerPage,
                              boolean verifyChecksums) {
    /**
     * Bounded legacy-compatible policy; checksum verification is explicitly disabled.
     */
    public static final PageReadOptions DEFAULT = new PageReadOptions(
            1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024, 16 * 1024 * 1024, false);

    /**
     * Same bounds as DEFAULT, with verification of every present page checksum.
     */
    public static final PageReadOptions STRICT = new PageReadOptions(
            DEFAULT.maxHeaderBytes(), DEFAULT.maxCompressedPageBytes(),
            DEFAULT.maxUncompressedPageBytes(), DEFAULT.maxValuesPerPage(), true);

    /**
     * @throws IllegalArgumentException if any resource limit is non-positive
     */
    public PageReadOptions {
        if (maxHeaderBytes <= 0 || maxCompressedPageBytes <= 0
                || maxUncompressedPageBytes <= 0 || maxValuesPerPage <= 0) {
            throw new IllegalArgumentException("Page read limits must all be positive");
        }
    }
}
