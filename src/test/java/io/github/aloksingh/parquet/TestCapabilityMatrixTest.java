package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

class TestCapabilityMatrixTest {
    @Test
    void corpusSuiteIncludesExpectedFeatureDescriptors() throws Exception {
        Set<String> cases = ParquetJsonValidationTest.parquetFilesWithJson()
                .map(c -> c.fileName.toLowerCase().replace(".parquet", ""))
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        Set<String> required = Set.of("alltypes_plain", "binary", "fixed_length_decimal",
                "rle_boolean_encoding", "null_list", "nan_in_stats");
        Set<String> missing = new LinkedHashSet<>(required);
        missing.removeAll(cases);
        assertTrue(missing.isEmpty(), "Corpus validation is vacuous for expected core fixtures: " + missing);
    }
}
