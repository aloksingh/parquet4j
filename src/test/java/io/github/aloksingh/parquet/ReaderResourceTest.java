package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.aloksingh.parquet.model.ParquetException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReaderResourceTest {
    @TempDir
    Path temporary;

    @Test
    void invalidMetadataClosesInternallyOpenedFiles() throws Exception {
        Path descriptors = Path.of("/proc/self/fd");
        assumeTrue(Files.isDirectory(descriptors), "Descriptor counting requires Linux /proc");
        try (ParquetFileReader warmup = new ParquetFileReader(Path.of("src/test/data/alltypes_plain.parquet"))) {
            warmup.getMetadata();
        }
        Path invalid = temporary.resolve("invalid.parquet");
        Files.writeString(invalid, "not parquet!", StandardCharsets.UTF_8);
        // Initialize the invalid-file exception path before measuring resources.
        assertThrows(ParquetException.class, () -> new ParquetFileReader(invalid));
        long before = descriptorCount(descriptors);
        for (int i = 0; i < 25; i++) {
            assertThrows(ParquetException.class, () -> new ParquetFileReader(invalid));
        }
        assertEquals(before, descriptorCount(descriptors),
                "A failed owning-reader constructor must close its input immediately, without relying on GC");
    }

    private static long descriptorCount(Path directory) throws Exception {
        try (Stream<Path> descriptors = Files.list(directory)) {
            return descriptors.count();
        }
    }
}
