package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ParquetException;
import io.github.aloksingh.parquet.model.RowColumnGroup;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/**
 * Failures surfacing from the read and decode paths name their file/source and keep the
 * original failure as the cause.
 */
class ReaderErrorContextTest {
    private static final Path DATA = Path.of("src/test/data/");

    @Test
    void readFailuresNameTheSourceAndKeepTheIoCause() throws Exception {
        Path file = DATA.resolve("non_hadoop_lz4_compressed.parquet");
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            ParquetRowIterator rows = new ParquetRowIterator(reader, false);
            ParquetException failure = assertThrows(ParquetException.class, () -> {
                while (rows.hasNext()) {
                    rows.next();
                }
            });
            assertTrue(failure.getMessage().contains("Failed to read row group 0"),
                    "must name the row group but was: " + failure.getMessage());
            assertTrue(failure.getMessage().contains("non_hadoop_lz4_compressed.parquet"),
                    "must name the file but was: " + failure.getMessage());
            assertInstanceOf(IOException.class, failure.getCause(),
                    "the read failure stays the cause");
        }
    }

    @Test
    void decodeFailuresNameTheSourceColumnAndType() throws Exception {
        Path file = DATA.resolve("int96_from_spark.parquet");
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            RowColumnGroup row = reader.rowIterator().next();
            ParquetException failure = assertThrows(ParquetException.class,
                    () -> row.getColumnValue("a"));
            assertTrue(failure.getMessage().contains("Failed to decode column"),
                    "must name the decode stage but was: " + failure.getMessage());
            assertTrue(failure.getMessage().contains("INT96"),
                    "must name the type but was: " + failure.getMessage());
            assertTrue(failure.getMessage().contains("int96_from_spark.parquet"),
                    "must name the file but was: " + failure.getMessage());
            assertNotNull(failure.getMessage());
        }
    }

    @Test
    void malformedChunkFailuresKeepTheirOwnMessageAndNameTheFile() throws Exception {
        Path file = DATA.resolve("nation.dict-malformed.parquet");
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            ParquetRowIterator rows = new ParquetRowIterator(reader, false);
            ParquetException failure = assertThrows(ParquetException.class, () -> {
                while (rows.hasNext()) {
                    rows.next();
                }
            });
            assertTrue(failure.getMessage().contains("Page body exceeds column chunk boundary"),
                    "the underlying rejection must stay visible but was: " + failure.getMessage());
            assertTrue(failure.getMessage().contains("nation.dict-malformed.parquet"),
                    "must name the file but was: " + failure.getMessage());
        }
    }

    @Test
    void sourceDescriptionIsTheFilePath() throws Exception {
        Path file = DATA.resolve("alltypes_plain.parquet");
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            assertTrue(reader.getSourceDescription().contains("alltypes_plain.parquet"));
            try (ParquetRowIterator rows = reader.rowIterator(ReadOptions.builder().build())) {
                assertTrue(rows.getSourceDescription().contains("alltypes_plain.parquet"));
            }
        }
    }
}
