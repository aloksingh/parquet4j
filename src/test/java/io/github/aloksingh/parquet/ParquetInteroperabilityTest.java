package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Independent engine checks, not matching private reader/writer round trips.
 */
class ParquetInteroperabilityTest {
    @TempDir
    Path temporary;

    @Test
    void independentReaderPreservesPackedBooleanValuesAcrossByteBoundary() throws Exception {
        ColumnDescriptor physical = new ColumnDescriptor(Type.BOOLEAN, new String[]{"flag"}, 0, 0, 0);
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("boolean_interop",
                List.of(new LogicalColumnDescriptor("flag", LogicalType.PRIMITIVE, Type.BOOLEAN, physical)));
        Path file = temporary.resolve("booleans.parquet");
        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema)) {
            for (int i = 0; i < 9; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i % 2 != 0}));
            }
        }
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             PreparedStatement statement = connection.prepareStatement("SELECT flag FROM read_parquet(?)")) {
            statement.setString(1, file.toString());
            try (ResultSet rows = statement.executeQuery()) {
                for (int i = 0; i < 9; i++) {
                    assertTrue(rows.next(), "Missing row " + i);
                    assertEquals(i % 2 != 0, rows.getBoolean(1), "Incorrect packed BOOLEAN at row " + i);
                    assertFalse(rows.wasNull());
                }
                assertFalse(rows.next());
            }
        }
    }
}
