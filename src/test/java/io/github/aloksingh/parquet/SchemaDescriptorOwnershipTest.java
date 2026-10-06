package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.util.HashMap;

import org.junit.jupiter.api.Test;

class SchemaDescriptorOwnershipTest {
    @Test
    void columnPathsAreOwnedAndComparedByValue() {
        String[] path = {"record", "amount"};
        ColumnDescriptor column = new ColumnDescriptor(Type.INT64, path, 1, 0, 0);
        path[0] = "mutated";
        assertEquals("record.amount", column.getPathString(), "constructor must own its path");
        column.path()[1] = "mutated";
        assertEquals("record.amount", column.getPathString(), "accessor must not expose its path");
        ColumnDescriptor equivalent = new ColumnDescriptor(Type.INT64,
                new String[]{"record", "amount"}, 1, 0, 0);
        assertEquals(column, equivalent);
        assertEquals(column.hashCode(), equivalent.hashCode());
        HashMap<ColumnDescriptor, String> lookup = new HashMap<>();
        lookup.put(column, "found");
        assertEquals("found", lookup.get(equivalent));
        assertNotEquals(column, new ColumnDescriptor(Type.INT32, equivalent.path(), 1, 0, 0));
    }
}
