package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnBatch;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.nio.IntBuffer;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

class DictionaryColumnBatchTest {
    @Test
    void retainsDictionaryIndicesAndMaterializesOnlyRequestedViews() throws Exception {
        ColumnDescriptor descriptor = new ColumnDescriptor(Type.INT32, new String[]{"v"}, 1, 0, 0);
        int[] indexes = {1, -1, 0};
        Object[] dictionary = {10, 20};
        BitSet present = new BitSet();
        present.set(0);
        present.set(2);
        ColumnBatch batch;
        try {
            batch = (ColumnBatch) ColumnBatch.class.getMethod("dictionary", ColumnDescriptor.class,
                    int[].class, Object[].class, BitSet.class).invoke(null, descriptor, indexes, dictionary, present);
        } catch (ReflectiveOperationException e) {
            org.junit.jupiter.api.Assertions.fail("Dictionary-preserving column batches are missing", e);
            return;
        }
        assertTrue((Boolean) ColumnBatch.class.getMethod("isDictionaryEncoded").invoke(batch));
        IntBuffer encoded = (IntBuffer) ColumnBatch.class.getMethod("dictionaryIndices").invoke(batch);
        assertTrue(encoded.isReadOnly());
        assertEquals(1, encoded.get(0));
        assertEquals(20, batch.getObject(0));
        assertNull(batch.getObject(1));
        indexes[0] = 0;
        dictionary[0] = 99;
        assertEquals(20, batch.getObject(0));
        assertEquals(10, batch.getObject(2));
        IntBuffer nativeValues = batch.intValues();
        assertEquals(20, nativeValues.get(0));
        assertEquals(10, nativeValues.get(2));
        assertTrue(nativeValues.isReadOnly());
        assertEquals(1, encoded.get(0));
    }
}
