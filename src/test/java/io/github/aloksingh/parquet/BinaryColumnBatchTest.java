package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnBatch;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

class BinaryColumnBatchTest {
    @Test
    void binaryOffsetsDistinguishNullEmptyAndArbitraryBytesWithoutLoss() throws Exception {
        ColumnDescriptor descriptor = new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"blob"}, 1, 0, 0);
        byte[] bytes = {(byte) 0xff, 0, (byte) 0x80};
        int[] offsets = {0, 3, 3, 3};
        BitSet valid = new BitSet();
        valid.set(0);
        valid.set(2);
        ColumnBatch batch;
        try {
            batch = (ColumnBatch) ColumnBatch.class.getMethod("binary", ColumnDescriptor.class,
                    int[].class, ByteBuffer.class, BitSet.class).invoke(null, descriptor, offsets, ByteBuffer.wrap(bytes), valid);
        } catch (ReflectiveOperationException e) {
            org.junit.jupiter.api.Assertions.fail("Binary column batches are missing", e);
            return;
        }
        assertEquals(3, batch.size());
        assertArrayEquals(bytes, (byte[]) batch.getObject(0));
        assertNull(batch.getObject(1));
        assertArrayEquals(new byte[0], (byte[]) batch.getObject(2));
        bytes[0] = 1;
        offsets[1] = 0;
        valid.clear();
        assertArrayEquals(new byte[]{(byte) 0xff, 0, (byte) 0x80}, (byte[]) batch.getObject(0));
        ByteBuffer view = (ByteBuffer) ColumnBatch.class.getMethod("getBytes", int.class).invoke(batch, 0);
        assertTrue(view.isReadOnly());
        assertThrows(ReadOnlyBufferException.class, () -> view.put(0, (byte) 1));
    }
}
