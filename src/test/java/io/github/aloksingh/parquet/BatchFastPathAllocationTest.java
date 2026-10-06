package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ColumnBatch;
import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.ColumnValues;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Allocation sanity for the required nonrepeated fast path (not a benchmark):
 * the unboxed route for a large required column must allocate fewer bytes than
 * the boxed list adapters it feeds, using the ByteStreamSplitSafetyTest technique.
 */
class BatchFastPathAllocationTest {

    @Test
    void requiredFastPathAllocatesFewerBytesThanBoxedList(@TempDir Path tempDir) throws Exception {
        var bean = ManagementFactory.getThreadMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(bean instanceof com.sun.management.ThreadMXBean);
        var allocation = (com.sun.management.ThreadMXBean) bean;
        org.junit.jupiter.api.Assumptions.assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);

        int rows = 65_536;
        Path file = tempDir.resolve("required_large.parquet");
        ColumnDescriptor descriptor = new ColumnDescriptor(Type.INT32, new String[]{"req"}, 0, 0, 0);
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("alloc",
                List.of(new LogicalColumnDescriptor("req", LogicalType.PRIMITIVE, Type.INT32, descriptor)));
        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema)) {
            for (int i = 0; i < rows; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i}));
            }
        }

        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            // Warm up both routes over the same chunk so JIT noise stays out of the numbers.
            for (int i = 0; i < 5; i++) {
                boxedListSize(reader);
                batchSize(reader);
            }
            long boxed = Long.MAX_VALUE;
            long batch = Long.MAX_VALUE;
            for (int i = 0; i < 3; i++) {
                boxed = Math.min(boxed, measure(allocation, () -> boxedListSizeQuiet(reader)));
                batch = Math.min(batch, measure(allocation, () -> batchSizeQuiet(reader)));
            }
            System.out.println("REQUIRED_FAST_PATH boxed list bytes=" + boxed
                    + ", batch bytes=" + batch + ", values=" + rows);
            assertEquals(rows, boxedListSize(reader));
            assertEquals(rows, batchSize(reader));
            assertTrue(batch < boxed,
                    "unboxed batch route must allocate less than the boxed list route: batch=" + batch + " boxed=" + boxed);
        }
    }

    private static long measure(com.sun.management.ThreadMXBean allocation, Runnable work) {
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        work.run();
        return allocation.getThreadAllocatedBytes(thread) - before;
    }

    private static int boxedListSize(ParquetFileReader reader) throws java.io.IOException {
        List<Integer> values = reader.getRowGroup(0).readColumn(0).decodeAsInt32();
        return values.size();
    }

    private static int batchSize(ParquetFileReader reader) throws java.io.IOException {
        ColumnBatch batch = reader.getRowGroup(0).readColumnBatch(0);
        return batch.intValues().remaining();
    }

    private static int boxedListSizeQuiet(ParquetFileReader reader) {
        try {
            return boxedListSize(reader);
        } catch (java.io.IOException failure) {
            throw new RuntimeException(failure);
        }
    }

    private static int batchSizeQuiet(ParquetFileReader reader) {
        try {
            return batchSize(reader);
        } catch (java.io.IOException failure) {
            throw new RuntimeException(failure);
        }
    }

    @Test
    void unboxedAndBoxedRoutesProduceIdenticalValues(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("required_small.parquet");
        ColumnDescriptor descriptor = new ColumnDescriptor(Type.INT32, new String[]{"req"}, 0, 0, 0);
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("same",
                List.of(new LogicalColumnDescriptor("req", LogicalType.PRIMITIVE, Type.INT32, descriptor)));
        try (ParquetFileWriter writer = new ParquetFileWriter(file, schema)) {
            for (int i = 0; i < 1000; i++) {
                writer.addRow(new SimpleRowColumnGroup(schema, new Object[]{i * 3}));
            }
        }
        try (ParquetFileReader reader = new ParquetFileReader(file)) {
            ColumnValues values = reader.getRowGroup(0).readColumn(0);
            int[] unboxed = (int[]) values.decodeRequiredUnboxed();
            List<Integer> boxed = values.decodeAsInt32();
            assertEquals(boxed.size(), unboxed.length);
            for (int i = 0; i < unboxed.length; i++) {
                assertEquals(boxed.get(i).intValue(), unboxed[i], "value at " + i);
            }
        }
    }
}
