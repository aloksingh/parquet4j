package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.SimpleRowColumnGroup;
import io.github.aloksingh.parquet.model.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriterSnapshotTest {
    @TempDir
    Path directory;

    @Test
    void acceptedRowIsEncodedWithoutRetainingCallerRowsMapsArraysOrBuffers() throws Exception {
        List<ColumnDescriptor> primitive = List.of(
                new ColumnDescriptor(Type.INT32, new String[]{"id"}, 0, 0, 0),
                new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"raw"}, 0, 0, 0),
                new ColumnDescriptor(Type.FIXED_LEN_BYTE_ARRAY, new String[]{"fixed"}, 0, 0, 3));
        List<LogicalColumnDescriptor> logical = new java.util.ArrayList<>(
                SchemaDescriptor.createLogicalColumnsFromPhysical(primitive));
        logical.add(SchemaDescriptor.createMapColumn("map", Type.BYTE_ARRAY, Type.BYTE_ARRAY, true, true));
        SchemaDescriptor schema = SchemaDescriptor.fromLogicalColumns("snapshots", logical);
        byte[] raw = {'a', 'b'};
        byte[] fixed = {'x', 'y', 'z'};
        ByteBuffer buffer = ByteBuffer.wrap(fixed);
        byte[] mapValue = {'c', 'd'};
        Map<String, byte[]> map = new LinkedHashMap<>();
        map.put("k", mapValue);
        Object[] mutableRow = {1, raw, buffer, map};
        int[] reads = new int[4];
        SimpleRowColumnGroup row = new SimpleRowColumnGroup(schema, mutableRow) {
            @Override
            public Object getColumnValue(int index) {
                if (++reads[index] != 1) throw new IllegalStateException("Row getter called after acceptance");
                return super.getColumnValue(index);
            }

            @Override
            public Object getColumnValue(String name) {
                throw new IllegalStateException("Writer must use bound logical indexes");
            }
        };
        Path destination = directory.resolve("snapshot.parquet");
        ParquetFileWriter writer = new ParquetFileWriter(destination, schema);
        writer.addRow(row);
        mutableRow[0] = 99;
        raw[0] = 'q';
        fixed[0] = 'q';
        buffer.position(2);
        mapValue[0] = 'q';
        map.clear();
        assertDoesNotThrow(writer::close);
        assertArrayEquals(new int[]{1, 1, 1, 1}, reads);
        assertArrayEquals(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(1).array(),
                WriterTestSupport.firstPageValues(destination, 0, 0, 0));
        assertArrayEquals(binary('a', 'b'), WriterTestSupport.firstPageValues(destination, 1, 0, 0));
        assertArrayEquals(new byte[]{'x', 'y', 'z'}, WriterTestSupport.firstPageValues(destination, 2, 0, 0));
        assertArrayEquals(binary('k'), WriterTestSupport.firstPageValues(destination, 3, 1, 2));
        assertArrayEquals(binary('c', 'd'), WriterTestSupport.firstPageValues(destination, 4, 1, 3));
        var columns = WriterTestSupport.footer(destination).getRow_groups().getFirst().getColumns();
        assertArrayEquals(new byte[]{'a', 'b'}, columns.get(1).getMeta_data().getStatistics().getMin_value());
        assertArrayEquals(new byte[]{'c', 'd'}, columns.get(4).getMeta_data().getStatistics().getMin_value());
    }

    private static byte[] binary(char... characters) {
        ByteBuffer result = ByteBuffer.allocate(4 + characters.length).order(ByteOrder.LITTLE_ENDIAN);
        result.putInt(characters.length);
        for (char character : characters) result.put((byte) character);
        return result.array();
    }
}
