package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class LogicalAnnotationTest {
    @Test
    void stringMetadataRoundTripsTheActualModernAndLegacyFields() throws Exception {
        Class<?> annotationType = Class.forName("io.github.aloksingh.parquet.model.PrimitiveLogicalType");
        java.lang.reflect.Method read = assertDoesNotThrow(() -> annotationType.getMethod(
                "fromSchemaElement", org.apache.parquet.format.SchemaElement.class));
        java.lang.reflect.Method write = assertDoesNotThrow(() -> annotationType.getMethod(
                "applyTo", org.apache.parquet.format.SchemaElement.class));
        org.apache.parquet.format.SchemaElement modern = binaryElement("text")
                .setLogicalType(org.apache.parquet.format.LogicalType.STRING(new org.apache.parquet.format.StringType()));
        org.apache.parquet.format.SchemaElement legacy = binaryElement("text")
                .setConverted_type(org.apache.parquet.format.ConvertedType.UTF8);
        org.apache.parquet.format.SchemaElement both = modern.deepCopy()
                .setConverted_type(org.apache.parquet.format.ConvertedType.UTF8);
        for (org.apache.parquet.format.SchemaElement source : java.util.List.of(modern, legacy, both)) {
            Object annotation = read.invoke(null, thriftRoundTrip(source));
            org.apache.parquet.format.SchemaElement target = binaryElement("text")
                    .setConverted_type(org.apache.parquet.format.ConvertedType.JSON).setPrecision(9).setScale(1);
            write.invoke(annotation, target);
            assertEquals(source, thriftRoundTrip(target));
            assertEquals("STRING", annotationType.getMethod("kind").invoke(annotation).toString());
            assertEquals(annotationType.getMethod("string").invoke(null), annotation);
        }
        org.apache.parquet.format.SchemaElement interval = binaryElement("opaque")
                .setConverted_type(org.apache.parquet.format.ConvertedType.INTERVAL).setPrecision(7).setScale(2);
        Object unknown = read.invoke(null, interval);
        org.apache.parquet.format.SchemaElement output = binaryElement("opaque");
        write.invoke(unknown, output);
        assertEquals(interval, thriftRoundTrip(output), "representable unsupported metadata is not erased");
        assertEquals("UNKNOWN", annotationType.getMethod("kind").invoke(unknown).toString());
        byte[] raw = {1, 2, 3};
        assertSame(raw, annotationType.getMethod("toLogicalValue", Object.class).invoke(unknown, raw));
        Object copy = read.invoke(null, modern);
        modern.getLogicalType().setJSON(new org.apache.parquet.format.JsonType());
        org.apache.parquet.format.SchemaElement copiedOutput = binaryElement("text");
        write.invoke(copy, copiedOutput);
        assertTrue(copiedOutput.getLogicalType().isSetSTRING(), "input Thrift structs must be deep copied");
        copiedOutput.getLogicalType().setJSON(new org.apache.parquet.format.JsonType());
        write.invoke(copy, copiedOutput);
        assertTrue(copiedOutput.getLogicalType().isSetSTRING(), "output Thrift structs must not expose ownership");
    }

    private static org.apache.parquet.format.SchemaElement binaryElement(String name) {
        return new org.apache.parquet.format.SchemaElement(name)
                .setType(org.apache.parquet.format.Type.BYTE_ARRAY)
                .setRepetition_type(org.apache.parquet.format.FieldRepetitionType.OPTIONAL);
    }

    private static org.apache.parquet.format.SchemaElement thriftRoundTrip(
            org.apache.parquet.format.SchemaElement source) throws Exception {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        source.write(new shaded.parquet.org.apache.thrift.protocol.TCompactProtocol(
                new shaded.parquet.org.apache.thrift.transport.TIOStreamTransport(bytes)));
        org.apache.parquet.format.SchemaElement restored = new org.apache.parquet.format.SchemaElement();
        restored.read(new shaded.parquet.org.apache.thrift.protocol.TCompactProtocol(
                new shaded.parquet.org.apache.thrift.transport.TIOStreamTransport(
                        new java.io.ByteArrayInputStream(bytes.toByteArray()))));
        return restored;
    }

    @Test
    void explicitStringAnnotationDistinguishesUtf8FromUnannotatedBinary() throws Exception {
        Class<?> annotationType = assertDoesNotThrow(
                () -> Class.forName("io.github.aloksingh.parquet.model.PrimitiveLogicalType"),
                "primitive logical annotations must have an immutable public model");
        Object none = annotationType.getMethod("none").invoke(null);
        Object string = annotationType.getMethod("string").invoke(null);
        byte[] binary = {(byte) 0xff, 0, 1};
        assertSame(binary, annotationType.getMethod("toLogicalValue", Object.class).invoke(none, binary));
        assertEquals("NONE", annotationType.getMethod("kind").invoke(none).toString());
        assertEquals("STRING", annotationType.getMethod("kind").invoke(string).toString());
        assertEquals(true, annotationType.getMethod("isString").invoke(string));
        byte[] utf8 = "héllo 世界".getBytes(StandardCharsets.UTF_8);
        assertEquals("héllo 世界",
                annotationType.getMethod("toLogicalValue", Object.class).invoke(string, utf8));
        assertArrayEquals(utf8, (byte[]) annotationType.getMethod("toPhysicalValue", Object.class)
                .invoke(string, "héllo 世界"));
        assertNull(annotationType.getMethod("toLogicalValue", Object.class).invoke(string, new Object[]{null}));
        ColumnDescriptor legacy = new ColumnDescriptor(Type.BYTE_ARRAY, new String[]{"raw"}, 0, 0, 0);
        assertEquals(none, ColumnDescriptor.class.getMethod("annotation").invoke(legacy));
        assertEquals(6, ColumnDescriptor.class.getRecordComponents().length);
    }
}
