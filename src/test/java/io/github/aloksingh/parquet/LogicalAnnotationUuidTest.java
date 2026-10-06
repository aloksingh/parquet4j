package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.PrimitiveLogicalType;

import java.util.UUID;

import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

class LogicalAnnotationUuidTest {
    @Test
    void uuidIsBigEndianFixed16AndUnknownNullAnnotationIsRetained() throws Exception {
        SchemaElement source = new SchemaElement("id").setType(Type.FIXED_LEN_BYTE_ARRAY).setType_length(16)
                .setLogicalType(org.apache.parquet.format.LogicalType.UUID(new org.apache.parquet.format.UUIDType()));
        PrimitiveLogicalType annotation = PrimitiveLogicalType.fromSchemaElement(source);
        UUID expected = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        byte[] bytes = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                (byte) 0x88, (byte) 0x99, (byte) 0xaa, (byte) 0xbb, (byte) 0xcc, (byte) 0xdd, (byte) 0xee, (byte) 0xff};
        assertEquals(expected, annotation.toLogicalValue(bytes));
        assertArrayEquals(bytes, (byte[]) annotation.toPhysicalValue(expected));
        assertEquals(PrimitiveLogicalType.Kind.UUID, annotation.kind());
        PrimitiveLogicalType factory = (PrimitiveLogicalType) PrimitiveLogicalType.class.getMethod("uuid").invoke(null);
        assertEquals(annotation, factory);
        assertThrows(IllegalArgumentException.class, () -> annotation.toLogicalValue(new byte[15]));
        assertThrows(IllegalArgumentException.class, () -> new ColumnDescriptor(
                io.github.aloksingh.parquet.model.Type.FIXED_LEN_BYTE_ARRAY, new String[]{"id"}, 0, 0, 15, factory));
        SchemaElement restored = new SchemaElement("id").setType(Type.FIXED_LEN_BYTE_ARRAY).setType_length(16);
        annotation.applyTo(restored);
        assertEquals(source, thriftRoundTrip(restored));
        SchemaElement nullType = new SchemaElement("opaque").setType(Type.INT32)
                .setLogicalType(org.apache.parquet.format.LogicalType.UNKNOWN(new org.apache.parquet.format.NullType()));
        PrimitiveLogicalType unknown = PrimitiveLogicalType.fromSchemaElement(thriftRoundTrip(nullType));
        PrimitiveLogicalType unknownFactory = (PrimitiveLogicalType) PrimitiveLogicalType.class.getMethod("unknown").invoke(null);
        assertEquals(unknown, unknownFactory);
        assertNull(unknown.toLogicalValue(null));
        SchemaElement nullRestored = new SchemaElement("opaque").setType(Type.INT32);
        unknown.applyTo(nullRestored);
        assertEquals(nullType, thriftRoundTrip(nullRestored));
        SchemaElement unknownUnit = new SchemaElement("clock").setType(Type.INT64)
                .setLogicalType(org.apache.parquet.format.LogicalType.TIME(
                        new org.apache.parquet.format.TimeType(false, new org.apache.parquet.format.TimeUnit())));
        PrimitiveLogicalType raw = PrimitiveLogicalType.fromSchemaElement(unknownUnit);
        assertEquals(PrimitiveLogicalType.Kind.UNKNOWN, raw.kind());
        assertEquals(123L, raw.toLogicalValue(123L), "future units must not be guessed as millis");
    }

    private static SchemaElement thriftRoundTrip(SchemaElement source) throws Exception {
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        source.write(new shaded.parquet.org.apache.thrift.protocol.TCompactProtocol(
                new shaded.parquet.org.apache.thrift.transport.TIOStreamTransport(output)));
        SchemaElement restored = new SchemaElement();
        restored.read(new shaded.parquet.org.apache.thrift.protocol.TCompactProtocol(
                new shaded.parquet.org.apache.thrift.transport.TIOStreamTransport(
                        new java.io.ByteArrayInputStream(output.toByteArray()))));
        return restored;
    }
}
