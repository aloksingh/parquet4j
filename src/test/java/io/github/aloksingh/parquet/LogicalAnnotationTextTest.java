package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.PrimitiveLogicalType;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

class LogicalAnnotationTextTest {
    @Test
    void enumAndJsonAreStrictUtf8WhileBsonAndUnsupportedCarriersStayBinary() throws Exception {
        List<org.apache.parquet.format.LogicalType> modern = List.of(
                org.apache.parquet.format.LogicalType.ENUM(new org.apache.parquet.format.EnumType()),
                org.apache.parquet.format.LogicalType.JSON(new org.apache.parquet.format.JsonType()),
                org.apache.parquet.format.LogicalType.BSON(new org.apache.parquet.format.BsonType()));
        ConvertedType[] legacy = {ConvertedType.ENUM, ConvertedType.JSON, ConvertedType.BSON};
        String[] factories = {"enumeration", "json", "bson"};
        byte[] raw = "{\"text\":\"世界\"}".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < modern.size(); i++) {
            SchemaElement source = new SchemaElement("text").setType(Type.BYTE_ARRAY).setLogicalType(modern.get(i));
            PrimitiveLogicalType annotation = PrimitiveLogicalType.fromSchemaElement(source);
            assertEquals(legacy[i].name(), annotation.kind().name());
            if (i < 2) {
                assertEquals("{\"text\":\"世界\"}", annotation.toLogicalValue(raw));
                assertArrayEquals(raw, (byte[]) annotation.toPhysicalValue("{\"text\":\"世界\"}"));
            } else {
                assertSame(raw, annotation.toLogicalValue(raw));
                assertSame(raw, annotation.toPhysicalValue(raw));
            }
            PrimitiveLogicalType converted = PrimitiveLogicalType.fromSchemaElement(
                    new SchemaElement("text").setType(Type.BYTE_ARRAY).setConverted_type(legacy[i]));
            PrimitiveLogicalType factory = (PrimitiveLogicalType) PrimitiveLogicalType.class.getMethod(factories[i]).invoke(null);
            assertEquals(converted, annotation);
            assertEquals(factory, annotation);
            SchemaElement restored = new SchemaElement("text").setType(Type.BYTE_ARRAY);
            annotation.applyTo(restored);
            assertEquals(source, restored);
        }
        assertThrows(IllegalArgumentException.class, () -> PrimitiveLogicalType.string().toLogicalValue(new byte[]{(byte) 0xff}));
        assertThrows(IllegalArgumentException.class, () -> PrimitiveLogicalType.string().toPhysicalValue("\ud800"));
        SchemaElement unsupported = new SchemaElement("opaque").setType(Type.INT64)
                .setConverted_type(ConvertedType.UTF8)
                .setLogicalType(org.apache.parquet.format.LogicalType.STRING(new org.apache.parquet.format.StringType()));
        PrimitiveLogicalType annotation = PrimitiveLogicalType.fromSchemaElement(unsupported);
        assertEquals(PrimitiveLogicalType.Kind.UNKNOWN, annotation.kind());
        assertEquals(42L, annotation.toLogicalValue(42L));
        SchemaElement restored = new SchemaElement("opaque").setType(Type.INT64);
        annotation.applyTo(restored);
        assertEquals(unsupported, restored);
        assertThrows(IllegalArgumentException.class, () -> new ColumnDescriptor(
                io.github.aloksingh.parquet.model.Type.INT64, new String[]{"text"}, 0, 0, 0,
                PrimitiveLogicalType.string()));
        assertThrows(IllegalArgumentException.class, () -> new ColumnDescriptor(
                io.github.aloksingh.parquet.model.Type.INT64, new String[]{"day"}, 0, 0, 0,
                PrimitiveLogicalType.date()));
    }
}
