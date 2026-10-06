package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.PrimitiveLogicalType;
import io.github.aloksingh.parquet.model.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.SchemaElement;
import org.junit.jupiter.api.Test;

class LogicalAnnotationDecimalTest {
    @Test
    void decimalsUseExactScaledSignedPhysicalValues() throws Exception {
        SchemaElement source = decimalElement(org.apache.parquet.format.Type.INT64, 10, 2);
        PrimitiveLogicalType annotation = PrimitiveLogicalType.fromSchemaElement(source);
        assertEquals(new BigDecimal("-12.34"), annotation.toLogicalValue(-1234L));
        assertEquals(PrimitiveLogicalType.Kind.DECIMAL, annotation.kind());
        assertEquals(10, PrimitiveLogicalType.class.getMethod("precision").invoke(annotation));
        assertEquals(2, PrimitiveLogicalType.class.getMethod("scale").invoke(annotation));
        assertEquals(-1234L, annotation.toPhysicalValue(new BigDecimal("-12.34")));
        assertNull(annotation.toLogicalValue(null));
        assertNull(annotation.toPhysicalValue(null));
        assertThrows(ArithmeticException.class, () -> annotation.toPhysicalValue(new BigDecimal("1.234")));
        assertThrows(ArithmeticException.class, () -> annotation.toPhysicalValue(new BigDecimal("123456789.01")));
        assertThrows(ArithmeticException.class, () -> annotation.toLogicalValue(12345678901L));

        PrimitiveLogicalType int32 = PrimitiveLogicalType.fromSchemaElement(
                decimalElement(org.apache.parquet.format.Type.INT32, 9, 0));
        assertEquals(new BigDecimal("-123"), int32.toLogicalValue(-123));
        assertEquals(-123, int32.toPhysicalValue(new BigDecimal("-123")));

        PrimitiveLogicalType binary = PrimitiveLogicalType.fromSchemaElement(
                decimalElement(org.apache.parquet.format.Type.BYTE_ARRAY, 30, 4));
        BigDecimal wide = new BigDecimal("-123456789012345678901234.5678");
        byte[] encoded = wide.unscaledValue().toByteArray();
        assertEquals(wide, binary.toLogicalValue(encoded));
        ByteBuffer buffer = ByteBuffer.wrap(encoded);
        assertEquals(wide, binary.toLogicalValue(buffer));
        assertEquals(0, buffer.position(), "conversion must not advance caller buffers");
        assertArrayEquals(encoded, (byte[]) binary.toPhysicalValue(wide));
        assertThrows(IllegalArgumentException.class, () -> binary.toLogicalValue(new byte[0]));

        SchemaElement fixedElement = decimalElement(org.apache.parquet.format.Type.FIXED_LEN_BYTE_ARRAY, 8, 2)
                .setType_length(4);
        PrimitiveLogicalType fixed = PrimitiveLogicalType.fromSchemaElement(fixedElement);
        assertArrayEquals(new byte[]{-1, -1, -5, 46}, (byte[]) fixed.toPhysicalValue(new BigDecimal("-12.34")));
        assertEquals(new BigDecimal("-12.34"), fixed.toLogicalValue(new byte[]{-1, -1, -5, 46}));
        assertThrows(IllegalArgumentException.class, () -> fixed.toLogicalValue(new byte[]{1}));

        java.lang.reflect.Method factory = assertDoesNotThrow(() -> PrimitiveLogicalType.class.getMethod(
                "decimal", int.class, int.class));
        PrimitiveLogicalType unbound = (PrimitiveLogicalType) factory.invoke(null, 10, 2);
        assertEquals(BigInteger.valueOf(-1234), unbound.toPhysicalValue(new BigDecimal("-12.34")));
        ColumnDescriptor bound = new ColumnDescriptor(Type.INT64, new String[]{"amount"}, 0, 0, 0, unbound);
        assertEquals(-1234L, bound.annotation().toPhysicalValue(new BigDecimal("-12.34")));
        assertEquals(unbound, annotation, "equivalent legacy/modern annotations have value identity");
        SchemaElement serialized = decimalElement(org.apache.parquet.format.Type.INT64, 10, 2);
        unbound.applyTo(serialized);
        assertTrue(serialized.getLogicalType().isSetDECIMAL());
        assertEquals(10, serialized.getPrecision());
        assertEquals(2, serialized.getScale());
        assertEquals(source, serialized);
        assertNotEquals(bound, new ColumnDescriptor(Type.INT64, bound.path(), 0, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> PrimitiveLogicalType.fromSchemaElement(
                decimalElement(org.apache.parquet.format.Type.INT32, 10, 2)));
        assertThrows(IllegalArgumentException.class, () -> PrimitiveLogicalType.fromSchemaElement(
                decimalElement(org.apache.parquet.format.Type.INT64, 0, 0)));
        assertThrows(IllegalArgumentException.class, () -> PrimitiveLogicalType.fromSchemaElement(
                decimalElement(org.apache.parquet.format.Type.INT64, 10, -1)));
        assertThrows(IllegalArgumentException.class, () -> PrimitiveLogicalType.fromSchemaElement(
                decimalElement(org.apache.parquet.format.Type.INT64, 2, 3)));
        assertThrows(IllegalArgumentException.class, () -> PrimitiveLogicalType.fromSchemaElement(
                decimalElement(org.apache.parquet.format.Type.FIXED_LEN_BYTE_ARRAY, 3, 0).setType_length(1)));
    }

    private static SchemaElement decimalElement(org.apache.parquet.format.Type type, int precision, int scale) {
        return new SchemaElement("amount").setType(type).setConverted_type(ConvertedType.DECIMAL)
                .setPrecision(precision).setScale(scale)
                .setLogicalType(org.apache.parquet.format.LogicalType.DECIMAL(new DecimalType(scale, precision)));
    }
}
