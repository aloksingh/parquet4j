package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.PrimitiveLogicalType;

import java.math.BigInteger;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

class LogicalAnnotationIntegerTest {
    @Test
    void annotatedUnsignedValuesUseWiderExactLogicalCarriersAndCheckedBounds() throws Exception {
        PrimitiveLogicalType uint32 = integer(32, false);
        assertEquals(4_294_967_295L, uint32.toLogicalValue(-1));
        assertEquals(-1, uint32.toPhysicalValue(4_294_967_295L));
        PrimitiveLogicalType uint64 = integer(64, false);
        BigInteger max64 = new BigInteger("18446744073709551615");
        assertEquals(max64, uint64.toLogicalValue(-1L));
        assertEquals(-1L, uint64.toPhysicalValue(max64));
        assertEquals(BigInteger.ZERO, uint64.toLogicalValue(0L));
        for (int width : new int[]{8, 16, 32, 64}) {
            for (boolean signed : new boolean[]{true, false}) {
                PrimitiveLogicalType modern = integer(width, signed);
                ConvertedType converted = ConvertedType.valueOf((signed ? "INT_" : "UINT_") + width);
                SchemaElement legacyElement = new SchemaElement("number").setType(width == 64 ? Type.INT64 : Type.INT32)
                        .setConverted_type(converted);
                PrimitiveLogicalType legacy = PrimitiveLogicalType.fromSchemaElement(legacyElement);
                assertEquals(modern, legacy);
                assertEquals(modern.hashCode(), legacy.hashCode());
                assertEquals(width, PrimitiveLogicalType.class.getMethod("bitWidth").invoke(modern));
                assertEquals(signed, PrimitiveLogicalType.class.getMethod("isSigned").invoke(modern));
                PrimitiveLogicalType factory = (PrimitiveLogicalType) PrimitiveLogicalType.class
                        .getMethod("integer", int.class, boolean.class).invoke(null, width, signed);
                assertEquals(modern, factory);
                BigInteger min = signed ? BigInteger.ONE.shiftLeft(width - 1).negate() : BigInteger.ZERO;
                BigInteger max = BigInteger.ONE.shiftLeft(signed ? width - 1 : width).subtract(BigInteger.ONE);
                for (BigInteger value : new BigInteger[]{min, BigInteger.ZERO, max}) {
                    Object physical = modern.toPhysicalValue(value);
                    Object logical = modern.toLogicalValue(physical);
                    assertEquals(value, new BigInteger(logical.toString()));
                    assertEquals(width == 64 ? Long.class : Integer.class, physical.getClass());
                }
                assertThrows(ArithmeticException.class, () -> modern.toPhysicalValue(min.subtract(BigInteger.ONE)));
                assertThrows(ArithmeticException.class, () -> modern.toPhysicalValue(max.add(BigInteger.ONE)));
                SchemaElement restored = new SchemaElement("number").setType(legacyElement.getType());
                legacy.applyTo(restored);
                assertEquals(legacyElement, restored);
                SchemaElement factorySchema = new SchemaElement("number").setType(legacyElement.getType());
                factory.applyTo(factorySchema);
                assertEquals(converted, factorySchema.getConverted_type());
                assertTrue(factorySchema.getLogicalType().isSetINTEGER());
            }
        }
        assertThrows(ArithmeticException.class, () -> integer(8, false).toLogicalValue(-1));
        assertThrows(ArithmeticException.class, () -> integer(16, false).toLogicalValue(65536));
        assertThrows(ArithmeticException.class, () -> integer(8, true).toLogicalValue(128));
        assertThrows(IllegalArgumentException.class, () -> integer(24, true));
        assertThrows(IllegalArgumentException.class, () -> uint64.toPhysicalValue(1.5));
        assertNull(uint64.toPhysicalValue(null));
        Integer raw = -1;
        assertSame(raw, PrimitiveLogicalType.none().toLogicalValue(raw), "unannotated integers remain raw");
        assertNotEquals(uint32, integer(32, true));
        assertNotEquals(uint32, uint64);
    }

    private static PrimitiveLogicalType integer(int width, boolean signed) {
        return PrimitiveLogicalType.fromSchemaElement(new SchemaElement("number")
                .setType(width == 64 ? Type.INT64 : Type.INT32)
                .setLogicalType(org.apache.parquet.format.LogicalType.INTEGER(new IntType((byte) width, signed))));
    }
}
