package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aloksingh.parquet.model.PrimitiveLogicalType;

import java.time.LocalDate;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DateType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.junit.jupiter.api.Test;

class LogicalAnnotationTemporalTest {
    @Test
    void timestampDistinguishesInstantsFromLocalWallClockWithoutRounding() throws Exception {
        org.apache.parquet.format.TimeUnit[] units = {
                org.apache.parquet.format.TimeUnit.MILLIS(new org.apache.parquet.format.MilliSeconds()),
                org.apache.parquet.format.TimeUnit.MICROS(new org.apache.parquet.format.MicroSeconds()),
                org.apache.parquet.format.TimeUnit.NANOS(new org.apache.parquet.format.NanoSeconds())};
        long[] nanos = {1_000_000, 1_000, 1};
        for (int i = 0; i < units.length; i++) {
            for (boolean utc : new boolean[]{false, true}) {
                SchemaElement source = new SchemaElement("created").setType(Type.INT64)
                        .setLogicalType(org.apache.parquet.format.LogicalType.TIMESTAMP(
                                new org.apache.parquet.format.TimestampType(utc, units[i])));
                PrimitiveLogicalType annotation = PrimitiveLogicalType.fromSchemaElement(source);
                java.time.Instant instant = java.time.Instant.ofEpochSecond(-1, 1_000_000_000 - nanos[i]);
                Object expected = utc ? instant : java.time.LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC);
                assertEquals(expected, annotation.toLogicalValue(-1L));
                assertEquals(-1L, annotation.toPhysicalValue(expected));
                assertEquals(utc, annotation.adjustedToUTC());
                assertEquals(PrimitiveLogicalType.Kind.TIMESTAMP, annotation.kind());
                for (long physical : new long[]{Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
                    assertEquals(physical, annotation.toPhysicalValue(annotation.toLogicalValue(physical)));
                }
                SchemaElement restored = new SchemaElement("created").setType(Type.INT64);
                annotation.applyTo(restored);
                assertEquals(source, restored);
                PrimitiveLogicalType factory = (PrimitiveLogicalType) PrimitiveLogicalType.class
                        .getMethod("timestamp", PrimitiveLogicalType.TimeUnit.class, boolean.class)
                        .invoke(null, annotation.unit(), utc);
                PrimitiveLogicalType reversed = (PrimitiveLogicalType) PrimitiveLogicalType.class
                        .getMethod("timestamp", boolean.class, PrimitiveLogicalType.TimeUnit.class)
                        .invoke(null, utc, annotation.unit());
                assertEquals(annotation, factory);
                assertEquals(factory, reversed);
                assertEquals(annotation.hashCode(), factory.hashCode());
            }
        }
        PrimitiveLogicalType millis = PrimitiveLogicalType.fromSchemaElement(new SchemaElement("created")
                .setType(Type.INT64).setConverted_type(ConvertedType.TIMESTAMP_MILLIS));
        assertEquals(java.time.Instant.EPOCH, millis.toLogicalValue(0L));
        assertThrows(ArithmeticException.class, () -> millis.toPhysicalValue(java.time.Instant.ofEpochSecond(0, 1)));
        assertThrows(ArithmeticException.class, () -> millis.toPhysicalValue(java.time.Instant.MAX));
        assertThrows(ClassCastException.class, () -> millis.toPhysicalValue(java.time.LocalDateTime.of(1970, 1, 1, 0, 0)));
        assertNull(millis.toPhysicalValue(null));
        PrimitiveLogicalType local = PrimitiveLogicalType.fromSchemaElement(new SchemaElement("created")
                .setType(Type.INT64).setLogicalType(org.apache.parquet.format.LogicalType.TIMESTAMP(
                        new org.apache.parquet.format.TimestampType(false, units[1]))));
        assertNotEquals(local, millis);
        assertThrows(ClassCastException.class, () -> local.toPhysicalValue(java.time.Instant.EPOCH));
    }

    @Test
    void timeRetainsUnitAndUtcFlagAndRejectsLossyOrOutOfDayValues() throws Exception {
        org.apache.parquet.format.TimeUnit[] units = {
                org.apache.parquet.format.TimeUnit.MILLIS(new org.apache.parquet.format.MilliSeconds()),
                org.apache.parquet.format.TimeUnit.MICROS(new org.apache.parquet.format.MicroSeconds()),
                org.apache.parquet.format.TimeUnit.NANOS(new org.apache.parquet.format.NanoSeconds())};
        long[] nanos = {1_000_000, 1_000, 1};
        String[] names = {"MILLIS", "MICROS", "NANOS"};
        for (int i = 0; i < units.length; i++) {
            for (boolean utc : new boolean[]{false, true}) {
                SchemaElement source = new SchemaElement("clock").setType(i == 0 ? Type.INT32 : Type.INT64)
                        .setLogicalType(org.apache.parquet.format.LogicalType.TIME(
                                new org.apache.parquet.format.TimeType(utc, units[i])));
                PrimitiveLogicalType annotation = PrimitiveLogicalType.fromSchemaElement(source);
                assertEquals(java.time.LocalTime.ofNanoOfDay(nanos[i]), annotation.toLogicalValue(1L));
                Object one = i == 0 ? (Object) Integer.valueOf(1) : Long.valueOf(1);
                assertEquals(one, annotation.toPhysicalValue(java.time.LocalTime.ofNanoOfDay(nanos[i])));
                assertEquals(names[i], PrimitiveLogicalType.class.getMethod("unit").invoke(annotation).toString());
                assertEquals(utc, PrimitiveLogicalType.class.getMethod("adjustedToUTC").invoke(annotation));
                SchemaElement restored = new SchemaElement("clock").setType(source.getType());
                annotation.applyTo(restored);
                assertEquals(source, restored);
                assertThrows(IllegalArgumentException.class, () -> annotation.toLogicalValue(-1L));
                assertThrows(IllegalArgumentException.class, () -> annotation.toLogicalValue(Long.MAX_VALUE));
                Class<?> unitClass = Class.forName("io.github.aloksingh.parquet.model.PrimitiveLogicalType$TimeUnit");
                Object unit = unitClass.getMethod("valueOf", String.class).invoke(null, names[i]);
                PrimitiveLogicalType factory = (PrimitiveLogicalType) PrimitiveLogicalType.class
                        .getMethod("time", unitClass, boolean.class).invoke(null, unit, utc);
                PrimitiveLogicalType reverseFactory = (PrimitiveLogicalType) PrimitiveLogicalType.class
                        .getMethod("time", boolean.class, unitClass).invoke(null, utc, unit);
                assertEquals(annotation, factory);
                assertEquals(factory, reverseFactory);
                assertEquals(annotation.hashCode(), factory.hashCode());
            }
        }
        PrimitiveLogicalType millis = PrimitiveLogicalType.fromSchemaElement(
                new SchemaElement("clock").setType(Type.INT32).setConverted_type(ConvertedType.TIME_MILLIS));
        assertThrows(ArithmeticException.class, () -> millis.toPhysicalValue(java.time.LocalTime.ofNanoOfDay(1)));
        assertNotEquals(millis, PrimitiveLogicalType.fromSchemaElement(new SchemaElement("clock").setType(Type.INT32)
                .setLogicalType(org.apache.parquet.format.LogicalType.TIME(new org.apache.parquet.format.TimeType(false, units[0])))));
        assertEquals(true, PrimitiveLogicalType.class.getMethod("adjustedToUTC").invoke(millis));
        assertNull(millis.toPhysicalValue(null));
    }

    @Test
    void dateUsesSignedEpochDaysWithCheckedInt32Encoding() throws Exception {
        SchemaElement legacy = new SchemaElement("day").setType(Type.INT32).setConverted_type(ConvertedType.DATE);
        PrimitiveLogicalType annotation = PrimitiveLogicalType.fromSchemaElement(legacy);
        assertEquals(LocalDate.of(1969, 12, 31), annotation.toLogicalValue(-1));
        assertEquals(-1, annotation.toPhysicalValue(LocalDate.of(1969, 12, 31)));
        assertEquals(PrimitiveLogicalType.Kind.DATE, annotation.kind());
        SchemaElement modern = new SchemaElement("day").setType(Type.INT32)
                .setLogicalType(org.apache.parquet.format.LogicalType.DATE(new DateType()));
        PrimitiveLogicalType modernAnnotation = PrimitiveLogicalType.fromSchemaElement(modern);
        assertEquals(annotation, modernAnnotation);
        assertEquals(annotation.hashCode(), modernAnnotation.hashCode());
        SchemaElement restored = new SchemaElement("day").setType(Type.INT32);
        annotation.applyTo(restored);
        assertEquals(legacy, restored);
        PrimitiveLogicalType factory = (PrimitiveLogicalType) assertDoesNotThrow(
                () -> PrimitiveLogicalType.class.getMethod("date")).invoke(null);
        assertEquals(annotation, factory);
        assertEquals(LocalDate.ofEpochDay(Integer.MIN_VALUE), factory.toLogicalValue(Integer.MIN_VALUE));
        assertEquals(Integer.MAX_VALUE, factory.toPhysicalValue(LocalDate.ofEpochDay(Integer.MAX_VALUE)));
        assertThrows(ArithmeticException.class, () -> factory.toPhysicalValue(LocalDate.MAX));
        assertThrows(ArithmeticException.class, () -> factory.toLogicalValue(Long.MAX_VALUE));
        assertNull(factory.toLogicalValue(null));
    }
}
