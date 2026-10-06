package io.github.aloksingh.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

class PageReadOptionsTest {
    @Test
    void exposesValidatedImmutableLimitsWithAnExplicitChecksumCompatibilityDefault() throws Exception {
        Class<?> type = assertDoesNotThrow(() -> Class.forName("io.github.aloksingh.parquet.PageReadOptions"));
        assertTrue(type.isRecord());
        assertNotNull(PageReader.class.getConstructor(ChunkReader.class,
                io.github.aloksingh.parquet.model.ParquetMetadata.ColumnChunkMetadata.class,
                io.github.aloksingh.parquet.model.ColumnDescriptor.class, type));
        assertEquals(Arrays.asList("maxHeaderBytes", "maxCompressedPageBytes", "maxUncompressedPageBytes",
                        "maxValuesPerPage", "verifyChecksums"),
                Arrays.stream(type.getRecordComponents()).map(java.lang.reflect.RecordComponent::getName).toList());
        Object defaults = type.getField("DEFAULT").get(null);
        Object strict = type.getField("STRICT").get(null);
        String[] limits = {"maxHeaderBytes", "maxCompressedPageBytes", "maxUncompressedPageBytes", "maxValuesPerPage"};
        int[] expected = {1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024, 16 * 1024 * 1024};
        for (int i = 0; i < limits.length; i++) {
            assertEquals(expected[i], type.getMethod(limits[i]).invoke(defaults));
            assertEquals(expected[i], type.getMethod(limits[i]).invoke(strict));
        }
        assertEquals(false, type.getMethod("verifyChecksums").invoke(defaults));
        assertEquals(true, type.getMethod("verifyChecksums").invoke(strict));
        var constructor = type.getConstructor(int.class, int.class, int.class, int.class, boolean.class);
        assertNotNull(constructor.newInstance(1, 2, 3, 4, true));
        for (int i = 0; i < 4; i++) {
            Object[] arguments = {1, 2, 3, 4, false};
            arguments[i] = 0;
            InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                    () -> constructor.newInstance(arguments));
            assertInstanceOf(IllegalArgumentException.class, failure.getCause());
            arguments[i] = -1;
            assertInstanceOf(IllegalArgumentException.class,
                    assertThrows(InvocationTargetException.class, () -> constructor.newInstance(arguments)).getCause());
        }
    }
}
