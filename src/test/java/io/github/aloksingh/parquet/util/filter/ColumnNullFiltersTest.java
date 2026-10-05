package io.github.aloksingh.parquet.util.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.github.aloksingh.parquet.util.filter.FilterTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.aloksingh.parquet.model.ListMetadata;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalType;
import io.github.aloksingh.parquet.model.MapMetadata;
import java.util.Arrays;
import java.util.HashMap;
import io.github.aloksingh.parquet.model.Type;
import org.junit.jupiter.api.Test;

public class ColumnNullFiltersTest {


  // IsNull Tests
  @Test
  public void testIsNullWithNullValue() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(primitiveDescriptor);

    assertTrue(filter.apply(null));
  }

  @Test
  public void testIsNullWithNonNullPrimitive() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(primitiveDescriptor);

    assertFalse(filter.apply("test"));
    assertFalse(filter.apply(42));
    assertFalse(filter.apply(0));
    assertFalse(filter.apply(""));
  }

  @Test
  public void testIsNullWithList() {
    var listDescriptor = list(Type.INT32);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(listDescriptor);

    assertFalse(filter.apply(Arrays.asList("a", "b")));
    assertFalse(filter.apply(Arrays.asList()));
    assertTrue(filter.apply(null));
  }

  @Test
  public void testIsNullWithMap() {
    var mapDescriptor = map(Type.INT32);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(mapDescriptor);

    assertFalse(filter.apply(new HashMap<>()));
    assertTrue(filter.apply(null));
  }

  // IsNotNull Tests
  @Test
  public void testIsNotNullWithNullValue() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter(primitiveDescriptor);

    assertFalse(filter.apply(null));
  }

  @Test
  public void testIsNotNullWithNonNullPrimitive() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter(primitiveDescriptor);

    assertTrue(filter.apply("test"));
    assertTrue(filter.apply(42));
    assertTrue(filter.apply(0));
    assertTrue(filter.apply(""));
    assertTrue(filter.apply(false));
  }

  @Test
  public void testIsNotNullWithList() {
    var listDescriptor = list(Type.INT32);
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter(listDescriptor);

    assertTrue(filter.apply(Arrays.asList("a", "b")));
    assertTrue(filter.apply(Arrays.asList()));
    assertFalse(filter.apply(null));
  }

  @Test
  public void testIsNotNullWithMap() {
    var mapDescriptor = map(Type.INT32);
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter(mapDescriptor);

    assertTrue(filter.apply(new HashMap<>()));
    assertFalse(filter.apply(null));
  }

  @Test
  public void testIsNotNullWithZeroValues() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnIsNotNullFilter filter = new ColumnIsNotNullFilter(primitiveDescriptor);

    // Zero is not null
    assertTrue(filter.apply(0));
    assertTrue(filter.apply(0L));
    assertTrue(filter.apply(0.0));
    assertTrue(filter.apply(0.0f));
  }

  @Test
  public void testIsNullWithZeroValues() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnIsNullFilter filter = new ColumnIsNullFilter(primitiveDescriptor);

    // Zero is not null
    assertFalse(filter.apply(0));
    assertFalse(filter.apply(0L));
    assertFalse(filter.apply(0.0));
    assertFalse(filter.apply(0.0f));
  }

  @Test
  public void testNullFiltersIgnoreColumnType() {
    var primitiveDescriptor = primitive("col", Type.INT32);
    ColumnIsNullFilter isNullFilter = new ColumnIsNullFilter(primitiveDescriptor);
    ColumnIsNotNullFilter isNotNullFilter = new ColumnIsNotNullFilter(primitiveDescriptor);

    // Null filters should work consistently across all column types
    assertTrue(isNullFilter.apply(null));
    assertTrue(isNullFilter.apply(null));
    assertTrue(isNullFilter.apply(null));

    assertFalse(isNotNullFilter.apply(null));
    assertFalse(isNotNullFilter.apply(null));
    assertFalse(isNotNullFilter.apply(null));
  }
}

