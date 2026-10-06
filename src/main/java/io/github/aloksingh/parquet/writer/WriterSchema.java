package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.MapMetadata;
import io.github.aloksingh.parquet.model.RowColumnGroup;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.SchemaElement;

/**
 * Internal checks for the deliberately limited, fully writable schema subset.
 */
public final class WriterSchema {
    private WriterSchema() {
    }

    public static void validate(SchemaDescriptor schema) {
        require(schema != null, "Schema must not be null");
        require(schema.name() != null && !schema.name().isEmpty(), "Schema name must not be empty");
        require(schema.columns() != null && !schema.columns().isEmpty(), "Schema must have physical columns");
        require(schema.logicalColumns() != null, "Logical columns must not be null");
        Set<String> names = new HashSet<>();
        if (!schema.hasLogicalColumns()) {
            for (ColumnDescriptor column : schema.columns()) {
                primitive(column);
                require(names.add(column.path()[0]), "Duplicate column name: " + column.path()[0]);
            }
            return;
        }
        int physicalIndex = 0;
        for (LogicalColumnDescriptor logical : schema.logicalColumns()) {
            require(logical != null && logical.getName() != null && !logical.getName().isEmpty(),
                    "Logical column name must not be empty");
            require(names.add(logical.getName()), "Duplicate column name: " + logical.getName());
            if (logical.isPrimitive()) {
                ColumnDescriptor column = logical.getPhysicalDescriptor();
                primitive(column);
                require(logical.getPhysicalType() == column.physicalType(),
                        "Logical/physical type mismatch: " + logical.getName());
                require(logical.getName().equals(column.path()[0]),
                        "Logical/physical name mismatch: " + logical.getName());
                matchPhysical(schema.columns(), physicalIndex++, column);
            } else if (logical.isMap()) {
                MapMetadata map = logical.getMapMetadata();
                require(map != null, "MAP metadata must not be null");
                ColumnDescriptor key = map.keyDescriptor();
                ColumnDescriptor value = map.valueDescriptor();
                leaf(key);
                leaf(value);
                require(map.keyType() == key.physicalType() && map.valueType() == value.physicalType(),
                        "MAP physical type mismatch: " + logical.getName());
                require(Arrays.equals(key.path(), new String[]{logical.getName(), "key_value", "key"})
                                && Arrays.equals(value.path(), new String[]{logical.getName(), "key_value", "value"}),
                        "Only canonical, top-level MAP paths are writable: " + logical.getName());
                require(key.maxRepetitionLevel() == 1 && value.maxRepetitionLevel() == 1,
                        "MAP repetition level must be one");
                require(key.maxDefinitionLevel() == 1 || key.maxDefinitionLevel() == 2,
                        "Only required or optional top-level MAPs are writable");
                require(value.maxDefinitionLevel() == key.maxDefinitionLevel()
                                || value.maxDefinitionLevel() == key.maxDefinitionLevel() + 1,
                        "MAP values must be required or optional primitive leaves");
                require(map.keyColumnIndex() == physicalIndex && map.valueColumnIndex() == physicalIndex + 1,
                        "MAP physical indexes do not match schema order");
                matchPhysical(schema.columns(), physicalIndex++, key);
                matchPhysical(schema.columns(), physicalIndex++, value);
            } else {
                throw new IllegalArgumentException("Writing " + logical.getLogicalType() + " is not supported");
            }
        }
        require(physicalIndex == schema.getNumColumns(), "Unmapped physical columns in writer schema");
    }

    public static void validateRow(SchemaDescriptor expected, RowColumnGroup row) {
        require(row != null, "Row must not be null");
        SchemaDescriptor actual = row.getSchema();
        validate(actual);
        require(expected.name().equals(actual.name())
                        && expected.getNumColumns() == actual.getNumColumns()
                        && expected.getNumLogicalColumns() == actual.getNumLogicalColumns(),
                "Row schema does not match writer schema");
        for (int i = 0; i < expected.getNumColumns(); i++) {
            require(sameColumn(expected.getColumn(i), actual.getColumn(i)),
                    "Row physical schema mismatch at index " + i);
        }
        for (int i = 0; i < expected.getNumLogicalColumns(); i++) {
            LogicalColumnDescriptor first = expected.getLogicalColumn(i);
            LogicalColumnDescriptor second = actual.getLogicalColumn(i);
            require(first.getName().equals(second.getName())
                            && first.getLogicalType() == second.getLogicalType(),
                    "Row logical schema mismatch at index " + i);
        }
        int count = expected.hasLogicalColumns() ? expected.getNumLogicalColumns() : expected.getNumColumns();
        require(row.getColumnCount() == count, "Row must have exactly " + count + " logical values");
    }

    private static void primitive(ColumnDescriptor column) {
        leaf(column);
        require(column.path().length == 1 && column.maxRepetitionLevel() == 0
                        && (column.maxDefinitionLevel() == 0 || column.maxDefinitionLevel() == 1),
                "Only required/optional, non-repeated, top-level primitives are writable");
    }

    private static void leaf(ColumnDescriptor column) {
        require(column != null && column.physicalType() != null, "Column type must not be null");
        require(column.path() != null && column.path().length > 0, "Column path must not be empty");
        for (String component : column.path()) {
            require(component != null && !component.isEmpty(), "Column path component must not be empty");
        }
        require(column.physicalType() != Type.INT96, "INT96 writing is not supported");
        require(column.maxDefinitionLevel() >= 0 && column.maxRepetitionLevel() >= 0,
                "Levels must not be negative");
        require(column.physicalType() == Type.FIXED_LEN_BYTE_ARRAY ? column.typeLength() > 0
                        : column.typeLength() == 0,
                "FIXED_LEN_BYTE_ARRAY requires a positive size; other types must have size zero");
    }

    private static void matchPhysical(List<ColumnDescriptor> columns, int index, ColumnDescriptor expected) {
        require(index < columns.size() && sameColumn(columns.get(index), expected),
                "Logical and physical columns do not match at index " + index);
    }

    public static boolean sameColumn(ColumnDescriptor first, ColumnDescriptor second) {
        return first != null && second != null && first.physicalType() == second.physicalType()
                && Arrays.equals(first.path(), second.path())
                && first.maxDefinitionLevel() == second.maxDefinitionLevel()
                && first.maxRepetitionLevel() == second.maxRepetitionLevel()
                && first.typeLength() == second.typeLength();
    }

    private static void require(boolean valid, String message) {
        if (!valid) throw new IllegalArgumentException(message);
    }

    // ------------------------------------------------------------------ annotation emission

    /**
     * Emits a leaf's logical annotation (modern LogicalType plus legacy ConvertedType with
     * precision/scale parameters) into a footer schema element. Unannotated leaves emit no
     * annotation fields at all.
     */
    public static void emitAnnotations(SchemaElement element, ColumnDescriptor descriptor) {
        require(element != null && descriptor != null, "Element and descriptor must not be null");
        descriptor.annotation().applyTo(element);
    }

    /**
     * Emits the MAP group annotation (modern LogicalType plus legacy ConvertedType) into a
     * footer schema element.
     */
    public static void emitMapAnnotations(SchemaElement element) {
        require(element != null, "Element must not be null");
        element.unsetLogicalType();
        element.unsetConverted_type();
        element.setLogicalType(org.apache.parquet.format.LogicalType.MAP(
                new org.apache.parquet.format.MapType()));
        element.setConverted_type(ConvertedType.MAP);
    }

    /**
     * Emits the LIST group annotation (modern LogicalType plus legacy ConvertedType) into a
     * footer schema element.
     */
    public static void emitListAnnotations(SchemaElement element) {
        require(element != null, "Element must not be null");
        element.unsetLogicalType();
        element.unsetConverted_type();
        element.setLogicalType(org.apache.parquet.format.LogicalType.LIST(
                new org.apache.parquet.format.ListType()));
        element.setConverted_type(ConvertedType.LIST);
    }
}
