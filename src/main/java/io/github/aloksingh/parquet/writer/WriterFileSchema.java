package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.ColumnDescriptor;
import io.github.aloksingh.parquet.model.CompressionCodec;
import io.github.aloksingh.parquet.model.LogicalColumnDescriptor;
import io.github.aloksingh.parquet.model.MapMetadata;
import io.github.aloksingh.parquet.model.SchemaDescriptor;
import io.github.aloksingh.parquet.model.Type;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;

/**
 * Footer schema serialization: the thrift SchemaElement tree and the thrift
 * type/codec enums. Kept apart from the file lifecycle so the on-disk schema
 * shape is testable without opening an output stream.
 */
public final class WriterFileSchema {

  private WriterFileSchema() {
  }

  /**
   * Convert internal Type to Parquet format Type.
   *
   * @param type Internal type enum
   * @return Parquet format type enum
   */
  public static org.apache.parquet.format.Type convertType(Type type) {
    return switch (type) {
      case BOOLEAN -> org.apache.parquet.format.Type.BOOLEAN;
      case INT32 -> org.apache.parquet.format.Type.INT32;
      case INT64 -> org.apache.parquet.format.Type.INT64;
      case INT96 -> org.apache.parquet.format.Type.INT96;
      case FLOAT -> org.apache.parquet.format.Type.FLOAT;
      case DOUBLE -> org.apache.parquet.format.Type.DOUBLE;
      case BYTE_ARRAY -> org.apache.parquet.format.Type.BYTE_ARRAY;
      case FIXED_LEN_BYTE_ARRAY -> org.apache.parquet.format.Type.FIXED_LEN_BYTE_ARRAY;
    };
  }

  /**
   * Convert internal CompressionCodec to Parquet format CompressionCodec.
   *
   * @param codec Internal compression codec enum
   * @return Parquet format compression codec enum
   */
  public static org.apache.parquet.format.CompressionCodec convertCompressionCodec(
      CompressionCodec codec) {
    return switch (codec) {
      case UNCOMPRESSED -> org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
      case SNAPPY -> org.apache.parquet.format.CompressionCodec.SNAPPY;
      case GZIP -> org.apache.parquet.format.CompressionCodec.GZIP;
      case LZO -> org.apache.parquet.format.CompressionCodec.LZO;
      case BROTLI -> org.apache.parquet.format.CompressionCodec.BROTLI;
      case LZ4 -> org.apache.parquet.format.CompressionCodec.LZ4;
      case ZSTD -> org.apache.parquet.format.CompressionCodec.ZSTD;
      case LZ4_RAW -> org.apache.parquet.format.CompressionCodec.LZ4_RAW;
    };
  }

  /**
   * Build the file schema from the schema descriptor.
   *
   * @return Root schema element representing the file schema
   */
  public static SchemaElement buildFileSchema(SchemaDescriptor schema) {
    SchemaElement root = new SchemaElement();
    root.setName(schema.name());

    // Count children: logical columns if present, otherwise physical columns
    int numChildren = schema.hasLogicalColumns()
        ? schema.getNumLogicalColumns()
        : schema.getNumColumns();
    root.setNum_children(numChildren);

    // Root has no type
    return root;
  }

  /**
   * Build schema elements for columns, supporting hierarchical MAP structures.
   *
   * @return List of schema elements representing all columns
   */
  public static List<SchemaElement> buildColumnSchemas(SchemaDescriptor schema) {
    List<SchemaElement> elements = new ArrayList<>();

    if (schema.hasLogicalColumns()) {
      // Build hierarchical schema for logical columns
      for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
        LogicalColumnDescriptor logicalCol = schema.getLogicalColumn(i);

        if (logicalCol.isPrimitive()) {
          // Add primitive column
          elements.add(buildPrimitiveSchemaElement(logicalCol.getPhysicalDescriptor()));
        } else if (logicalCol.isMap()) {
          // Add MAP group with nested key_value group
          elements.addAll(buildMapSchemaElements(logicalCol));
        }
      }
    } else {
      // Legacy: build flat schema from physical columns
      for (int i = 0; i < schema.getNumColumns(); i++) {
        elements.add(buildPrimitiveSchemaElement(schema.getColumn(i)));
      }
    }

    return elements;
  }

  /**
   * Build a schema element for a primitive column.
   *
   * @param col Column descriptor for the primitive column
   * @return Schema element representing the column
   */
  private static SchemaElement buildPrimitiveSchemaElement(ColumnDescriptor col) {
    SchemaElement element = new SchemaElement();

    // Use the last part of the path as the name
    String[] path = col.path();
    element.setName(path[path.length - 1]);
    element.setType(convertType(col.physicalType()));

    // Set repetition type
    if (col.maxRepetitionLevel() > 0) {
      element.setRepetition_type(FieldRepetitionType.REPEATED);
    } else if (col.maxDefinitionLevel() > 0) {
      element.setRepetition_type(FieldRepetitionType.OPTIONAL);
    } else {
      element.setRepetition_type(FieldRepetitionType.REQUIRED);
    }

    if (col.physicalType() == Type.FIXED_LEN_BYTE_ARRAY) {
      element.setType_length(col.typeLength());
    }

    // Emit the descriptor's logical annotation (LogicalType + legacy ConvertedType).
    WriterSchema.emitAnnotations(element, col);

    return element;
  }

  /**
   * Build schema elements for a MAP column.
   * Returns 4 elements: map group, key_value group, key, and value.
   *
   * @param logicalCol Logical column descriptor for the MAP column
   * @return List of schema elements (map group, key_value group, key element, value element)
   */
  private static List<SchemaElement> buildMapSchemaElements(LogicalColumnDescriptor logicalCol) {
    List<SchemaElement> elements = new ArrayList<>();
    MapMetadata mapMeta = logicalCol.getMapMetadata();

    // 1. Map group (optional group <name> (MAP))
    SchemaElement mapGroup = new SchemaElement();
    mapGroup.setName(logicalCol.getName());
    mapGroup.setRepetition_type(
        mapMeta.keyDescriptor().maxDefinitionLevel() > 1
            ? FieldRepetitionType.OPTIONAL
            : FieldRepetitionType.REQUIRED
    );
    // Emit the MAP annotation (modern LogicalType + legacy ConvertedType).
    WriterSchema.emitMapAnnotations(mapGroup);
    mapGroup.setNum_children(1);  // Contains key_value group
    elements.add(mapGroup);

    // 2. key_value group (repeated group key_value)
    SchemaElement keyValueGroup = new SchemaElement();
    keyValueGroup.setName("key_value");
    keyValueGroup.setRepetition_type(FieldRepetitionType.REPEATED);
    keyValueGroup.setNum_children(2);  // Contains key and value
    elements.add(keyValueGroup);

    // 3. Key element (required <type> key)
    SchemaElement keyElement = new SchemaElement();
    keyElement.setName("key");
    keyElement.setType(convertType(mapMeta.keyType()));
    keyElement.setRepetition_type(FieldRepetitionType.REQUIRED);
    if (mapMeta.keyType() == Type.FIXED_LEN_BYTE_ARRAY) {
      keyElement.setType_length(mapMeta.keyDescriptor().typeLength());
    }
    // Emit the key's own logical annotation (no blanket UTF8 for unannotated binaries).
    WriterSchema.emitAnnotations(keyElement,
        mapMeta.keyDescriptor());
    elements.add(keyElement);

    // 4. Value element (optional/required <type> value)
    SchemaElement valueElement = new SchemaElement();
    valueElement.setName("value");
    valueElement.setType(convertType(mapMeta.valueType()));
    if (mapMeta.valueType() == Type.FIXED_LEN_BYTE_ARRAY) {
      valueElement.setType_length(mapMeta.valueDescriptor().typeLength());
    }

    // Value is optional if maxDefLevel indicates it can be null
    int valueMaxDef = mapMeta.valueDescriptor().maxDefinitionLevel();
    int keyMaxDef = mapMeta.keyDescriptor().maxDefinitionLevel();
    boolean valueOptional = valueMaxDef > keyMaxDef;

    valueElement.setRepetition_type(
        valueOptional ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED
    );
    // Emit the value's own logical annotation (no blanket UTF8 for unannotated binaries).
    WriterSchema.emitAnnotations(valueElement,
        mapMeta.valueDescriptor());
    elements.add(valueElement);

    return elements;
  }

}
