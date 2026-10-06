package io.github.aloksingh.parquet.writer;

import io.github.aloksingh.parquet.model.MapMetadata;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Row staging for one MAP column: flattens a row's map into the paired key/value
 * leaf buffers. MAP keys are required by the Parquet specification; entries sit at
 * the key leaf's maximum definition level and the map container one level below.
 * Duplicate encoded keys are rejected because the format requires unique keys per
 * row and a duplicate would silently lose an entry.
 */
public final class MapRowStaging {

  private MapRowStaging() {
  }

  /**
   * Appends one row's map entries to the staged key and value buffers.
   *
   * @param data     the row's map value, or null for a null map
   * @param metadata the MAP column's key/value leaf descriptors
   * @param keys     staged key leaf buffer
   * @param values   staged value leaf buffer
   * @throws IllegalArgumentException if the map is null but required, the value is
   *                                  not a map, or a row repeats an encoded key
   */
  public static void appendRow(Object data, MapMetadata metadata, WriterColumnBuffer keys,
      WriterColumnBuffer values) {
    int keyDefinition = metadata.keyDescriptor().maxDefinitionLevel();
    int valueDefinition = metadata.valueDescriptor().maxDefinitionLevel();
    if (data == null) {
      if (keyDefinition == 1) throw new IllegalArgumentException("Required MAP must not be null");
      keys.add(null, 0, 0, true);
      values.add(null, 0, 0, true);
      return;
    }
    if (!(data instanceof Map<?, ?> map)) {
      throw new IllegalArgumentException("Expected a MAP value, got " + data.getClass().getName());
    }
    var entries = map.entrySet().iterator();
    if (!entries.hasNext()) {
      keys.add(null, keyDefinition - 1, 0, true);
      values.add(null, keyDefinition - 1, 0, true);
      return;
    }
    int repetition = 0;
    Set<Object> encodedKeys = new HashSet<>();
    do {
      Map.Entry<?, ?> entry = entries.next();
      Object key = entry.getKey();
      Object value = entry.getValue();
      Object normalizedKey = keys.add(key, keyDefinition, repetition, false);
      Object equalityKey = normalizedKey instanceof byte[] bytes
          ? ByteBuffer.wrap(bytes).asReadOnlyBuffer() : normalizedKey;
      if (!encodedKeys.add(equalityKey)) throw new IllegalArgumentException("Duplicate encoded MAP key");
      values.add(value, value == null ? keyDefinition : valueDefinition, repetition,
          valueDefinition > keyDefinition);
      repetition = 1;
    } while (entries.hasNext());
  }
}
