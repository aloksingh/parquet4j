package io.github.aloksingh.parquet.bench;

import io.github.aloksingh.parquet.model.ColumnBatch;
import io.github.aloksingh.parquet.model.ColumnValues;

/**
 * Columnar guard feeding: walks a decoded physical leaf's level-event stream and folds every
 * event (value or null slot) into the leaf accumulator, exactly matching the stream produced by
 * the row API and by dataset generation.
 */
final class Events {

  private Events() {
  }

  /** Boxed column-list access ({@link ColumnValues}): one fold per level event. */
  static void feedColumn(BenchHash.LeafAccumulator guard, int leaf, ColumnValues values) {
    switch (values.getType()) {
      case INT32 -> {
        for (Object v : values.decodeAsInt32()) guard.event(leaf, v);
      }
      case INT64 -> {
        for (Object v : values.decodeAsInt64()) guard.event(leaf, v);
      }
      case FLOAT -> {
        for (Object v : values.decodeAsFloat()) guard.event(leaf, v);
      }
      case DOUBLE -> {
        for (Object v : values.decodeAsDouble()) guard.event(leaf, v);
      }
      case BOOLEAN -> {
        for (Object v : values.decodeAsBoolean()) guard.event(leaf, v);
      }
      default -> {
        for (Object v : values.decodeAsRawBytes()) guard.event(leaf, v);
      }
    }
  }

  /** Columnar batch access ({@link ColumnBatch}): one fold per batch slot. */
  static void feedBatch(BenchHash.LeafAccumulator guard, int leaf, ColumnBatch batch) {
    for (int i = 0; i < batch.size(); i++) {
      guard.event(leaf, batch.isNull(i) ? null : batch.getObject(i));
    }
  }
}
