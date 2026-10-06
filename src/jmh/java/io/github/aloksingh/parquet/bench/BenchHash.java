package io.github.aloksingh.parquet.bench;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Deterministic value checksum used by every benchmark correctness guard.
 *
 * <p>Design: every physical leaf accumulates one fragment per level event (scalars have exactly
 * one event per row; repeated leaves such as MAP key/value have one event per entry plus one
 * event for null/empty containers). A leaf accumulator rolls as {@code h = h * PRIME + fragment}
 * in event order, and the final checksum folds the per-leaf accumulators in schema leaf order.
 * Both the row API and the columnar APIs therefore produce identical checksums regardless of
 * row-major vs column-major traversal, because each leaf's stream is hashed independently and
 * the fold order is fixed by schema order.
 *
 * <p>{@link #valueHash(Object)} is deliberately insensitive to representation: a {@link String}
 * and its UTF-8 {@code byte[]} hash identically, so annotation-aware row values and raw physical
 * column values agree. Null values and null/empty container events all contribute
 * {@link #NULL_FRAGMENT}.
 */
final class BenchHash {

  static final long NULL_FRAGMENT = 0x9E3779B97F4A7C15L;
  static final long PRIME = 0x100000001B3L;
  static final long FOLD_SEED = 0xCBF29CE484222325L;

  private BenchHash() {
  }

  /** Fragment for one value: typed, representation-independent, stable across JVM runs. */
  static long valueHash(Object value) {
    if (value == null) {
      return NULL_FRAGMENT;
    }
    if (value instanceof String s) {
      return fnv(s.getBytes(StandardCharsets.UTF_8));
    }
    if (value instanceof byte[] b) {
      return fnv(b);
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer copy = buffer.duplicate();
      byte[] bytes = new byte[copy.remaining()];
      copy.get(bytes);
      return fnv(bytes);
    }
    if (value instanceof Integer i) {
      return mix64(i);
    }
    if (value instanceof Long l) {
      return mix64(l);
    }
    if (value instanceof Boolean b) {
      return b ? 0x1234ABCD5678EF90L : 0x0FEDCBA987654321L;
    }
    if (value instanceof Double d) {
      return mix64(Double.doubleToLongBits(d));
    }
    if (value instanceof Float f) {
      return mix64(Float.floatToIntBits(f));
    }
    throw new IllegalArgumentException(
        "Unsupported value type for checksum: " + value.getClass().getName());
  }

  private static long fnv(byte[] bytes) {
    long h = FOLD_SEED;
    for (byte b : bytes) {
      h ^= (b & 0xFFL);
      h *= PRIME;
    }
    return h == FOLD_SEED ? PRIME : h; // never let an empty payload collide with the fold seed
  }

  private static long mix64(long value) {
    long h = value * 0x9E3779B97F4A7C15L;
    h ^= (h >>> 31);
    h *= 0xBF58476D1CE4E5B9L;
    h ^= (h >>> 27);
    return h;
  }

  /** Per-leaf rolling accumulators; fixed capacity covers the widest schema (40+ leaves). */
  static final class LeafAccumulator {
    final long[] hashes;

    LeafAccumulator(int leaves) {
      hashes = new long[Math.max(leaves, 1)];
    }

    /** One level event on one physical leaf (scalar row value or repeated entry slot). */
    void event(int leaf, Object value) {
      hashes[leaf] = hashes[leaf] * PRIME + valueHash(value);
    }

    /** Fold the first {@code leaves} leaf accumulators in schema order. */
    long fold(int leaves) {
      long f = FOLD_SEED;
      for (int i = 0; i < leaves; i++) {
        f = f * PRIME + hashes[i];
      }
      return f;
    }

    /** Fold the given leaf accumulators in ascending leaf order (projected scans). */
    long fold(int[] leafIndexes) {
      long f = FOLD_SEED;
      for (int leaf : leafIndexes) {
        f = f * PRIME + hashes[leaf];
      }
      return f;
    }
  }
}
