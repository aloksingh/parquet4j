package io.github.aloksingh.parquet.model;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * ColumnBatch materialization from decoded pages: validity bitmap, dense
 * primitive arrays, shared binary offsets+payload, or preserved dictionary
 * indexes. Batches copy everything they expose (copy-on-construct) and never
 * alias page buffers.
 */
final class ColumnBatchAssembler {

  private ColumnBatchAssembler() {
  }

  static boolean isBinary(Type type) {
    return type == Type.BYTE_ARRAY || type == Type.FIXED_LEN_BYTE_ARRAY || type == Type.INT96;
  }

  /** Returns the chunk's single dictionary when every data page shares it, else null. */
  private static Object[] sharedDictionary(List<DecodedPage> pages) {
    Object[] dictionary = null;
    boolean any = false;
    for (DecodedPage page : pages) {
      if (page.dictionaryIndices() == null) {
        return null;
      }
      if (!any) {
        dictionary = page.dictionary();
        any = true;
      } else if (dictionary != page.dictionary()) {
        return null;
      }
    }
    return any ? dictionary : null;
  }

  static ColumnBatch buildBatch(ColumnDescriptor descriptor, Type type, List<DecodedPage> pages) {
    int maxDefinition = descriptor.maxDefinitionLevel();
    int total = 0;
    for (DecodedPage page : pages) {
      total += page.numValues();
    }
    BitSet presence = null;
    boolean allPresent = true;
    for (DecodedPage page : pages) {
      for (int event = 0; event < page.numValues(); event++) {
        if (page.definitionLevel(event) != maxDefinition) {
          allPresent = false;
          break;
        }
      }
      if (!allPresent) {
        break;
      }
    }
    if (!allPresent) {
      presence = new BitSet(total);
      int pos = 0;
      for (DecodedPage page : pages) {
        for (int event = 0; event < page.numValues(); event++) {
          if (page.definitionLevel(event) == maxDefinition) {
            presence.set(pos);
          }
          pos++;
        }
      }
    }
    Object[] dictionary = sharedDictionary(pages);
    if (dictionary != null) {
      int[] indexes = new int[total];
      Arrays.fill(indexes, -1);
      int pos = 0;
      for (DecodedPage page : pages) {
        int[] pageIndexes = page.dictionaryIndices();
        int physical = 0;
        for (int event = 0; event < page.numValues(); event++) {
          if (page.definitionLevel(event) == maxDefinition) {
            indexes[pos] = pageIndexes[physical++];
          }
          pos++;
        }
      }
      return ColumnBatch.dictionary(descriptor, indexes, dictionary, presence);
    }
    if (isBinary(type)) {
      BinaryValues merged = mergeBinary(pages, total, maxDefinition);
      return ColumnBatch.binary(descriptor, merged.offsets(), merged.data(), presence);
    }
    return ColumnBatch.of(descriptor, assembleDense(descriptor, type, pages, total, presence), presence);
  }

  /**
   * Copies physical values into one dense primitive array, one slot per level
   * event. Absent events keep the array default and are marked in {@code presence}.
   * Dictionary pages are read through their typed dictionary entries; plain pages
   * bulk-copy when the chunk has no nulls at all.
   */
  static Object assembleDense(ColumnDescriptor descriptor, Type type, List<DecodedPage> pages, int total, BitSet presence) {
    int maxDefinition = descriptor.maxDefinitionLevel();
    switch (type) {
      case INT32: {
        int[] out = new int[total];
        int pos = 0;
        for (DecodedPage page : pages) {
          int[] pageIndexes = page.dictionaryIndices();
          int[] values = pageIndexes == null ? (int[]) page.values() : null;
          if (pageIndexes == null && presence == null) {
            System.arraycopy(values, 0, out, pos, page.numValues());
            pos += page.numValues();
            continue;
          }
          Object[] dictionary = page.dictionary();
          int physical = 0;
          for (int event = 0; event < page.numValues(); event++) {
            if (page.definitionLevel(event) == maxDefinition) {
              out[pos] = pageIndexes != null
                  ? (Integer) dictionary[pageIndexes[physical++]] : values[physical++];
            }
            pos++;
          }
        }
        return out;
      }
      case INT64: {
        long[] out = new long[total];
        int pos = 0;
        for (DecodedPage page : pages) {
          int[] pageIndexes = page.dictionaryIndices();
          long[] values = pageIndexes == null ? (long[]) page.values() : null;
          if (pageIndexes == null && presence == null) {
            System.arraycopy(values, 0, out, pos, page.numValues());
            pos += page.numValues();
            continue;
          }
          Object[] dictionary = page.dictionary();
          int physical = 0;
          for (int event = 0; event < page.numValues(); event++) {
            if (page.definitionLevel(event) == maxDefinition) {
              out[pos] = pageIndexes != null
                  ? (Long) dictionary[pageIndexes[physical++]] : values[physical++];
            }
            pos++;
          }
        }
        return out;
      }
      case FLOAT: {
        float[] out = new float[total];
        int pos = 0;
        for (DecodedPage page : pages) {
          int[] pageIndexes = page.dictionaryIndices();
          float[] values = pageIndexes == null ? (float[]) page.values() : null;
          if (pageIndexes == null && presence == null) {
            System.arraycopy(values, 0, out, pos, page.numValues());
            pos += page.numValues();
            continue;
          }
          Object[] dictionary = page.dictionary();
          int physical = 0;
          for (int event = 0; event < page.numValues(); event++) {
            if (page.definitionLevel(event) == maxDefinition) {
              out[pos] = pageIndexes != null
                  ? (Float) dictionary[pageIndexes[physical++]] : values[physical++];
            }
            pos++;
          }
        }
        return out;
      }
      case DOUBLE: {
        double[] out = new double[total];
        int pos = 0;
        for (DecodedPage page : pages) {
          int[] pageIndexes = page.dictionaryIndices();
          double[] values = pageIndexes == null ? (double[]) page.values() : null;
          if (pageIndexes == null && presence == null) {
            System.arraycopy(values, 0, out, pos, page.numValues());
            pos += page.numValues();
            continue;
          }
          Object[] dictionary = page.dictionary();
          int physical = 0;
          for (int event = 0; event < page.numValues(); event++) {
            if (page.definitionLevel(event) == maxDefinition) {
              out[pos] = pageIndexes != null
                  ? (Double) dictionary[pageIndexes[physical++]] : values[physical++];
            }
            pos++;
          }
        }
        return out;
      }
      case BOOLEAN: {
        boolean[] out = new boolean[total];
        int pos = 0;
        for (DecodedPage page : pages) {
          int[] pageIndexes = page.dictionaryIndices();
          boolean[] values = pageIndexes == null ? (boolean[]) page.values() : null;
          if (pageIndexes == null && presence == null) {
            System.arraycopy(values, 0, out, pos, page.numValues());
            pos += page.numValues();
            continue;
          }
          Object[] dictionary = page.dictionary();
          int physical = 0;
          for (int event = 0; event < page.numValues(); event++) {
            if (page.definitionLevel(event) == maxDefinition) {
              out[pos] = pageIndexes != null
                  ? (Boolean) dictionary[pageIndexes[physical++]] : values[physical++];
            }
            pos++;
          }
        }
        return out;
      }
      default:
        throw new ParquetException("Binary columns use offsets+payload storage: " + type);
    }
  }

  /**
   * Merges per-page binary storage into one offsets+payload pair, one slot per
   * level event; absent events get zero-length slots. Dictionary pages contribute
   * their byte[] entries, plain pages bulk-copy their {@link BinaryValues} slices.
   */
  static BinaryValues mergeBinary(List<DecodedPage> pages, int total, int maxDefinition) {
    int[] offsets = new int[total + 1];
    int pos = 0;
    for (DecodedPage page : pages) {
      int[] pageIndexes = page.dictionaryIndices();
      BinaryValues values = pageIndexes == null ? (BinaryValues) page.values() : null;
      Object[] dictionary = page.dictionary();
      int physical = 0;
      for (int event = 0; event < page.numValues(); event++) {
        int length = 0;
        if (page.definitionLevel(event) == maxDefinition) {
          length = pageIndexes != null
              ? ((byte[]) dictionary[pageIndexes[physical]]).length : values.byteBuffer(physical).remaining();
          physical++;
        }
        offsets[pos + 1] = offsets[pos] + length;
        pos++;
      }
    }
    ByteBuffer payload = ByteBuffer.allocate(offsets[total]);
    pos = 0;
    for (DecodedPage page : pages) {
      int[] pageIndexes = page.dictionaryIndices();
      BinaryValues values = pageIndexes == null ? (BinaryValues) page.values() : null;
      Object[] dictionary = page.dictionary();
      int physical = 0;
      for (int event = 0; event < page.numValues(); event++) {
        if (page.definitionLevel(event) == maxDefinition) {
          if (pageIndexes != null) {
            byte[] entry = (byte[]) dictionary[pageIndexes[physical++]];
            payload.put(offsets[pos], entry, 0, entry.length);
          } else {
            ByteBuffer view = values.byteBuffer(physical++);
            payload.put(offsets[pos], view, 0, view.remaining());
          }
        }
        pos++;
      }
    }
    return new BinaryValues(offsets, payload);
  }

}
