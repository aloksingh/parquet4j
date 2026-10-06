package io.github.aloksingh.parquet.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Materializing adapters over {@link ColumnPageDecoder}. Physical decoding has one
 * path for V1 and V2: primitive arrays or shared binary storage, level events, and
 * retained dictionary indexes. Row adapters box/copy only on materialization.
 *
 * <p>Primitive columnar results are available through {@link #toBatch()} and
 * {@link #toPageBatches()} (one owning {@link ColumnBatch} per data page); required
 * nonrepeated chunks additionally have an unboxed route, {@link #decodeRequiredUnboxed()},
 * which the boxed list adapters reuse so both views decode through the same cached pages.
 *
 * <p>Instances are not thread-safe. Borrowed page buffers and exposed decoded
 * arrays/dictionaries must not be mutated while the cached pages are in use.
 */
public class ColumnValues {
  private final Type type;
  private final List<Page> pages;
  private final ColumnDescriptor columnDescriptor;
  private final LogicalColumnDescriptor logicalColumnDescriptor;
  private List<DecodedPage> decodedPages;

  public ColumnValues(Type type, List<Page> pages,
                      ColumnDescriptor columnDescriptor,
                      LogicalColumnDescriptor logicalColumnDescriptor) {
    if (columnDescriptor == null || type != columnDescriptor.physicalType()) {
      throw new ParquetException("Physical column type does not match its descriptor: " + type);
    }
    this.type = type;
    this.pages = pages;
    this.columnDescriptor = columnDescriptor;
    this.logicalColumnDescriptor = logicalColumnDescriptor;
  }

  public Type getType() {
    return type;
  }

  public List<Page> getPages() {
    return pages;
  }

  public LogicalColumnDescriptor getLogicalColumnDescriptor() {
    return logicalColumnDescriptor;
  }

  public ColumnDescriptor getColumnDescriptor() {
    return columnDescriptor;
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> decodePrimitiveColumn(Class<T> typeClass) {
    if (Integer.class.equals(typeClass) || int.class.equals(typeClass)) {
      if (type != Type.INT32) {
        throw new ParquetException("Column type is not " + type);
      }
      return (List<T>) decodeAsInt32();
    }

    if (Long.class.equals(typeClass) || long.class.equals(typeClass)) {
      if (type != Type.INT64) {
        throw new ParquetException("Column type is not " + type);
      }
      return (List<T>) decodeAsInt64();
    }

    if (Float.class.equals(typeClass) || float.class.equals(typeClass)) {
      if (type != Type.FLOAT) {
        throw new ParquetException("Column type is not " + type);
      }
      return (List<T>) decodeAsFloat();
    }

    if (Double.class.equals(typeClass) || double.class.equals(typeClass)) {
      if (type != Type.DOUBLE) {
        throw new ParquetException("Column type is not " + type);
      }
      return (List<T>) decodeAsDouble();
    }

    if (Boolean.class.equals(typeClass) || boolean.class.equals(typeClass)) {
      if (type != Type.BOOLEAN) {
        throw new ParquetException("Column type is not " + type);
      }
      return (List<T>) decodeAsBoolean();
    }

    if (byte[].class.equals(typeClass)) {
      return (List<T>) decodeAsRawBytes();
    }

    if (String.class.equals(typeClass)) {
      if (type != Type.BYTE_ARRAY) {
        throw new ParquetException("Column type is not " + type);
      }
      return (List<T>) decodeAsString();
    }
    throw new ParquetException("Unsupported primitive:" + typeClass.getName());
  }

  public List<Integer> decodeAsInt32() {
    if (type != Type.INT32) {
      throw new ParquetException("Column type is not INT32: " + type);
    }
    return decodePhysicalColumn();
  }

  public List<Long> decodeAsInt64() {
    if (type != Type.INT64) {
      throw new ParquetException("Column type is not INT64: " + type);
    }
    return decodePhysicalColumn();
  }

  public List<Float> decodeAsFloat() {
    if (type != Type.FLOAT) {
      throw new ParquetException("Column type is not FLOAT: " + type);
    }
    return decodePhysicalColumn();
  }

  public List<Double> decodeAsDouble() {
    if (type != Type.DOUBLE) {
      throw new ParquetException("Column type is not DOUBLE: " + type);
    }
    return decodePhysicalColumn();
  }

  public List<byte[]> decodeAsByteArray() {
    if (type != Type.BYTE_ARRAY) {
      throw new ParquetException("Column type is not BYTE_ARRAY: " + type);
    }
    return decodePhysicalColumn();
  }

  public List<String> decodeAsString() {
    List<byte[]> byteArrays = decodeAsByteArray();
    List<String> strings = new ArrayList<>(byteArrays.size());
    for (byte[] bytes : byteArrays) {
      if (bytes == null) {
        strings.add(null);
      } else {
        strings.add(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
      }
    }
    return strings;
  }

  public List<Boolean> decodeAsBoolean() {
    if (type != Type.BOOLEAN) {
      throw new ParquetException("Column type is not BOOLEAN: " + type);
    }
    return decodePhysicalColumn();
  }

  /** Returns raw fixed-width bytes without logical conversion. */
  public List<byte[]> decodeAsFixedByteArray() {
    if (type != Type.FIXED_LEN_BYTE_ARRAY) {
      throw new ParquetException("Column type is not FIXED_LEN_BYTE_ARRAY: " + type);
    }
    return decodePhysicalColumn();
  }

  /** Returns exact twelve-byte INT96 values, not inferred timestamps. */
  public List<byte[]> decodeAsInt96() {
    if (type != Type.INT96) {
      throw new ParquetException("Column type is not INT96: " + type);
    }
    return decodePhysicalColumn();
  }

  /** Materializes any of the physical binary types as independent byte arrays. */
  public List<byte[]> decodeAsRawBytes() {
    return switch (type) {
      case BYTE_ARRAY -> decodeAsByteArray();
      case FIXED_LEN_BYTE_ARRAY -> decodeAsFixedByteArray();
      case INT96 -> decodeAsInt96();
      default -> throw new ParquetException("Column type is not binary: " + type);
    };
  }

  @SuppressWarnings("unchecked")
  private <T> List<T> decodePhysicalColumn() {
    if (isRequiredNonRepeated()) {
      Object dense = decodeRequiredUnboxed();
      int count = java.lang.reflect.Array.getLength(dense);
      List<T> values = new ArrayList<>(count);
      switch (type) {
        case INT32 -> {
          for (int value : (int[]) dense) values.add((T) Integer.valueOf(value));
        }
        case INT64 -> {
          for (long value : (long[]) dense) values.add((T) Long.valueOf(value));
        }
        case FLOAT -> {
          for (float value : (float[]) dense) values.add((T) Float.valueOf(value));
        }
        case DOUBLE -> {
          for (double value : (double[]) dense) values.add((T) Double.valueOf(value));
        }
        case BOOLEAN -> {
          for (boolean value : (boolean[]) dense) values.add((T) Boolean.valueOf(value));
        }
        default -> {
          for (byte[] value : (byte[][]) dense) values.add((T) value);
        }
      }
      return values;
    }
    List<T> values = new ArrayList<>();
    for (DecodedPage page : decodedPages()) {
      int physical = 0;
      for (int event = 0; event < page.numValues(); event++) {
        values.add(page.definitionLevel(event) == columnDescriptor.maxDefinitionLevel()
            ? (T) page.physicalValue(physical++) : null);
      }
    }
    return values;
  }

  /**
   * True for required nonrepeated chunks ({@code maxDefinitionLevel == 0} and
   * {@code maxRepetitionLevel == 0}): every level event carries exactly one value,
   * so no definition/repetition bookkeeping is needed while materializing.
   */
  public boolean isRequiredNonRepeated() {
    return columnDescriptor.maxDefinitionLevel() == 0 && columnDescriptor.maxRepetitionLevel() == 0;
  }

  /**
   * Unboxed decode route for required nonrepeated chunks. Returns one freshly
   * allocated primitive array owned by the caller: {@code int[]}, {@code long[]},
   * {@code float[]}, {@code double[]} or {@code boolean[]} for the physical
   * primitives and {@code byte[][]} for the binary types, with no per-value boxing.
   * Binary batches prefer the shared offsets+payload form instead
   * ({@link #toBatch()} flows through {@link BinaryValues}).
   *
   * @throws ParquetException if the column is optional or repeated
   */
  public Object decodeRequiredUnboxed() {
    if (!isRequiredNonRepeated()) {
      throw new ParquetException("Column is not required nonrepeated: " + columnDescriptor.getPathString());
    }
    List<DecodedPage> pages = decodedPages();
    int total = 0;
    for (DecodedPage page : pages) {
      total += page.numValues();
    }
    if (isBinary(type)) {
      BinaryValues merged = mergeBinary(pages, total, columnDescriptor.maxDefinitionLevel());
      byte[][] values = new byte[total][];
      for (int i = 0; i < total; i++) {
        values[i] = merged.bytesAt(i);
      }
      return values;
    }
    return assembleDense(pages, total, null);
  }

  /**
   * Materializes the whole column chunk into one owning {@link ColumnBatch}: a
   * validity bitmap plus a primitive array, shared binary offsets+payload, or
   * preserved dictionary indexes when every data page is dictionary encoded with
   * the chunk's single dictionary. Pages are decoded lazily and materialized one
   * page at a time, exactly like the boxed adapters above; the batch copies
   * everything it exposes (copy-on-construct) and never aliases page buffers, so
   * it stays valid after the reader is closed.
   *
   * <p>For repeated columns the batch holds one entry per level event, matching
   * {@link #decodeAsInt32()} and friends: events below the leaf maximum are null
   * slots, and repetition levels stay available through {@link #decodedPages()}.
   */
  public ColumnBatch toBatch() {
    return buildBatch(decodedPages());
  }

  /**
   * Materializes one owning {@link ColumnBatch} per data page, decoded page by
   * page in order. Concatenating the page batches reproduces {@link #toBatch()}
   * exactly (same values, null placement, and dictionary materialization).
   */
  public List<ColumnBatch> toPageBatches() {
    List<DecodedPage> pages = decodedPages();
    List<ColumnBatch> batches = new ArrayList<>(pages.size());
    for (DecodedPage page : pages) {
      batches.add(buildBatch(List.of(page)));
    }
    return List.copyOf(batches);
  }

  private static boolean isBinary(Type type) {
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

  private ColumnBatch buildBatch(List<DecodedPage> pages) {
    int maxDefinition = columnDescriptor.maxDefinitionLevel();
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
      return ColumnBatch.dictionary(columnDescriptor, indexes, dictionary, presence);
    }
    if (isBinary(type)) {
      BinaryValues merged = mergeBinary(pages, total, maxDefinition);
      return ColumnBatch.binary(columnDescriptor, merged.offsets(), merged.data(), presence);
    }
    return ColumnBatch.of(columnDescriptor, assembleDense(pages, total, presence), presence);
  }

  /**
   * Copies physical values into one dense primitive array, one slot per level
   * event. Absent events keep the array default and are marked in {@code presence}.
   * Dictionary pages are read through their typed dictionary entries; plain pages
   * bulk-copy when the chunk has no nulls at all.
   */
  private Object assembleDense(List<DecodedPage> pages, int total, BitSet presence) {
    int maxDefinition = columnDescriptor.maxDefinitionLevel();
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
  private BinaryValues mergeBinary(List<DecodedPage> pages, int total, int maxDefinition) {
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

  /** Cached primitive pages and one shared dictionary per column chunk. */
  public List<DecodedPage> decodedPages() {
    if (decodedPages == null) {
      ColumnPageDecoder decoder = new ColumnPageDecoder(columnDescriptor);
      List<DecodedPage> result = new ArrayList<>();
      for (Page source : pages) {
        if (source instanceof Page.DictionaryPage dictionary) {
          decoder.setDictionary(dictionary);
        } else {
          result.add(decoder.decode(source));
        }
      }
      decodedPages = List.copyOf(result);
    }
    return decodedPages;
  }

  /**
   * Materializes one standard LIST layer. The legacy convention is an optional
   * top-level LIST (definition 1), except a bare required repeated field (maximum
   * definition 1). Nested repetition levels (a deeper repeated layer) flatten into
   * the row container. Use explicit thresholds for required lists with optional
   * elements or optional ancestors; maximum levels alone cannot disambiguate them.
   */
  public <T> List<List<T>> decodeAsList(java.util.function.Function<Object, T> elementDecoder) {
    int maxDefinition = columnDescriptor.maxDefinitionLevel();
    if (columnDescriptor.maxRepetitionLevel() < 1 || maxDefinition < 1 || maxDefinition > 3) {
      throw new ParquetException("Unsupported LIST shape; use physical decoded level events");
    }
    int listDefinition = maxDefinition == 1 ? 0 : 1;
    return decodeAsList(listDefinition, listDefinition + 1, elementDecoder);
  }

  /**
   * Materializes one repeated LIST layer with structural definition thresholds.
   * {@code listDefinition} is the definition level of the container itself and
   * {@code elementDefinition} that of the repeated entry layer: definitions below
   * {@code listDefinition} produce a null container, {@code listDefinition} alone an
   * empty container, and every event from {@code elementDefinition} up appends exactly
   * one element, a value only at the leaf maximum and null otherwise. Null element
   * slots below the entry level (nested nulls carried by continuation events, including
   * the first event of a new V1 page) append a null element and keep the container
   * active instead of being dropped or rejected. The active container survives physical
   * page boundaries. Only events at the leaf maximum consume physical values.
   */
  public <T> List<List<T>> decodeAsList(int listDefinition, int elementDefinition,
                                      java.util.function.Function<Object, T> elementDecoder) {
    int maxDefinition = columnDescriptor.maxDefinitionLevel();
    if (columnDescriptor.maxRepetitionLevel() < 1) {
      throw new ParquetException("Unsupported LIST structural definition levels");
    }
    return NestedAssembler.assembleLists(decodedPages(), maxDefinition,
        listDefinition, elementDefinition, elementDecoder);
  }

  /**
   * A physical leaf cannot supply both MAP chunks. This legacy signature remains
   * source-compatible but rejects the former guessed alternating-value format.
   */
  public <K, V> List<java.util.Map<K, V>> decodeAsMap(
      java.util.function.Function<Object, K> keyDecoder,
      java.util.function.Function<Object, V> valueDecoder) {
    throw new ParquetException("MAP decoding requires separate key and value columns; use decodeMapFromKeyValueColumns");
  }

  /**
   * Materializes scalar MAP leaves by joining complete level-event streams, not
   * matching page indexes. Keys must be required; value nullability is represented
   * by the value leaf's maximum definition level. Nested repeated values need a
   * schema-aware reconstruction layer and are rejected rather than flattened.
   */
  public static <K, V> List<java.util.Map<K, V>> decodeMapFromKeyValueColumns(
      ColumnValues keyColumn,
      ColumnValues valueColumn,
      java.util.function.Function<Object, K> keyDecoder,
      java.util.function.Function<Object, V> valueDecoder) {
    return NestedAssembler.assembleScalarMaps(keyColumn, valueColumn, keyDecoder, valueDecoder,
        NestedAssembler.DuplicateKeyPolicy.LAST_WINS);
  }

}
