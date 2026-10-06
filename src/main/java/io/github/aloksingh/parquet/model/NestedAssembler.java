package io.github.aloksingh.parquet.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Shared nested-container assembly for LIST and MAP leaves.
 * <p>
 * Both public MAP paths assemble here with an explicit
 * {@link DuplicateKeyPolicy}; their other rules are intentionally different and
 * preserved by their call sites:
 * <ul>
 *   <li>{@code NestedStructureReader.readMap} zips per-row entry lists produced by
 *       list assembly, so nested MAP-of-MAP entries flatten per row (one item list
 *       per row, baseline-equivalent) and duplicate keys are rejected because a
 *       duplicate would drop flattened data;</li>
 *   <li>{@code ColumnValues.decodeMapFromKeyValueColumns} joins scalar leaves by
 *       level events, rejects nested repetition shapes, and keeps the last of a
 *       row's duplicate keys (legacy behavior).</li>
 * </ul>
 */
public final class NestedAssembler {

  /** Rules for a row whose MAP keys repeat. */
  public enum DuplicateKeyPolicy {
    /** Reject the row: a duplicate would silently drop an entry (flattening path). */
    REJECT,
    /** Keep the last entry for the key (legacy scalar path). */
    LAST_WINS
  }

  private NestedAssembler() {
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
  public static <T> List<List<T>> assembleLists(List<DecodedPage> pages, int maxDefinition,
                                                int listDefinition, int elementDefinition,
                                                Function<Object, T> elementDecoder) {
    if (listDefinition < 0 || elementDefinition != listDefinition + 1 || elementDefinition > maxDefinition) {
      throw new ParquetException("Unsupported LIST structural definition levels");
    }
    java.util.Objects.requireNonNull(elementDecoder, "elementDecoder");
    List<List<T>> result = new ArrayList<>();
    List<T> active = null;
    for (DecodedPage page : pages) {
      int physical = 0;
      for (int event = 0; event < page.numValues(); event++) {
        int definition = page.definitionLevel(event);
        int repetition = page.repetitionLevel(event);
        if (repetition == 0) {
          if (definition < listDefinition) {
            active = null;
            result.add(null);
            continue;
          }
          active = new ArrayList<>();
          result.add(active);
          if (definition < elementDefinition) {
            continue; // Present container without an entry: an empty container.
          }
        } else if (active == null) {
          throw new ParquetException("LIST continuation without an active element container");
        }
        // This slot holds an entry: only the leaf maximum carries a physical value,
        // every lower definition appends null and keeps the container active.
        active.add(definition == maxDefinition
            ? elementDecoder.apply(page.physicalValue(physical++)) : null);
      }
    }
    return result;
  }

  /**
   * Materializes scalar MAP leaves by joining complete level-event streams, not
   * matching page indexes. Keys must be required; value nullability is represented
   * by the value leaf's maximum definition level. Nested repeated values need a
   * schema-aware reconstruction layer and are rejected rather than flattened.
   */
  public static <K, V> List<Map<K, V>> assembleScalarMaps(
      ColumnValues keyColumn,
      ColumnValues valueColumn,
      Function<Object, K> keyDecoder,
      Function<Object, V> valueDecoder,
      DuplicateKeyPolicy policy) {
    java.util.Objects.requireNonNull(keyColumn, "keyColumn");
    java.util.Objects.requireNonNull(valueColumn, "valueColumn");
    java.util.Objects.requireNonNull(keyDecoder, "keyDecoder");
    java.util.Objects.requireNonNull(valueDecoder, "valueDecoder");
    java.util.Objects.requireNonNull(policy, "policy");
    int entryDefinition = keyColumn.getColumnDescriptor().maxDefinitionLevel();
    int valueDefinition = valueColumn.getColumnDescriptor().maxDefinitionLevel();
    if (keyColumn.getColumnDescriptor().maxRepetitionLevel() != 1
        || valueColumn.getColumnDescriptor().maxRepetitionLevel() != 1 || entryDefinition < 1
        || valueDefinition < entryDefinition || valueDefinition > entryDefinition + 1) {
      throw new ParquetException("Unsupported scalar MAP key/value shape");
    }
    int mapDefinition = entryDefinition - 1;
    LevelEventCursor keys = new LevelEventCursor(keyColumn.decodedPages(),
        keyColumn.getColumnDescriptor().maxDefinitionLevel());
    LevelEventCursor values = new LevelEventCursor(valueColumn.decodedPages(),
        valueColumn.getColumnDescriptor().maxDefinitionLevel());
    List<Map<K, V>> result = new ArrayList<>();
    Map<K, V> active = null;
    boolean hasEntries = false;
    while (keys.hasNext()) {
      if (!values.hasNext()) throw new ParquetException("MAP key/value event count mismatch");
      int keyDef = keys.definition();
      int valueDef = values.definition();
      int repetition = keys.repetition();
      if (repetition != values.repetition()) throw new ParquetException("MAP repetition levels do not align");
      if (keyDef < entryDefinition && valueDef != keyDef) {
        throw new ParquetException("MAP container definition levels do not align");
      }
      if (keyDef == entryDefinition && valueDef < entryDefinition) {
        throw new ParquetException("MAP entry is missing a required key or value event");
      }
      if (repetition == 0) {
        hasEntries = keyDef == entryDefinition;
        if (keyDef < mapDefinition) {
          active = null;
          result.add(null);
        } else {
          active = new LinkedHashMap<>();
          result.add(active);
        }
      } else if (active == null || !hasEntries || keyDef < entryDefinition) {
        throw new ParquetException("MAP continuation without an active entry container");
      }
      if (keyDef == entryDefinition) {
        K key = keyDecoder.apply(keys.physicalValue());
        if (key == null) throw new ParquetException("MAP keys are required");
        V value = valueDef == valueDefinition ? valueDecoder.apply(values.physicalValue()) : null;
        if (value == null && valueDefinition == entryDefinition) {
          throw new ParquetException("MAP value is required");
        }
        if (policy == DuplicateKeyPolicy.REJECT && active.containsKey(key)) {
          throw new ParquetException("MAP row " + result.size() + " repeats key " + key);
        }
        active.put(key, value);
      }
      keys.advance();
      values.advance();
    }
    if (values.hasNext()) throw new ParquetException("MAP key/value event count mismatch");
    return result;
  }

  /**
   * Zips per-row entry lists into per-row maps. Both lists must have the same row
   * count; a row is either null on both sides or non-null on both sides, and its
   * key and value lists must have the same entry count.
   */
  public static <K, V> List<Map<K, V>> zipEntryLists(List<List<K>> keyLists, List<List<V>> valueLists,
                                                     DuplicateKeyPolicy policy) {
    java.util.Objects.requireNonNull(policy, "policy");
    if (keyLists.size() != valueLists.size()) {
      throw new ParquetException("Key and value lists have different sizes: " +
          keyLists.size() + " vs " + valueLists.size());
    }
    List<Map<K, V>> result = new ArrayList<>();
    for (int i = 0; i < keyLists.size(); i++) {
      List<K> keys = keyLists.get(i);
      List<V> values = valueLists.get(i);
      if (keys == null && values == null) {
        result.add(null);
      } else if (keys == null || values == null) {
        throw new ParquetException("Key and value lists should both be null or both be non-null");
      } else if (keys.size() != values.size()) {
        throw new ParquetException("Key and value lists have different sizes at index " + i +
            ": " + keys.size() + " vs " + values.size());
      } else {
        Map<K, V> map = new LinkedHashMap<>();
        for (int j = 0; j < keys.size(); j++) {
          if (policy == DuplicateKeyPolicy.REJECT && map.containsKey(keys.get(j))) {
            throw new ParquetException("MAP row " + i + " repeats key " + keys.get(j) +
                "; flattening nested MAP entries would drop data");
          }
          map.put(keys.get(j), values.get(j));
        }
        result.add(map);
      }
    }
    return result;
  }
}
