package io.github.aloksingh.parquet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import io.github.aloksingh.parquet.model.Type;

/**
 * Helper class for writing MAP logical type columns to Parquet.
 * <p>
 * Maps are stored as nested structures with repetition and definition levels:
 * - Repetition levels: track which entries belong to which map
 * - Definition levels: track NULL maps, empty maps, and NULL values
 * <p>
 * Physical structure:
 * optional group map_column (MAP) {
 * repeated group key_value {
 * required TYPE key;
 * optional TYPE value;
 * }
 * }
 */
public class MapColumnWriter {

  private final Type keyType;
  private final Type valueType;

  /**
   * Create a map column writer for specific key/value types.
   *
   * @param keyType the type of the map keys
   * @param valueType the type of the map values
   */
  public MapColumnWriter(Type keyType, Type valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }

  /**
   * Extract all keys from a list of maps, flattening them in order.
   * NULL maps and empty maps don't contribute any keys.
   *
   * @param maps the list of maps to extract keys from
   * @return a flattened list of all keys from non-null, non-empty maps
   */
  public List<Object> extractKeys(List<Map<?, ?>> maps) {
    List<Object> keys = new ArrayList<>();

    for (Map<?, ?> map : maps) {
      if (map != null && !map.isEmpty()) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          keys.add(entry.getKey());
        }
      }
    }

    return keys;
  }

  /**
   * Extract all values from a list of maps, flattening them in order.
   * NULL maps and empty maps don't contribute any values.
   *
   * @param maps the list of maps to extract values from
   * @return a flattened list of all values from non-null, non-empty maps
   */
  public List<Object> extractValues(List<Map<?, ?>> maps) {
    List<Object> values = new ArrayList<>();

    for (Map<?, ?> map : maps) {
      if (map != null && !map.isEmpty()) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          values.add(entry.getValue());
        }
      }
    }

    return values;
  }

  /**
   * Calculate repetition levels for map entries.
   * <p>
   * Repetition level meanings:
   * <ul>
   * <li>0 = First entry of a new map (or NULL/empty map)</li>
   * <li>1 = Additional entry in the same map</li>
   * </ul>
   * <p>
   * Example:
   * <pre>
   * Row 0: {a: 1, b: 2}  -&gt; levels: [0, 1]
   * Row 1: {c: 3}        -&gt; levels: [0]
   * Row 2: null          -&gt; levels: [0]
   * Row 3: {}            -&gt; levels: [0]
   * </pre>
   *
   * @param maps the list of maps to calculate repetition levels for
   * @return the repetition levels for all map entries
   */
  public List<Integer> calculateRepetitionLevels(List<Map<?, ?>> maps) {
    List<Integer> levels = new ArrayList<>();

    for (Map<?, ?> map : maps) {
      if (map == null || map.isEmpty()) {
        // NULL or empty map still produces one entry with rep level 0
        levels.add(0);
      } else {
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          levels.add(first ? 0 : 1);
          first = false;
        }
      }
    }

    return levels;
  }

  /**
   * Calculate definition levels for map keys.
   * <p>
   * Definition level meanings (max def level = 2):
   * <ul>
   * <li>0 = Map is NULL</li>
   * <li>1 = Map is empty (defined but has no entries)</li>
   * <li>2 = Key is present (keys are always required/non-null)</li>
   * </ul>
   * <p>
   * Example:
   * <pre>
   * Row 0: {a: 1, b: 2}  -&gt; levels: [2, 2]
   * Row 1: {c: 3}        -&gt; levels: [2]
   * Row 2: null          -&gt; levels: [0]
   * Row 3: {}            -&gt; levels: [1]
   * </pre>
   *
   * @param maps the list of maps to calculate key definition levels for
   * @return the definition levels for all map keys
   */
  public List<Integer> calculateKeyDefinitionLevels(List<Map<?, ?>> maps) {
    List<Integer> levels = new ArrayList<>();

    for (Map<?, ?> map : maps) {
      if (map == null) {
        levels.add(0);  // NULL map
      } else if (map.isEmpty()) {
        levels.add(1);  // Empty map
      } else {
        // Each key is present (level 2)
        for (int i = 0; i < map.size(); i++) {
          levels.add(2);
        }
      }
    }

    return levels;
  }

  /**
   * Calculate definition levels for map values.
   * <p>
   * Definition level meanings (max def level = 3):
   * <ul>
   * <li>0 = Map is NULL</li>
   * <li>1 = Map is empty (defined but has no entries)</li>
   * <li>2 = Entry exists but value is NULL</li>
   * <li>3 = Value is present (non-null)</li>
   * </ul>
   * <p>
   * Example:
   * <pre>
   * Row 0: {a: 1, b: null}  -&gt; levels: [3, 2]
   * Row 1: {c: 3}           -&gt; levels: [3]
   * Row 2: null             -&gt; levels: [0]
   * Row 3: {}               -&gt; levels: [1]
   * </pre>
   *
   * @param maps the list of maps to calculate value definition levels for
   * @return the definition levels for all map values
   */
  public List<Integer> calculateValueDefinitionLevels(List<Map<?, ?>> maps) {
    List<Integer> levels = new ArrayList<>();

    for (Map<?, ?> map : maps) {
      if (map == null) {
        levels.add(0);  // NULL map
      } else if (map.isEmpty()) {
        levels.add(1);  // Empty map
      } else {
        // Check each value
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          levels.add(entry.getValue() == null ? 2 : 3);
        }
      }
    }

    return levels;
  }

  /**
   * Count the total number of key-value pairs across all maps.
   * This is used to set numValues in the data page.
   *
   * @param maps the list of maps to count entries from
   * @return the total number of entries (including one entry each for NULL and empty maps)
   */
  public int countTotalEntries(List<Map<?, ?>> maps) {
    int count = 0;
    for (Map<?, ?> map : maps) {
      if (map == null || map.isEmpty()) {
        count++;  // NULL and empty maps still count as one "entry" for level data
      } else {
        count += map.size();
      }
    }
    return count;
  }
}
