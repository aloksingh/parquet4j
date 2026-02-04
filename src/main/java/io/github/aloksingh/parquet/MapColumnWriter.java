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
  private final int keyMaxDefLevel;
  private final int valueMaxDefLevel;

  /**
   * Create a map column writer for specific key/value types.
   *
   * @param keyType the type of the map keys
   * @param valueType the type of the map values
   * @param keyMaxDefLevel the maximum definition level for keys
   * @param valueMaxDefLevel the maximum definition level for values
   */
  public MapColumnWriter(Type keyType, Type valueType, int keyMaxDefLevel, int valueMaxDefLevel) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.keyMaxDefLevel = keyMaxDefLevel;
    this.valueMaxDefLevel = valueMaxDefLevel;
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
   * Definition level meanings depend on whether the map itself is optional:
   * <ul>
   * <li>0 = Map is NULL (only if map is optional)</li>
   * <li>1 = Map is empty (defined but has no entries)</li>
   * <li>keyMaxDefLevel = Key is present (keys are always required/non-null)</li>
   * </ul>
   * <p>
   * Example (with optional map):
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

    // Definition levels for MAP keys:
    // - 0: map is NULL (only if map is optional, i.e., keyMaxDefLevel >= 2)
    // - mapLevel: map exists but is empty (this is keyMaxDefLevel - 1)
    // - keyMaxDefLevel: key is present (keys are always required in key_value group)

    // Determine the "map exists" level based on whether the map itself can be null
    // If keyMaxDefLevel == 2: map is optional, so 0=NULL, 1=empty, 2=present
    // If keyMaxDefLevel == 1: map is required, so 0=empty, 1=present
    int mapExistsLevel = keyMaxDefLevel - 1;

    for (Map<?, ?> map : maps) {
      if (map == null) {
        levels.add(0);  // NULL map
      } else if (map.isEmpty()) {
        levels.add(mapExistsLevel);  // Empty map (map exists but no entries)
      } else {
        // Each key is present (at max definition level)
        for (int i = 0; i < map.size(); i++) {
          levels.add(keyMaxDefLevel);
        }
      }
    }

    return levels;
  }

  /**
   * Calculate definition levels for map values.
   * <p>
   * Definition level meanings depend on whether the map and values are optional:
   * <ul>
   * <li>0 = Map is NULL (only if map is optional)</li>
   * <li>emptyLevel = Map is empty (defined but has no entries)</li>
   * <li>valueMaxDefLevel-1 = Entry exists but value is NULL (only if values are optional)</li>
   * <li>valueMaxDefLevel = Value is present (non-null)</li>
   * </ul>
   * <p>
   * Example (with optional map and optional values):
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

    // Definition levels for MAP values:
    // Values are optional if valueMaxDefLevel > keyMaxDefLevel
    //
    // Example 1: optional map, optional values (keyMax=2, valueMax=3):
    //   - 0: map is NULL
    //   - 1: map exists but is empty
    //   - 2: entry exists, but value is NULL
    //   - 3: value is present
    //
    // Example 2: optional map, required values (keyMax=2, valueMax=2):
    //   - 0: map is NULL
    //   - 1: map exists but is empty
    //   - 2: value is present (values can't be NULL)
    //
    // The "map exists but is empty" level is always keyMaxDefLevel - 1
    // This is the same level for both keys and values
    int mapExistsLevel = keyMaxDefLevel - 1;

    // Values are optional if there's an extra level beyond keys
    boolean valuesOptional = (valueMaxDefLevel > keyMaxDefLevel);

    for (Map<?, ?> map : maps) {
      if (map == null) {
        levels.add(0);  // NULL map
      } else if (map.isEmpty()) {
        levels.add(mapExistsLevel);  // Empty map (must match key column!)
      } else {
        // Check each value
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (entry.getValue() == null && valuesOptional) {
            levels.add(valueMaxDefLevel - 1);  // NULL value (entry exists, value is NULL)
          } else {
            levels.add(valueMaxDefLevel);  // Present value
          }
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
