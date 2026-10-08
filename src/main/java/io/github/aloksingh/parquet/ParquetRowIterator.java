package io.github.aloksingh.parquet;

import io.github.aloksingh.parquet.model.*;
import io.github.aloksingh.parquet.model.SchemaDescriptor.GroupNode;
import io.github.aloksingh.parquet.model.SchemaDescriptor.LeafNode;
import io.github.aloksingh.parquet.model.SchemaDescriptor.Repetition;
import io.github.aloksingh.parquet.model.SchemaDescriptor.SchemaNode;
import io.github.aloksingh.parquet.util.filter.RowColumnGroupFilter;

import java.io.IOException;
import java.util.*;

/**
 * Iterator that reads Parquet files row by row across all row groups.
 *
 * <p>Rows are decoded in bounded batches instead of whole row groups: at most
 * {@link ReadOptions#batchSize()} rows and approximately {@link ReadOptions#maxBatchBytes()}
 * of materialized values are retained at a time, and pages are decoded on demand and dropped
 * once consumed, so a budget smaller than a row group never decodes or retains rows that have
 * not been requested. Row values are annotation-aware: only STRING/ENUM/JSON annotations
 * decode as UTF-8 text, unannotated BYTE_ARRAY and BSON preserve raw bytes,
 * FIXED_LEN_BYTE_ARRAY keeps its fixed bytes, and DECIMAL/TIMESTAMP/TIME/DATE/INTEGER/UUID
 * annotations convert per {@link PrimitiveLogicalType#toLogicalValue(Object)}. MAP groups
 * surface as Map values (nested containers materialize recursively), repeated leaves surface
 * as per-row lists, and INT96 values are rejected with an explicit exception instead of being
 * silently nulled.
 *
 * <p>Row groups whose statistics prove that no row can match {@link ReadOptions#filter()} are
 * skipped before any of their chunks are read when {@link ReadOptions#pruning()} is enabled.
 * Columns referenced only by the filter are decoded for filtering but never surface in output
 * rows, which contain exactly the projected logical columns.
 *
 * <p>Usage example:
 * <pre>{@code
 * try (ParquetRowIterator iterator = new ParquetRowIterator(fileReader)) {
 *   while (iterator.hasNext()) {
 *     RowColumnGroup row = iterator.next();
 *     // Process row...
 *   }
 * }
 * }</pre>
 *
 * @see RowColumnGroupIterator
 * @see ParquetFileReader
 */
public class ParquetRowIterator implements RowColumnGroupIterator, AutoCloseable {
  private final ParquetFileReader fileReader;
  private final String source;
  private final SchemaDescriptor physicalSchema;
  private final SchemaDescriptor schema;
  private final SchemaDescriptor scanSchema;
  private final int[] outputToScan;
  private final ReadOptions readOptions;
  private final boolean closeFileReader;
  private final Map<SchemaNode, Integer> itemDefinitionLevels;
  private final RowColumnGroupFilter filter;

  private RowColumnGroupFilter pruningFilter;

  private int currentRowGroupIndex;
  private long currentRowGroupRowCount;
  private int currentRowIndex;
  private boolean groupLoaded;
  private Materializer materializer;
  private ParquetException[] columnFailures;
  private List<RowColumnGroup> batchRows = new ArrayList<>();
  private int batchPos;
  private long returnedRows;
  private long scannedRows;

  // Instrumentation observability: pruning, decode, and materialization progress.
  private long droppedRowGroups;
  private int decodedPageCount;
  private long materializedRowCount;

  /**
   * Create an iterator for a Parquet file
   *
   * @param fileReader      The file reader to iterate over
   * @param closeFileReader Whether to close the file reader when done
   */
  public ParquetRowIterator(ParquetFileReader fileReader, boolean closeFileReader) {
    this(fileReader, closeFileReader, ReadOptions.DEFAULT);
  }

  public ParquetRowIterator(ParquetFileReader fileReader, boolean closeFileReader, ReadOptions options) {
    this.fileReader = java.util.Objects.requireNonNull(fileReader, "fileReader");
    this.readOptions = java.util.Objects.requireNonNull(options, "options");
    this.physicalSchema = fileReader.getSchema();
    this.schema = options.projectsAllColumns()
        ? physicalSchema : physicalSchema.project(options.projection());
    this.source = fileReader.getSourceDescription();
    this.closeFileReader = closeFileReader;
    this.filter = options.filter();
    this.pruningFilter = options.filter();
    this.currentRowGroupIndex = 0;
    this.currentRowIndex = 0;
    this.groupLoaded = false;
    this.currentRowGroupRowCount = 0;

    // Predicate-required columns join the scan even when they are not projected; output rows
    // expose only the projected logical columns.
    SchemaDescriptor scan = schema;
    int[] mapping = identityMapping(schema.getNumLogicalColumns());
    if (filter != null) {
      Set<String> required = filter.requiredColumns(physicalSchema);
      if (!options.projectsAllColumns()) {
        LinkedHashSet<String> scanNames = new LinkedHashSet<>(options.projection());
        scanNames.addAll(required);
        if (scanNames.size() > options.projection().size()) {
          scan = physicalSchema.project(List.copyOf(scanNames));
          for (int i = 0; i < mapping.length; i++) {
            mapping[i] = logicalIndex(scan, schema.getLogicalColumn(i).getName());
          }
        }
      }
    }
    this.scanSchema = scan;
    this.outputToScan = mapping;
    this.itemDefinitionLevels = computeItemDefinitionLevels(scanSchema.root());
  }

  /**
   * Create an iterator for a Parquet file.
   * The file reader will be closed automatically when {@link #close()} is called.
   *
   * @param fileReader The file reader to iterate over
   */
  public ParquetRowIterator(ParquetFileReader fileReader) {
    this(fileReader, true);
  }

  private static int[] identityMapping(int size) {
    int[] mapping = new int[size];
    for (int i = 0; i < size; i++) {
      mapping[i] = i;
    }
    return mapping;
  }

  private static int logicalIndex(SchemaDescriptor schema, String name) {
    for (int i = 0; i < schema.getNumLogicalColumns(); i++) {
      if (schema.getLogicalColumn(i).getName().equals(name)) {
        return i;
      }
    }
    throw new IllegalArgumentException("Unknown scan column: " + name);
  }

  /**
   * The definition level at which a flattened repeated leaf's item slots begin. LIST and MAP
   * containers absorb their repeated entry layer and record its definition level as
   * {@link GroupNode#entryDefinitionLevel()}; an unannotated repeated node is its own entry
   * layer. The outermost such layer decides the row's item grouping. Leaves with no
   * repeated entry layer have no entry level (-1).
   */
  private static Map<SchemaNode, Integer> computeItemDefinitionLevels(SchemaNode root) {
    Map<SchemaNode, Integer> levels = new LinkedHashMap<>();
    collectItemDefinitionLevels(root, -1, levels);
    return levels;
  }

  private static void collectItemDefinitionLevels(SchemaNode node, int entryLevel,
                                                  Map<SchemaNode, Integer> levels) {
    int level = entryLevel;
    if (level < 0) {
      if (node instanceof GroupNode group
          && (group.kind() == LogicalType.LIST || group.kind() == LogicalType.MAP)) {
        level = group.entryDefinitionLevel();
      } else if (node.repetition() == Repetition.REPEATED) {
        level = node.maxDefinitionLevel();
      }
    }
    if (node instanceof LeafNode leaf) {
      levels.put(leaf, level);
      return;
    }
    for (SchemaNode child : node.children()) {
      collectItemDefinitionLevels(child, level, levels);
    }
  }

  // ------------------------------------------------------------------ row group loading

  /**
   * Advances to the next row group that must be read, skipping (without reading any of its
   * chunks) every group whose statistics prove that no row can match the pruning filter.
   *
   * @return false when every remaining row group has been consumed or pruned
   */
  private boolean loadNextRowGroup() {
    while (currentRowGroupIndex < fileReader.getNumRowGroups()) {
      if (canDropRowGroup(currentRowGroupIndex)) {
        droppedRowGroups++;
        currentRowGroupIndex++;
        continue;
      }
      loadRowGroup(currentRowGroupIndex);
      return true;
    }
    return false;
  }

  /** Conservative group pruning: only metadata that proves impossibility drops a group. */
  private boolean canDropRowGroup(int rowGroupIndex) {
    if (!readOptions.pruning() || pruningFilter == null) {
      return false;
    }
    ParquetMetadata.RowGroupMetadata group =
        fileReader.getMetadata().rowGroups().get(rowGroupIndex);
      return pruningFilter.canDrop(group, scanSchema, columnPath -> {
          try {
              int physicalIdx = physicalSchema.leafIndex(columnPath);
              if (physicalIdx < 0) return null;
              return fileReader.readBloomFilter(rowGroupIndex, physicalIdx);
          } catch (Exception e) {
              return null;
          }
      });
  }

  /**
   * Load a row group's column chunks and prepare per-leaf cursors. Pages are decoded lazily
   * as rows are materialized in bounded batches; nothing beyond the first batch is decoded
   * or retained until its rows are requested.
   *
   * @param rowGroupIndex The index of the row group to load
   * @throws ParquetException If reading the row group fails
   */
  private void loadRowGroup(int rowGroupIndex) {
    try {
      ParquetFileReader.RowGroupReader rowGroupReader =
          fileReader.getRowGroup(rowGroupIndex);
      currentRowGroupRowCount = rowGroupReader.getNumRows();

      // One cursor per physical leaf, resolved centrally by full path against the file schema.
      Map<SchemaNode, LeafCursor> cursors = new LinkedHashMap<>();
      for (LogicalColumnDescriptor logicalCol : scanSchema.logicalColumns()) {
        for (LeafNode leaf : logicalCol.node().leaves()) {
          int physicalIndex = physicalSchema.leafIndex(leaf.descriptor().path());
          ColumnValues values = rowGroupReader.readColumn(physicalIndex);
          cursors.put(leaf, new LeafCursor(values, () -> decodedPageCount++));
        }
      }

      materializer = new Materializer(cursors, itemDefinitionLevels);
      columnFailures = new ParquetException[scanSchema.getNumLogicalColumns()];
      currentRowIndex = 0;
      groupLoaded = true;
    } catch (IOException | RuntimeException e) {
      groupLoaded = false;
      throw new ParquetException("Failed to read row group " + rowGroupIndex + " in " + source
          + (e.getMessage() == null ? "" : ": " + e.getMessage()), e);
    }
  }

  // ------------------------------------------------------------------ iteration

  /**
   * Check if there are more rows to iterate.
   *
   * @return true if there are more matching rows in the current batch, row group, or later
   *     row groups
   */
  @Override
  public boolean hasNext() {
    if (returnedRows >= readOptions.limit()) return false;
    return ensureBatch();
  }

  /** Makes sure the current batch holds at least one not-yet-returned row. */
  private boolean ensureBatch() {
    while (batchPos >= batchRows.size()) {
      if (!groupLoaded && !loadNextRowGroup()) {
        return false;
      }
      if (fillBatch()) {
        return true;
      }
      groupLoaded = false;
      currentRowGroupIndex++;
    }
    return true;
  }

  /**
   * Materializes the next bounded batch of matching rows from the loaded row group. Rows are
   * decoded in row order (batches always end on a row boundary, so repeated columns never
   * lose or duplicate items at a batch seam) and the batch stops at {@link ReadOptions#batchSize()}
   * rows or {@link ReadOptions#maxBatchBytes()} of estimated materialized values. Only the
   * projected logical columns are retained per row; filter-only columns are evaluated and
   * dropped.
   */
  private boolean fillBatch() {
    batchRows = new ArrayList<>();
    batchPos = 0;
    if (currentRowIndex >= currentRowGroupRowCount) {
      return false;
    }
    long estimatedBytes = 0;
    long remainingLimit = readOptions.limit() - returnedRows;
    while (currentRowIndex < currentRowGroupRowCount
        && batchRows.size() < readOptions.batchSize()
        && batchRows.size() < remainingLimit
        && (batchRows.isEmpty() || estimatedBytes < readOptions.maxBatchBytes())) {
      Object[] scanValues = materializeScanRow();
      currentRowIndex++;
      long rowPosition = scannedRows++;
      materializedRowCount++;
      if (filter != null && !matchesFilter(scanValues, rowPosition)) {
        continue;
      }
      estimatedBytes += estimateRowBytes(scanValues);
      batchRows.add(projectRow(scanValues));
    }
    return !batchRows.isEmpty();
  }

  /** Materializes one scan row's values, one logical column at a time. */
  private Object[] materializeScanRow() {
    Object[] scanValues = new Object[scanSchema.getNumLogicalColumns()];
    for (int column = 0; column < scanValues.length; column++) {
      LogicalColumnDescriptor logicalCol = scanSchema.getLogicalColumn(column);
      if (columnFailures[column] != null) {
        // A previous failure left this column's cursor mid-row: keep rejecting its values
        // instead of reading misaligned events.
        scanValues[column] = new DeferredFailure(columnFailures[column]);
        continue;
      }
      try {
        scanValues[column] = materializer.readRow(logicalCol.node());
      } catch (RuntimeException failure) {
        ParquetException wrapped = new ParquetException(
            "Failed to decode column '" + String.join(".", logicalCol.node().path())
                + "' of type " + columnType(logicalCol) + " in " + source
                + (failure.getMessage() == null ? "" : ": " + failure.getMessage()), failure);
        if (containsInt96(logicalCol.node())) {
          // Deferred like eager materialization: the value rejects on access, not before.
          columnFailures[column] = wrapped;
          scanValues[column] = new DeferredFailure(wrapped);
        } else {
          throw wrapped;
        }
      }
    }
    return scanValues;
  }

  private static String columnType(LogicalColumnDescriptor column) {
    if (column.node() instanceof LeafNode leaf) {
      return String.valueOf(leaf.descriptor().physicalType());
    }
    return String.valueOf(column.getLogicalType());
  }

  private boolean matchesFilter(Object[] scanValues, long rowPosition) {
    RowColumnGroup row = new SimpleRowColumnGroup(scanSchema,
        index -> unwrap(scanValues[index]));
    try {
      return filter.apply(row);
    } catch (RuntimeException failure) {
      throw new ParquetException("Failed to evaluate filter " + filter.expression()
          + " at row " + rowPosition + " in " + source
          + (failure.getMessage() == null ? "" : ": " + failure.getMessage()), failure);
    }
  }

  /** Copies the projected values out of the scan row so hidden columns are not retained. */
  private RowColumnGroup projectRow(Object[] scanValues) {
    Object[] values = new Object[outputToScan.length];
    for (int i = 0; i < values.length; i++) {
      values[i] = scanValues[outputToScan[i]];
    }
    return new SimpleRowColumnGroup(schema, index -> unwrap(values[index]));
  }

  private static Object unwrap(Object value) {
    if (value instanceof DeferredFailure deferred) {
      throw deferred.failure;
    }
    return value;
  }

  /** Rough retained-size estimate for one materialized scan row. */
  private static long estimateRowBytes(Object[] scanValues) {
    long total = 16L * scanValues.length + 16L;
    for (Object value : scanValues) {
      total += estimateValueBytes(value);
    }
    return total;
  }

  private static long estimateValueBytes(Object value) {
    if (value == null || value instanceof DeferredFailure) return 8L;
    if (value instanceof byte[] bytes) return bytes.length + 24L;
    if (value instanceof String text) return 2L * text.length() + 24L;
    if (value instanceof List<?> list) {
      long total = 32L;
      for (Object item : list) {
        total += estimateValueBytes(item);
      }
      return total;
    }
    if (value instanceof Map<?, ?> map) {
      long total = 48L;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        total += estimateValueBytes(entry.getKey()) + estimateValueBytes(entry.getValue());
      }
      return total;
    }
    return 16L;
  }

  /**
   * Get the next row from the Parquet file.
   * Automatically loads the next row group when the current one is exhausted.
   *
   * @return A RowColumnGroup containing the values for all logical columns in the row
   * @throws NoSuchElementException If there are no more rows to read
   */
  @Override
  public RowColumnGroup next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more rows");
    }
    RowColumnGroup row = batchRows.get(batchPos++);
    returnedRows++;
    return row;
  }

  /**
   * Close the underlying file reader if this iterator owns it.
   * Only closes the file reader if closeFileReader was set to true during construction.
   *
   * @throws IOException If closing the file reader fails
   */
  public void close() throws IOException {
    if (closeFileReader) {
      fileReader.close();
    }
  }

  /**
   * Get the total number of rows that will be iterated across all row groups.
   *
   * @return The total row count from the file metadata
   */
  public long getTotalRowCount() {
    return fileReader.getTotalRowCount();
  }

  /**
   * Get the schema for the rows being iterated: exactly the projected logical columns.
   *
   * @return The schema descriptor containing all logical column definitions
   */
  public SchemaDescriptor getSchema() {
    return schema;
  }

  /** A short description of the underlying file/source, used to contextualize failures. */
  public String getSourceDescription() {
    return source;
  }

  /** Number of row groups skipped by statistics pruning so far (instrumentation). */
  public long getDroppedRowGroupCount() {
    return droppedRowGroups;
  }

  /** Number of data pages decoded so far (instrumentation for decode-bound compliance). */
  public int getDecodedPageCount() {
    return decodedPageCount;
  }

  /** Number of scan rows materialized so far (instrumentation for batch-bound compliance). */
  public long getMaterializedRowCount() {
    return materializedRowCount;
  }

  /**
   * Adopts a filter for row-group pruning when this iterator has none. Used by
   * {@link FilteringParquetRowIterator} so plain and filtering scans share one pruning
   * mechanism; the delegate's output rows and residual filtering are unaffected. Binding
   * consults metadata only.
   */
  void attachPruningFilter(RowColumnGroupFilter candidate) {
    if (candidate == null || pruningFilter != null) {
      return;
    }
    candidate.requiredColumns(scanSchema);
    pruningFilter = candidate;
  }

  // ------------------------------------------------------------------ materialization

  /**
   * Annotation-aware leaf conversion; explicit failures instead of silent nulls. INT96 has
   * no row-level representation and is rejected — its raw bytes stay available through
   * {@link ColumnValues#decodeAsInt96()}.
   */
  private static Object convertValue(LeafNode leaf, Object physical) {
    ColumnDescriptor descriptor = leaf.descriptor();
    if (descriptor.physicalType() == Type.INT96) {
      throw new ParquetException("INT96 values are not supported by the row API; column '"
          + descriptor.getPathString() + "' must be read as raw bytes via "
          + "ColumnValues.decodeAsInt96()");
    }
    PrimitiveLogicalType annotation = descriptor.annotation();
    if (annotation.kind() == PrimitiveLogicalType.Kind.NONE) {
      return physical;
    }
    try {
      return annotation.toLogicalValue(physical);
    } catch (RuntimeException failure) {
      throw new ParquetException("Failed to convert column '" + descriptor.getPathString()
          + "' as " + annotation.kind() + ": " + failure.getMessage(), failure);
    }
  }

  private static boolean containsInt96(SchemaNode node) {
    for (LeafNode leaf : node.leaves()) {
      if (leaf.descriptor().physicalType() == Type.INT96) {
        return true;
      }
    }
    return false;
  }

  /** Value placeholder that rethrows its captured rejection on access (e.g. INT96). */
  private static final class DeferredFailure {
    private final ParquetException failure;

    private DeferredFailure(ParquetException failure) {
      this.failure = failure;
    }
  }

  /** Recursive reader of one row's values from the schema tree. */
  private static final class Materializer {
    private final Map<SchemaNode, LeafCursor> cursors;
    private final Map<SchemaNode, Integer> itemDefinitionLevels;

    private Materializer(Map<SchemaNode, LeafCursor> cursors,
                         Map<SchemaNode, Integer> itemDefinitionLevels) {
      this.cursors = cursors;
      this.itemDefinitionLevels = itemDefinitionLevels;
    }

    /** One row value: repeated leaves surface as per-row item lists. */
    private Object readRow(SchemaNode node) {
      if (node instanceof LeafNode leaf && leaf.maxRepetitionLevel() > 0) {
        return readRepeatedLeaf(leaf);
      }
      return readNode(node);
    }

    private Object readNode(SchemaNode node) {
      if (node instanceof LeafNode leaf) {
        return readLeafValue(leaf);
      }
      GroupNode group = (GroupNode) node;
      return switch (group.kind()) {
        case LIST -> readList(group);
        case MAP -> readMap(group);
        default -> {
          if (group.repetition() == Repetition.REPEATED) {
            yield readInstances(group, group.maxDefinitionLevel(), group.maxRepetitionLevel(),
                () -> readFields(group));
          }
          yield readOptional(group, () -> readFields(group));
        }
      };
    }

    private Object readLeafValue(LeafNode leaf) {
      LeafCursor cursor = cursors.get(leaf);
      if (!cursor.hasNext()) {
        throw new ParquetException("Ran out of values for column '"
            + String.join(".", leaf.path()) + "'");
      }
      int definition = cursor.peekDefinition();
      Object physical = cursor.next();
      if (definition < leaf.maxDefinitionLevel()) {
        return null;
      }
      return convertValue(leaf, physical);
    }

    /**
     * One row value for a repeated leaf: the row's item values (nulls preserved), or
     * {@code null} when the row carries no item at all (null or empty container).
     */
    private Object readRepeatedLeaf(LeafNode leaf) {
      LeafCursor cursor = cursors.get(leaf);
      if (!cursor.hasNext()) {
        throw new ParquetException("Ran out of values for column '"
            + String.join(".", leaf.path()) + "'");
      }
      int entryDefinition = itemDefinitionLevels.get(leaf);
      if (cursor.peekDefinition() < entryDefinition) {
        cursor.next();
        return null;
      }
      List<Object> items = new ArrayList<>();
      boolean first = true;
      while (cursor.hasNext()) {
        if (!first && cursor.peekRepetition() == 0) {
          break;
        }
        first = false;
        int definition = cursor.peekDefinition();
        Object physical = cursor.next();
        items.add(definition == leaf.maxDefinitionLevel()
            ? convertValue(leaf, physical) : null);
      }
      return items;
    }

    private Map<String, Object> readFields(GroupNode group) {
      Map<String, Object> fields = new LinkedHashMap<>();
      for (SchemaNode child : group.children()) {
        fields.put(child.name(), readNode(child));
      }
      return fields;
    }

    private Object readOptional(GroupNode group, java.util.function.Supplier<Object> body) {
      if (group.repetition() == Repetition.OPTIONAL
          && probe(group).peekDefinition() < group.maxDefinitionLevel()) {
        drainOneEventPerLeaf(group);
        return null;
      }
      return body.get();
    }

    private Object readList(GroupNode group) {
      if (group.repetition() == Repetition.OPTIONAL
          && probe(group).peekDefinition() < group.maxDefinitionLevel()) {
        drainOneEventPerLeaf(group);
        return null;
      }
      SchemaNode element = group.children().get(0);
      return readInstances(group, group.entryDefinitionLevel(), group.entryRepetitionLevel(),
          () -> readNode(element));
    }

    private Object readMap(GroupNode group) {
      if (group.repetition() == Repetition.OPTIONAL
          && probe(group).peekDefinition() < group.maxDefinitionLevel()) {
        drainOneEventPerLeaf(group);
        return null;
      }
      List<SchemaNode> children = group.children();
      SchemaNode key = children.get(0);
      SchemaNode value = children.size() > 1 ? children.get(1) : null;
      Map<Object, Object> entries = new LinkedHashMap<>();
      readInstances(group, group.entryDefinitionLevel(), group.entryRepetitionLevel(), () -> {
        Object mapKey = readNode(key);
        Object mapValue = value == null ? null : readNode(value);
        entries.put(mapKey, mapValue);
        return null;
      });
      return entries;
    }

    /**
     * Materializes one repeated entry layer. Entries exist only when the next event reaches
     * {@code entryDefinition}; otherwise exactly one event per leaf marks the empty (or
     * absent) container. New instances start at events with {@code entryRepetition}; larger
     * repetition levels continue the current instance and smaller ones end the container.
     */
    private List<Object> readInstances(SchemaNode subtree, int entryDefinition,
                                       int entryRepetition,
                                       java.util.function.Supplier<Object> instanceReader) {
      List<Object> out = new ArrayList<>();
      LeafCursor probe = probe(subtree);
      if (!probe.hasNext()) {
        return out;
      }
      if (probe.peekDefinition() < entryDefinition) {
        drainOneEventPerLeaf(subtree);
        return out;
      }
      boolean first = true;
      while (probe.hasNext()) {
        if (!first) {
          int repetition = probe.peekRepetition();
          if (repetition < entryRepetition) {
            break;
          }
          if (repetition == entryRepetition && probe.peekDefinition() < entryDefinition) {
            break;
          }
        }
        first = false;
        out.add(instanceReader.get());
      }
      return out;
    }

    private LeafCursor probe(SchemaNode subtree) {
      return cursors.get(subtree.leaves().get(0));
    }

    /** A skipped occurrence (null group/container, empty entry layer) costs one event per leaf. */
    private void drainOneEventPerLeaf(SchemaNode subtree) {
      for (LeafNode leaf : subtree.leaves()) {
        cursors.get(leaf).next();
      }
    }
  }

  /**
   * Event cursor over one physical leaf, independent of page boundaries. Pages decode lazily
   * one at a time and drop once fully consumed, so retention is bounded by the current batch
   * instead of the whole column chunk. Physical values are consumed only at the leaf's
   * maximum definition level.
   */
  private static final class LeafCursor {
    private final List<Page> sources;
    private final ColumnPageDecoder decoder;
    private final Runnable onPageDecoded;
    private final int maxDefinitionLevel;
    private final ArrayDeque<DecodedPage> ready = new ArrayDeque<>();
    private int sourceIndex;
    private int eventIndex;
    private int physicalIndex;

    private LeafCursor(ColumnValues values, Runnable onPageDecoded) {
      this.sources = values.getPages();
      this.maxDefinitionLevel = values.getColumnDescriptor().maxDefinitionLevel();
      this.decoder = new ColumnPageDecoder(values.getColumnDescriptor());
      this.onPageDecoded = onPageDecoded;
    }

    private boolean hasNext() {
      return current() != null;
    }

    private int peekDefinition() {
      return current().definitionLevel(eventIndex);
    }

    private int peekRepetition() {
      return current().repetitionLevel(eventIndex);
    }

    /** Consumes the current event and returns its physical value (null below the maximum). */
    private Object next() {
      DecodedPage page = current();
      int definition = page.definitionLevel(eventIndex);
      Object physical = definition == maxDefinitionLevel
          ? page.physicalValue(physicalIndex) : null;
      if (definition == maxDefinitionLevel) {
        physicalIndex++;
      }
      eventIndex++;
      return physical;
    }

    /** The not-yet-consumed head page, decoding source pages on demand; null at exhaustion. */
    private DecodedPage current() {
      skipExhaustedPages();
      while (ready.isEmpty() && sourceIndex < sources.size()) {
        Page source = sources.get(sourceIndex++);
        if (source instanceof Page.DictionaryPage dictionary) {
          decoder.setDictionary(dictionary);
        } else {
          ready.addLast(decoder.decode(source));
          onPageDecoded.run();
        }
      }
      return ready.peekFirst();
    }

    private void skipExhaustedPages() {
      while (!ready.isEmpty() && eventIndex == ready.peekFirst().numValues()) {
        ready.pollFirst();
        eventIndex = 0;
        physicalIndex = 0;
      }
    }
  }
}
