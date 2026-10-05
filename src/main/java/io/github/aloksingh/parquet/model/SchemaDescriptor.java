package io.github.aloksingh.parquet.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;

/**
 * Describes the schema structure of a Parquet file, including both physical and logical column
 * representations and the annotated schema tree they are derived from.
 * <p>
 * Physical columns represent the actual storage layout in the Parquet file (one per primitive
 * leaf), while logical columns provide the user-facing view of the schema's top-level fields.
 * The schema tree ({@link #root()}) is reconstructed from the file's LogicalType/ConvertedType
 * annotations (LIST per the 3-level standard plus legacy 2-level/repeated-element variants, MAP
 * per the 3-level standard with a required key, STRUCT for every unannotated group) and is the
 * single source of truth for leaf index resolution.
 *
 * @param name           the schema name (typically "message" for root schema)
 * @param columns        the physical column descriptors that define the actual storage layout,
 *                       in leaf (depth-first) schema order
 * @param logicalColumns the logical column descriptors (top-level fields) of the user-facing
 *                       schema, in schema order
 * @param root           the reconstructed schema tree; when {@code null}, a flat tree of the
 *                       physical columns is synthesized
 */
public record SchemaDescriptor(String name, List<ColumnDescriptor> columns,
                               List<LogicalColumnDescriptor> logicalColumns,
                               GroupNode root) {

  /** Compatibility constructor without an explicit schema tree. */
  public SchemaDescriptor(String name, List<ColumnDescriptor> columns,
                          List<LogicalColumnDescriptor> logicalColumns) {
    this(name, columns, logicalColumns, null);
  }

  public SchemaDescriptor {
    Objects.requireNonNull(name, "name");
    columns = columns == null ? List.of() : List.copyOf(columns);
    logicalColumns = logicalColumns == null ? List.of() : List.copyOf(logicalColumns);
    if (root == null) {
      root = synthesizeFlatTree(columns);
    }
  }

  // ------------------------------------------------------------------ schema tree

  /** Repetition of one schema element. */
  public enum Repetition {
    REQUIRED, OPTIONAL, REPEATED;

    static Repetition of(FieldRepetitionType type) {
      return switch (type) {
        case REQUIRED -> REQUIRED;
        case OPTIONAL -> OPTIONAL;
        case REPEATED -> REPEATED;
      };
    }
  }

  /**
   * One node of the reconstructed schema tree. Group nodes are classified as LIST, MAP or
   * STRUCT (the unannotated default); leaves are PRIMITIVE. Paths are full physical paths from
   * the root message's children, so leaves resolve to physical column indexes exactly.
   * Name matching is case-sensitive throughout.
   */
  public sealed interface SchemaNode permits GroupNode, LeafNode {

    String name();

    /** Full path from the root message's children to this node (never includes the root name). */
    List<String> path();

    LogicalType kind();

    /** Cumulative definition level at which this node is present. */
    int maxDefinitionLevel();

    /** Cumulative repetition level of this node. */
    int maxRepetitionLevel();

    /**
     * Repetition of this schema element. Elements whose repetition is absorbed by a parent
     * LIST/MAP container read {@link Repetition#OPTIONAL} here.
     */
    Repetition repetition();

    List<SchemaNode> children();

    default boolean isLeaf() {
      return this instanceof LeafNode;
    }

    /** All primitive leaves below (or of) this node, in depth-first schema order. */
    default List<LeafNode> leaves() {
      List<LeafNode> out = new ArrayList<>();
      collectLeaves(this, out);
      return out;
    }

    private static void collectLeaves(SchemaNode node, List<LeafNode> out) {
      if (node instanceof LeafNode leaf) {
        out.add(leaf);
      } else {
        for (SchemaNode child : node.children()) {
          collectLeaves(child, out);
        }
      }
    }

    static boolean samePath(List<String> a, String[] b) {
      if (a.size() != b.length) {
        return false;
      }
      for (int i = 0; i < b.length; i++) {
        if (!a.get(i).equals(b[i])) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * A primitive leaf. The annotation on {@link #descriptor()} carries the leaf's logical
   * interpretation (see {@link PrimitiveLogicalType}).
   */
  public record LeafNode(String name, List<String> path, int maxDefinitionLevel,
                         int maxRepetitionLevel, Repetition repetition,
                         ColumnDescriptor descriptor) implements SchemaNode {

    public LeafNode {
      path = List.copyOf(path);
      Objects.requireNonNull(descriptor, "descriptor");
    }

    @Override
    public LogicalType kind() {
      return LogicalType.PRIMITIVE;
    }

    @Override
    public List<SchemaNode> children() {
      return List.of();
    }
  }

  /**
   * A group node classified as LIST, MAP or STRUCT. LIST and MAP nodes are normalized: their
   * children are the logical entries (the LIST element; the MAP key and optional value) rather
   * than the anonymous repeated wrapper group, which is described by
   * {@link #entryDefinitionLevel()}/{@link #entryRepetitionLevel()}. A node whose repetition is
   * {@link Repetition#REPEATED} materializes as a list of instances.
   */
  public record GroupNode(String name, List<String> path, LogicalType kind,
                          int maxDefinitionLevel, int maxRepetitionLevel,
                          Repetition repetition, int entryDefinitionLevel,
                          int entryRepetitionLevel,
                          List<SchemaNode> children) implements SchemaNode {

    public GroupNode {
      path = List.copyOf(path);
      children = List.copyOf(children);
      if (kind == LogicalType.PRIMITIVE) {
        throw new IllegalArgumentException("A group cannot be PRIMITIVE");
      }
    }
  }

  // ------------------------------------------------------------------ construction

  /**
   * Reconstructs the annotated schema tree from Thrift schema elements and derives both the
   * physical leaf columns and the logical (top-level) columns from it. This is the single
   * place where leaves get their {@link PrimitiveLogicalType} annotation and their leaf index.
   *
   * @param name     the root schema (message) name
   * @param elements the flat schema element list, root first
   */
  public static SchemaDescriptor fromSchemaElements(String name, List<SchemaElement> elements) {
    Objects.requireNonNull(elements, "elements");
    if (elements.isEmpty()) {
      throw new ParquetException("Schema must contain a root element");
    }
    SchemaElement rootElement = elements.get(0);
    int[] cursor = {1};
    List<SchemaNode> children = new ArrayList<>();
    List<ColumnDescriptor> columns = new ArrayList<>();
    for (int i = 0; i < rootElement.getNum_children(); i++) {
      children.add(parseNode(elements, cursor, List.of(), 0, 0, columns));
    }
    if (cursor[0] != elements.size()) {
      throw new ParquetException("Schema element count does not match the declared tree");
    }
    GroupNode root = new GroupNode(rootElement.getName(), List.of(), LogicalType.STRUCT,
        0, 0, Repetition.REQUIRED, 0, 0, children);
    List<LogicalColumnDescriptor> logicalColumns = new ArrayList<>();
    for (SchemaNode child : children) {
      collectLogicalColumns(child, columns, logicalColumns);
    }
    return new SchemaDescriptor(name, columns, logicalColumns, root);
  }

  /**
   * Builds the user-facing logical columns from the reconstructed tree, in physical leaf
   * order. MAP groups outside repeated containers collapse into a single Map-valued column
   * that owns their leaves (nested values materialize recursively); every other leaf becomes
   * its own PRIMITIVE column named by its full dot-separated path, so STRUCT groups are
   * represented by their leaves and repeated leaves surface as per-row lists.
   */
  private static void collectLogicalColumns(SchemaNode node, List<ColumnDescriptor> allColumns,
                                            List<LogicalColumnDescriptor> out) {
    if (node instanceof LeafNode leaf) {
      out.add(new LogicalColumnDescriptor(String.join(".", leaf.path()), LogicalType.PRIMITIVE,
          leaf.descriptor().physicalType(), leaf.descriptor(), node));
      return;
    }
    GroupNode group = (GroupNode) node;
    if (group.kind() == LogicalType.MAP && group.maxRepetitionLevel() == 0) {
      List<LeafNode> leaves = group.leaves();
      LeafNode key = leaves.isEmpty() ? null : leaves.get(0);
      LeafNode value = leaves.size() > 1 ? leaves.get(1) : null;
      MapMetadata metadata = new MapMetadata(
          leafIndex(allColumns, key), value == null ? -1 : leafIndex(allColumns, value),
          key == null ? null : key.descriptor().physicalType(),
          value == null ? null : value.descriptor().physicalType(),
          key == null ? null : key.descriptor(),
          value == null ? null : value.descriptor());
      out.add(new LogicalColumnDescriptor(String.join(".", group.path()), LogicalType.MAP,
          metadata, node));
      return;
    }
    for (SchemaNode child : group.children()) {
      collectLogicalColumns(child, allColumns, out);
    }
  }

  private static SchemaNode parseNode(List<SchemaElement> elements, int[] cursor,
                                      List<String> parentPath, int parentDef, int parentRep,
                                      List<ColumnDescriptor> columns) {
    if (cursor[0] >= elements.size()) {
      throw new ParquetException("Truncated schema element list");
    }
    SchemaElement element = elements.get(cursor[0]++);
    Repetition repetition = element.isSetRepetition_type()
        ? Repetition.of(element.getRepetition_type()) : Repetition.REQUIRED;
    List<String> path = new ArrayList<>(parentPath);
    path.add(element.getName());
    int def = parentDef + (repetition == Repetition.REQUIRED ? 0 : 1);
    int rep = parentRep + (repetition == Repetition.REPEATED ? 1 : 0);

    if (element.isSetType()) {
      Type type = Type.fromValue(element.getType().getValue());
      int typeLength = element.isSetType_length() ? element.getType_length() : 0;
      ColumnDescriptor descriptor = new ColumnDescriptor(type, path.toArray(new String[0]),
          def, rep, typeLength, PrimitiveLogicalType.fromSchemaElement(element));
      columns.add(descriptor);
      return new LeafNode(element.getName(), path, def, rep, repetition, descriptor);
    }

    List<ParsedChild> rawChildren = new ArrayList<>();
    for (int i = 0; i < element.getNum_children(); i++) {
      SchemaElement childElement = elements.get(cursor[0]);
      SchemaNode childNode = parseNode(elements, cursor, path, def, rep, columns);
      rawChildren.add(new ParsedChild(childElement, childNode));
    }
    return classifyGroup(element, path, def, rep, repetition, rawChildren);
  }

  private record ParsedChild(SchemaElement element, SchemaNode node) {
  }

  /**
   * Classifies a group from its annotations and shape. Annotation first: a LIST or MAP
   * annotation selects the interpretation and shape validation decides between MAP and its
   * STRUCT fallback (a standard MAP has the 3-level structure with a required key). Unannotated
   * groups fall back to the legacy MAP shapes only as a last resort — an annotated
   * {@code MAP_KEY_VALUE} wrapper, then the exact {@code key_value}/{@code key}/{@code value}
   * name pattern — and are STRUCT otherwise, so unannotated structures with merely MAP-like
   * child names are never misread as MAP.
   */
  private static GroupNode classifyGroup(SchemaElement element, List<String> path, int def,
                                         int rep, Repetition repetition,
                                         List<ParsedChild> rawChildren) {
    boolean annotatedList = element.isSetLogicalType() && element.getLogicalType().isSetLIST()
        || element.getConverted_type() == ConvertedType.LIST;
    boolean annotatedMap = element.isSetLogicalType() && element.getLogicalType().isSetMAP()
        || element.getConverted_type() == ConvertedType.MAP;

    if (annotatedList) {
      GroupNode list = asList(path, def, rep, repetition, rawChildren);
      if (list != null) {
        return list;
      }
    }
    if (annotatedMap) {
      GroupNode map = asMap(path, def, rep, repetition, rawChildren, MapRecognition.ANNOTATED);
      if (map != null) {
        return map;
      }
    }
    GroupNode map = asMap(path, def, rep, repetition, rawChildren, MapRecognition.LEGACY_WRAPPER);
    if (map != null) {
      return map;
    }
    map = asMap(path, def, rep, repetition, rawChildren, MapRecognition.LEGACY_NAMES);
    if (map != null) {
      return map;
    }
    return struct(path, def, rep, repetition, rawChildren);
  }

  private enum MapRecognition {
    /** Modern MAP or legacy MAP annotation on the group. */
    ANNOTATED,
    /** Unannotated group whose repeated wrapper carries the legacy MAP_KEY_VALUE annotation. */
    LEGACY_WRAPPER,
    /** Last resort: the exact {@code key_value}/{@code key}/{@code value} name pattern. */
    LEGACY_NAMES
  }

  private static GroupNode asMap(List<String> path, int def, int rep, Repetition repetition,
                                 List<ParsedChild> children, MapRecognition recognition) {
    if (children.size() != 1 || children.get(0).node().repetition() != Repetition.REPEATED) {
      return null;
    }
    ParsedChild wrapper = children.get(0);
    List<SchemaNode> entries = wrapper.node().children();
    if (entries.isEmpty() || entries.size() > 2) {
      return null;
    }
    SchemaNode key = entries.get(0);
    if (!"key".equals(key.name()) || key.repetition() != Repetition.REQUIRED) {
      return null;
    }
    SchemaNode value = entries.size() == 2 ? entries.get(1) : null;
    if (value != null && !"value".equals(value.name())) {
      return null;
    }
    switch (recognition) {
      case ANNOTATED -> { }
      case LEGACY_WRAPPER -> {
        if (wrapper.element().getConverted_type() != ConvertedType.MAP_KEY_VALUE) {
          return null;
        }
      }
      case LEGACY_NAMES -> {
        if (!"key_value".equals(wrapper.node().name())) {
          return null;
        }
      }
    }
    List<SchemaNode> normalized = value == null ? List.of(key) : List.of(key, value);
    return new GroupNode(path.get(path.size() - 1), path, LogicalType.MAP, def, rep, repetition,
        wrapper.node().maxDefinitionLevel(), wrapper.node().maxRepetitionLevel(), normalized);
  }

  private static GroupNode asList(List<String> path, int def, int rep, Repetition repetition,
                                  List<ParsedChild> children) {
    if (children.size() != 1) {
      return null;
    }
    ParsedChild only = children.get(0);
    if (only.node().repetition() != Repetition.REPEATED) {
      return null;
    }
    // The entry layer is this repeated child; its own repetition is absorbed by the LIST.
    int entryDef = only.node().maxDefinitionLevel();
    int entryRep = only.node().maxRepetitionLevel();
    SchemaNode element;
    if (only.node().kind() == LogicalType.LIST) {
      // Legacy 2-level variant: the repeated group itself is annotated LIST (list of lists).
      element = asContainerElement(only.node());
    } else if (only.node() instanceof LeafNode) {
      // Legacy repeated-element variant: repeated primitive directly under the LIST group.
      element = only.node();
    } else if (only.node().children().size() == 1) {
      // 3-level standard: anonymous repeated wrapper with a single element child.
      element = asContainerElement(only.node().children().get(0));
    } else {
      // Legacy 2-level variant with a multi-field struct element.
      element = only.node();
    }
    return new GroupNode(path.get(path.size() - 1), path, LogicalType.LIST, def, rep, repetition,
        entryDef, entryRep, List.of(element));
  }

  /** Strips the entry-level repetition from the element that the LIST container absorbs. */
  private static SchemaNode asContainerElement(SchemaNode node) {
    if (node instanceof LeafNode leaf) {
      return new LeafNode(leaf.name(), leaf.path(), leaf.maxDefinitionLevel(),
          leaf.maxRepetitionLevel(),
          leaf.repetition() == Repetition.REPEATED ? Repetition.OPTIONAL : leaf.repetition(),
          leaf.descriptor());
    }
    GroupNode group = (GroupNode) node;
    return new GroupNode(group.name(), group.path(), group.kind(), group.maxDefinitionLevel(),
        group.maxRepetitionLevel(),
        group.repetition() == Repetition.REPEATED ? Repetition.OPTIONAL : group.repetition(),
        group.entryDefinitionLevel(), group.entryRepetitionLevel(), group.children());
  }

  private static GroupNode struct(List<String> path, int def, int rep, Repetition repetition,
                                  List<ParsedChild> children) {
    List<SchemaNode> nodes = new ArrayList<>(children.size());
    for (ParsedChild child : children) {
      nodes.add(child.node());
    }
    return new GroupNode(path.get(path.size() - 1), path, LogicalType.STRUCT, def, rep,
        repetition, def, rep, nodes);
  }

  /** Wraps each physical column as a flat tree (used for programmatically built schemas). */
  private static GroupNode synthesizeFlatTree(List<ColumnDescriptor> columns) {
    return synthesizeGroup("", List.of(), columns, 0, 0, 0);
  }

  private static GroupNode synthesizeGroup(String name, List<String> path,
                                           List<ColumnDescriptor> columns, int depth,
                                           int parentDef, int parentRep) {
    Map<String, List<ColumnDescriptor>> byChild = new LinkedHashMap<>();
    for (ColumnDescriptor column : columns) {
      byChild.computeIfAbsent(column.path()[depth], k -> new ArrayList<>()).add(column);
    }
    List<SchemaNode> children = new ArrayList<>();
    for (Map.Entry<String, List<ColumnDescriptor>> entry : byChild.entrySet()) {
      List<ColumnDescriptor> group = entry.getValue();
      List<String> childPath = new ArrayList<>(path);
      childPath.add(entry.getKey());
      boolean leaf = group.get(0).path().length == depth + 1;
      if (leaf) {
        ColumnDescriptor column = group.get(0);
        children.add(new LeafNode(entry.getKey(), childPath, column.maxDefinitionLevel(),
            column.maxRepetitionLevel(),
            column.maxRepetitionLevel() > parentRep ? Repetition.REPEATED
                : column.maxDefinitionLevel() > parentDef ? Repetition.OPTIONAL
                : Repetition.REQUIRED,
            column));
      } else {
        children.add(synthesizeGroup(entry.getKey(), childPath, group, depth + 1, parentDef,
            parentRep));
      }
    }
    int def = parentDef;
    int rep = parentRep;
    int minDef = Integer.MAX_VALUE;
    int minRep = Integer.MAX_VALUE;
    for (ColumnDescriptor column : columns) {
      minDef = Math.min(minDef, column.maxDefinitionLevel());
      minRep = Math.min(minRep, column.maxRepetitionLevel());
    }
    Repetition repetition = Repetition.REQUIRED;
    if (!columns.isEmpty()) {
      def = minDef;
      rep = minRep;
      repetition = minRep > parentRep ? Repetition.REPEATED
          : minDef > parentDef ? Repetition.OPTIONAL : Repetition.REQUIRED;
    }
    return new GroupNode(name, path, LogicalType.STRUCT, def, rep, repetition, def, rep, children);
  }

  // ------------------------------------------------------------------ logical columns

  /** Builds the user-facing logical column for one top-level schema tree node. */
  public static LogicalColumnDescriptor logicalColumnFor(SchemaNode node) {
    return logicalColumnFor(node, null);
  }

  private static LogicalColumnDescriptor logicalColumnFor(SchemaNode node,
                                                         List<ColumnDescriptor> allColumns) {
    if (node instanceof LeafNode leaf) {
      return new LogicalColumnDescriptor(leaf.name(), LogicalType.PRIMITIVE,
          leaf.descriptor().physicalType(), leaf.descriptor(), node);
    }
    GroupNode group = (GroupNode) node;
    List<LeafNode> leaves = group.leaves();
    switch (group.kind()) {
      case MAP -> {
        LeafNode key = leaves.isEmpty() ? null : leaves.get(0);
        LeafNode value = leaves.size() > 1 ? leaves.get(1) : null;
        MapMetadata metadata = new MapMetadata(
            leafIndex(allColumns, key), value == null ? -1 : leafIndex(allColumns, value),
            key == null ? null : key.descriptor().physicalType(),
            value == null ? null : value.descriptor().physicalType(),
            key == null ? null : key.descriptor(),
            value == null ? null : value.descriptor());
        return new LogicalColumnDescriptor(group.name(), LogicalType.MAP, metadata, node);
      }
      case LIST -> {
        LeafNode element = leaves.isEmpty() ? null : leaves.get(0);
        ListMetadata metadata = new ListMetadata(leafIndex(allColumns, element),
            element == null ? null : element.descriptor().physicalType(),
            element == null ? null : element.descriptor());
        return new LogicalColumnDescriptor(group.name(), LogicalType.LIST, metadata, node);
      }
      default -> {
        return new LogicalColumnDescriptor(group.name(), LogicalType.STRUCT, node);
      }
    }
  }

  private static int leafIndex(List<ColumnDescriptor> columns, LeafNode leaf) {
    if (leaf == null || columns == null) {
      return -1;
    }
    for (int i = 0; i < columns.size(); i++) {
      if (SchemaNode.samePath(leaf.path(), columns.get(i).path())) {
        return i;
      }
    }
    return -1;
  }

  // ------------------------------------------------------------------ resolver

  /**
   * Resolves a leaf's central, physical index by its full path. Matching is exact and
   * case-sensitive; ambiguous (duplicate) full paths are rejected, so callers must use full
   * paths when a schema contains duplicate leaf names at different paths.
   */
  public int leafIndex(String[] path) {
    Objects.requireNonNull(path, "path");
    List<LeafNode> leaves = root.leaves();
    int found = -1;
    for (int i = 0; i < leaves.size(); i++) {
      if (SchemaNode.samePath(leaves.get(i).path(), path)) {
        if (found >= 0) {
          throw new ParquetException("Ambiguous column path: " + String.join(".", path));
        }
        found = i;
      }
    }
    if (found < 0) {
      throw new ParquetException("Column not found: " + String.join(".", path));
    }
    return found;
  }

  /** Resolves a leaf index from a dot-separated, case-sensitive full path. */
  public int leafIndex(String dottedPath) {
    return leafIndex(dottedPath.split("\\.", -1));
  }

  /** Resolves a leaf index from its physical column descriptor (by full path). */
  public int leafIndex(ColumnDescriptor descriptor) {
    return leafIndex(descriptor.path());
  }

  /**
   * Resolves the leaf carrying a name, matching either the exact full path or the leaf's
   * short name. Matching is case-sensitive; when several leaves share the short name this
   * throws instead of guessing — use {@link #leafIndex(String[])} with the full path.
   */
  public int leafIndexByName(String name) {
    List<LeafNode> leaves = root.leaves();
    int found = -1;
    for (int i = 0; i < leaves.size(); i++) {
      LeafNode leaf = leaves.get(i);
      if (leaf.name().equals(name) || String.join(".", leaf.path()).equals(name)) {
        if (found >= 0) {
          throw new ParquetException("Ambiguous column name '" + name
              + "'; use the full dot-separated path");
        }
        found = i;
      }
    }
    if (found < 0) {
      throw new ParquetException("Column not found: " + name);
    }
    return found;
  }

  /** Resolves any (group or leaf) node by its dot-separated, case-sensitive full path. */
  public SchemaNode node(String dottedPath) {
    return node(root, dottedPath.split("\\.", -1), 0);
  }

  private static SchemaNode node(SchemaNode current, String[] path, int depth) {
    if (depth == path.length) {
      return current;
    }
    for (SchemaNode child : current.children()) {
      if (child.name().equals(path[depth])) {
        return node(child, path, depth + 1);
      }
    }
    return null;
  }

  /** All leaf indexes whose full path starts with the given prefix, in schema order. */
  public List<Integer> leafIndexesByPathPrefix(String[] prefix) {
    Objects.requireNonNull(prefix, "prefix");
    List<Integer> out = new ArrayList<>();
    List<LeafNode> leaves = root.leaves();
    for (int i = 0; i < leaves.size(); i++) {
      List<String> path = leaves.get(i).path();
      if (path.size() >= prefix.length) {
        boolean match = true;
        for (int j = 0; j < prefix.length; j++) {
          if (!path.get(j).equals(prefix[j])) {
            match = false;
            break;
          }
        }
        if (match) {
          out.add(i);
        }
      }
    }
    return out;
  }

  /**
   * Finds the logical column with the given name. Matching is case-sensitive; when several
   * logical columns share a name, the first in schema order wins. A name that is not a
   * logical column name resolves as a schema tree path: when the addressed node holds
   * exactly one leaf, that leaf's logical column is returned (so a LIST/MAP/STRUCT container
   * addressable by name yields its single leaf column). Returns {@code null} when nothing
   * matches.
   */
  public LogicalColumnDescriptor getLogicalColumn(String name) {
    for (LogicalColumnDescriptor column : logicalColumns) {
      if (column.getName().equals(name)) {
        return column;
      }
    }
    SchemaNode resolved = node(name);
    if (resolved != null && resolved.leaves().size() == 1) {
      List<String> leafPath = resolved.leaves().get(0).path();
      for (LogicalColumnDescriptor column : logicalColumns) {
        if (column.node() != null && column.node().path().equals(leafPath)) {
          return column;
        }
      }
    }
    return null;
  }

  /** Projects this schema to the named logical columns (case-sensitive names). */
  public SchemaDescriptor project(List<String> names) {
    List<LogicalColumnDescriptor> projected = new ArrayList<>();
    List<SchemaNode> nodes = new ArrayList<>();
    for (String name : names) {
      LogicalColumnDescriptor column = getLogicalColumn(name);
      if (column == null) {
        throw new IllegalArgumentException("Unknown projection column: " + name);
      }
      projected.add(column);
      nodes.add(column.node());
    }
    List<ColumnDescriptor> projectedColumns = new ArrayList<>();
    for (SchemaNode node : nodes) {
      for (LeafNode leaf : node.leaves()) {
        projectedColumns.add(leaf.descriptor());
      }
    }
    GroupNode projectedRoot = new GroupNode(root.name(), List.of(), LogicalType.STRUCT, 0, 0,
        Repetition.REQUIRED, 0, 0, nodes);
    return new SchemaDescriptor(name, projectedColumns, projected, projectedRoot);
  }

  // ------------------------------------------------------------------ legacy factories

  /**
   * Creates a SchemaDescriptor from logical columns only, automatically deriving physical
   * columns and assigning map key/value physical indexes in schema order.
   */
  public static SchemaDescriptor fromLogicalColumns(String name,
                                                    List<LogicalColumnDescriptor> logicalColumns) {
    List<ColumnDescriptor> physicalColumns = new ArrayList<>();
    List<LogicalColumnDescriptor> updatedLogicalColumns = new ArrayList<>();

    for (LogicalColumnDescriptor logicalCol : logicalColumns) {
      if (logicalCol.isPrimitive() || logicalCol.isList()) {
        physicalColumns.addAll(logicalCol.getPhysicalColumns());
        updatedLogicalColumns.add(logicalCol);
      } else if (logicalCol.isMap()) {
        int keyColumnIndex = physicalColumns.size();
        int valueColumnIndex = keyColumnIndex + 1;

        MapMetadata oldMapMeta = logicalCol.getMapMetadata();
        MapMetadata newMapMeta = new MapMetadata(
            keyColumnIndex,
            valueColumnIndex,
            oldMapMeta.keyType(),
            oldMapMeta.valueType(),
            oldMapMeta.keyDescriptor(),
            oldMapMeta.valueDescriptor()
        );

        LogicalColumnDescriptor updatedLogicalCol = new LogicalColumnDescriptor(
            logicalCol.getName(),
            LogicalType.MAP,
            newMapMeta
        );
        physicalColumns.addAll(updatedLogicalCol.getPhysicalColumns());
        updatedLogicalColumns.add(updatedLogicalCol);
      } else {
        physicalColumns.addAll(logicalCol.getPhysicalColumns());
        updatedLogicalColumns.add(logicalCol);
      }
    }

    return new SchemaDescriptor(name, physicalColumns, updatedLogicalColumns);
  }

  /** Gets the number of physical columns in the schema. */
  public int getNumColumns() {
    return columns.size();
  }

  /** Gets a physical column descriptor by index. */
  public ColumnDescriptor getColumn(int index) {
    return columns.get(index);
  }

  @Override
  public List<LogicalColumnDescriptor> logicalColumns() {
    return logicalColumns;
  }

  /** Gets the number of logical columns in the schema. */
  public int getNumLogicalColumns() {
    return logicalColumns.size();
  }

  /** Gets a logical column descriptor by index. */
  public LogicalColumnDescriptor getLogicalColumn(int index) {
    return logicalColumns.get(index);
  }

  /** Checks if logical columns are defined in the schema. */
  public boolean hasLogicalColumns() {
    return !logicalColumns.isEmpty();
  }

  /**
   * Finds the logical column descriptor containing a given physical column index. MAP
   * columns own their key and value leaves; repeated/leaf columns own themselves. Leaves are
   * matched by descriptor identity first so duplicated paths resolve to the exact owner;
   * a path match is used only as a fallback for schemas whose descriptors are copies.
   */
  public LogicalColumnDescriptor findLogicalColumnByPhysicalIndex(int physicalColumnIndex) {
    if (physicalColumnIndex < 0 || physicalColumnIndex >= columns.size()) {
      return null;
    }
    ColumnDescriptor physicalDescriptor = columns.get(physicalColumnIndex);
    for (LogicalColumnDescriptor logicalCol : logicalColumns) {
      for (ColumnDescriptor leaf : logicalCol.getPhysicalColumns()) {
        if (leaf == physicalDescriptor) {
          return logicalCol;
        }
      }
    }
    for (LogicalColumnDescriptor logicalCol : logicalColumns) {
      for (ColumnDescriptor leaf : logicalCol.getPhysicalColumns()) {
        if (leaf != null && java.util.Arrays.equals(leaf.path(), physicalDescriptor.path())) {
          return logicalCol;
        }
      }
    }
    return null;
  }

  /** Creates logical columns from physical columns as top-level PRIMITIVE columns. */
  public static List<LogicalColumnDescriptor> createLogicalColumnsFromPhysical(
      List<ColumnDescriptor> columns) {
    return columns.stream()
        .map(col -> new LogicalColumnDescriptor(
            col.getPathString(),
            LogicalType.PRIMITIVE,
            col.physicalType(),
            col))
        .toList();
  }

  /**
   * Creates a MAP logical column with String keys and String values (annotated UTF8).
   */
  public static LogicalColumnDescriptor createStringMapColumn(String name, boolean optional) {
    return createStringMapColumn(name, optional, true);
  }

  /**
   * Creates a MAP logical column with String keys and String values (annotated UTF8), with
   * explicit control over value optionality.
   */
  public static LogicalColumnDescriptor createStringMapColumn(String name, boolean optional,
                                                              boolean valuesOptional) {
    return createMapColumn(name,
        new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {name, "key_value", "key"},
            optional ? 2 : 1, 1, 0, PrimitiveLogicalType.string()),
        new ColumnDescriptor(Type.BYTE_ARRAY, new String[] {name, "key_value", "value"},
            valuesOptional ? (optional ? 3 : 2) : (optional ? 2 : 1), 1, 0,
            PrimitiveLogicalType.string()),
        optional, valuesOptional);
  }

  /**
   * Creates a MAP logical column with specified key and value physical types (no annotations).
   */
  public static LogicalColumnDescriptor createMapColumn(
      String name, Type keyType, Type valueType, boolean optional, boolean valuesOptional) {
    return createMapColumn(name,
        new ColumnDescriptor(keyType, new String[] {name, "key_value", "key"}, 0, 1, 0),
        new ColumnDescriptor(valueType, new String[] {name, "key_value", "value"}, 0, 1, 0),
        optional, valuesOptional);
  }

  /**
   * Creates a MAP logical column from fully described key and value leaves. The standard
   * Parquet MAP structure is assumed: a repeated key_value group with a required key and an
   * optional/required value; definition and repetition levels are taken from the descriptors.
   */
  public static LogicalColumnDescriptor createMapColumn(
      String name, ColumnDescriptor keyDescriptor, ColumnDescriptor valueDescriptor,
      boolean optional, boolean valuesOptional) {
    Objects.requireNonNull(keyDescriptor, "keyDescriptor");
    Objects.requireNonNull(valueDescriptor, "valueDescriptor");
    int mapMaxDef = optional ? 1 : 0;
    int keyMaxDef = mapMaxDef + 1;
    int valueMaxDef = valuesOptional ? mapMaxDef + 2 : mapMaxDef + 1;
    int maxRep = 1;

    ColumnDescriptor key = new ColumnDescriptor(keyDescriptor.physicalType(),
        new String[] {name, "key_value", "key"}, keyMaxDef, maxRep,
        keyDescriptor.typeLength(), keyDescriptor.annotation());
    ColumnDescriptor value = new ColumnDescriptor(valueDescriptor.physicalType(),
        new String[] {name, "key_value", "value"}, valueMaxDef, maxRep,
        valueDescriptor.typeLength(), valueDescriptor.annotation());

    MapMetadata mapMetadata = new MapMetadata(-1, -1, key.physicalType(), value.physicalType(),
        key, value);
    return new LogicalColumnDescriptor(name, LogicalType.MAP, mapMetadata);
  }

  /**
   * Returns a string representation of the schema in Parquet schema notation, showing the
   * message name and all physical columns with their types and paths.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("message ").append(name).append(" {\n");
    for (ColumnDescriptor col : columns) {
      sb.append("  ").append(col).append("\n");
    }
    sb.append("}");
    return sb.toString();
  }
}
