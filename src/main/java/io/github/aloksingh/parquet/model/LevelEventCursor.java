package io.github.aloksingh.parquet.model;

import java.util.List;

/**
 * Level-event cursor over one leaf's decoded pages, independent of page
 * boundaries: a nested container's entries may span V1 pages, so container
 * assembly must walk row/level events, never page indexes. Physical values are
 * consumed only for events at the leaf's maximum definition level.
 */
final class LevelEventCursor {
  private final List<DecodedPage> pages;
  private final int maxDefinition;
  private int pageIndex;
  private int eventIndex;
  private int physicalIndex;

  LevelEventCursor(List<DecodedPage> pages, int maxDefinition) {
    this.pages = pages;
    this.maxDefinition = maxDefinition;
  }

  boolean hasNext() {
    while (pageIndex < pages.size() && eventIndex == pages.get(pageIndex).numValues()) {
      pageIndex++;
      eventIndex = 0;
      physicalIndex = 0;
    }
    return pageIndex < pages.size();
  }

  int definition() {
    return pages.get(pageIndex).definitionLevel(eventIndex);
  }

  int repetition() {
    return pages.get(pageIndex).repetitionLevel(eventIndex);
  }

  Object physicalValue() {
    return pages.get(pageIndex).physicalValue(physicalIndex);
  }

  void advance() {
    if (definition() == maxDefinition) physicalIndex++;
    eventIndex++;
  }
}
