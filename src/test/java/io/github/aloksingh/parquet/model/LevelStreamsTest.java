package io.github.aloksingh.parquet.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Direct framing tests for V1 prefixed and V2 raw level sections.
 */
class LevelStreamsTest {

  /** One-value repeated runs (LSB 0), the same minimal framing the fixtures use. */
  private static ByteBuffer runs(int... values) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    for (int value : values) {
      output.write(2); // repeated run of one value
      output.write(value);
    }
    return ByteBuffer.wrap(output.toByteArray());
  }

  private static ByteBuffer v1Section(ByteBuffer levels) {
    ByteBuffer data = ByteBuffer.allocate(4 + levels.remaining()).order(ByteOrder.LITTLE_ENDIAN);
    data.putInt(levels.remaining()).put(levels.duplicate());
    return (ByteBuffer) data.flip();
  }

  @ParameterizedTest
  @MethodSource("validLevelSections")
  void readsV1AndV2Sections(int[] expected, int maxLevel) {
    ByteBuffer levels = runs(expected);
    assertArrayEquals(expected, LevelStreams.readLevels(levels, expected.length, maxLevel, "definition"));
    ByteBuffer v1 = v1Section(runs(expected));
    assertArrayEquals(expected,
        LevelStreams.readV1Levels(v1, v1.remaining(), expected.length, maxLevel, "definition"));
  }

  static Stream<Arguments> validLevelSections() {
    return Stream.of(
        Arguments.of(new int[]{2, 1, 2}, 2),
        Arguments.of(new int[]{1, 0, 1, 1}, 1),
        Arguments.of(new int[]{3, 3, 0}, 3));
  }

  @Test
  void zeroMaxLevelHasNoSection() {
    assertNull(LevelStreams.readLevels(null, 4, 0, "repetition"));
    assertNull(LevelStreams.readV1Levels(ByteBuffer.allocate(0), 0, 4, 0, "repetition"));
  }

  @Test
  void rejectsMismatchedV1SectionLength() {
    // 8-byte section whose 4-byte prefix declares 5 payload bytes: 4 + 5 != 8.
    ByteBuffer v1 = ByteBuffer.wrap(new byte[]{5, 0, 0, 0, 2, 1, 2, 1})
        .order(ByteOrder.LITTLE_ENDIAN);
    ParquetException failure = assertThrows(ParquetException.class,
        () -> LevelStreams.readV1Levels(v1, v1.remaining(), 2, 1, "definition"));
    assertEquals("Invalid definition level section length: 5/8", failure.getMessage());
  }

  @Test
  void rejectsLevelsAboveTheMaximum() {
    ParquetException failure = assertThrows(ParquetException.class,
        () -> LevelStreams.readLevels(runs(3), 1, 2, "definition"));
    assertEquals("Invalid definition level 3, maximum 2", failure.getMessage());
  }

  @Test
  void rejectsTruncatedRunsInsteadOfZeroFilling() {
    ByteBuffer truncated = ByteBuffer.wrap(new byte[]{0x04}); // repeated run of 2, value missing
    assertThrows(ParquetException.class,
        () -> LevelStreams.readLevels(truncated, 2, 1, "definition"));
  }

  @Test
  void rejectsTrailingRunBytes() {
    ByteBuffer extra = ByteBuffer.wrap(new byte[]{2, 1, 2, 1});
    assertThrows(ParquetException.class,
        () -> LevelStreams.readLevels(extra, 1, 1, "definition"));
  }

  @Test
  void rejectsRunsExceedingTheCount() {
    // Repeated run of 4 values for a 2-value section.
    ByteBuffer tooMany = ByteBuffer.wrap(new byte[]{8, 1});
    ParquetException failure = assertThrows(ParquetException.class,
        () -> LevelStreams.readLevels(tooMany, 2, 1, "definition"));
    assertEquals("Too many definition levels values", failure.getMessage());
  }
}
