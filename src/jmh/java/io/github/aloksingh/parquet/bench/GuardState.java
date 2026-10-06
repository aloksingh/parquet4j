package io.github.aloksingh.parquet.bench;

import java.util.Arrays;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Per-iteration correctness guard accumulators.
 *
 * <p>Guards work at two levels. Every measured operation must reproduce the precomputed row
 * count and value checksum exactly ({@link #endOp}); a mismatch throws immediately, so an empty,
 * partial or broken scan fails the run instead of posting a fast time. In addition each
 * benchmark's {@code @TearDown(Level.Iteration)} asserts the iteration-level accounting
 * (total rows = operations x expected rows and the checksum of the last operation) against the
 * expectations, which were computed from the generated source values.
 *
 * <p>This state is {@code Scope.Benchmark}: JMH creates separate instances of
 * {@code Scope.Thread} states when they are injected both into benchmark methods and into other
 * states' lifecycle methods, which silently split the accumulators. Benchmarks are pinned to one
 * thread, so a single shared instance is correct. Kept separate from {@link ScanCounters} because
 * JMH auxiliary counters may only contain primitive fields.
 */
@State(Scope.Benchmark)
public class GuardState {

  /** Per-leaf checksum accumulators for the current operation. */
  public final BenchHash.LeafAccumulator guard = new BenchHash.LeafAccumulator(64);
  /** Rows accumulated in the current operation. */
  public long opRows;
  /** Rows accumulated this iteration (all operations). */
  public long guardRows;
  /** Operations completed this iteration. */
  public long ops;
  /** Checksum produced by the last completed operation. */
  public long lastChecksum;
  /** Row groups pruned this iteration (filter benchmark). */
  public long guardGroups;
  /** First-row checksum of the current operation (time-to-first-row benchmark). */
  public long guardFirstRow;

  @Setup(Level.Iteration)
  public void reset() {
    opRows = 0;
    guardRows = 0;
    ops = 0;
    lastChecksum = 0;
    guardGroups = 0;
    guardFirstRow = 0;
    Arrays.fill(guard.hashes, 0L);
  }

  /** Starts one measured operation: clears the per-operation accumulators. */
  public void beginOp() {
    opRows = 0;
    Arrays.fill(guard.hashes, 0L);
  }

  /**
   * Completes one measured scan operation and asserts its exact row count and checksum.
   *
   * @param expectedRows     rows the operation must have produced
   * @param expectedChecksum checksum the operation must have produced
   * @param actualChecksum   checksum the operation did produce
   * @param what             label for the failure message
   */
  public void endOp(long expectedRows, long expectedChecksum, long actualChecksum, String what) {
    ops++;
    guardRows += opRows;
    lastChecksum = actualChecksum;
    if (opRows != expectedRows || lastChecksum != expectedChecksum) {
      throw new AssertionError(what + ": scan produced rows=" + opRows + " (expected "
          + expectedRows + "), checksum=" + lastChecksum + " (expected " + expectedChecksum
          + ")");
    }
  }

  /** Completes one measured write operation (row accounting only; output size is checked inline). */
  public void endOpRows(long rowsThisOp, String what) {
    ops++;
    guardRows += rowsThisOp;
    if (rowsThisOp <= 0) {
      throw new AssertionError(what + ": write produced " + rowsThisOp + " rows");
    }
  }

  /** Iteration-level accounting assertion for scan benchmarks, run in @TearDown(Level.Iteration). */
  public void assertIteration(long expectedRows, long expectedChecksum, String what) {
    if (ops <= 0 || guardRows != ops * expectedRows || lastChecksum != expectedChecksum) {
      throw new AssertionError(what + ": iteration accounting failed: ops=" + ops + ", rows="
          + guardRows + " (expected " + ops + " x " + expectedRows + "), lastChecksum="
          + lastChecksum + " (expected " + expectedChecksum + ")");
    }
  }

  /** Iteration-level accounting assertion for write benchmarks. */
  public void assertIterationRows(long expectedRows, String what) {
    if (ops <= 0 || guardRows != ops * expectedRows) {
      throw new AssertionError(what + ": iteration accounting failed: ops=" + ops + ", rowsWritten="
          + guardRows + " (expected " + ops + " x " + expectedRows + ")");
    }
  }
}
