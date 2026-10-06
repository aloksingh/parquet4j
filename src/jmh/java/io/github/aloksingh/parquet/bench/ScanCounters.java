package io.github.aloksingh.parquet.bench;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/**
 * Instrumented per-operation counters, reported as JMH auxiliary counters
 * ({@code AuxCounters.Type.OPERATIONS}: one value per benchmark operation). Results therefore
 * show rows/op, bytes/op and output-bytes/op directly; rows/s and MB/s are derived as
 * {@code perOp / score} in the reports. Read counts come from these instrumented counters, not
 * from estimates. Per-iteration guard accumulators live in {@link GuardState} because JMH
 * auxiliary counters may only contain primitive fields.
 */
@AuxCounters(AuxCounters.Type.OPERATIONS)
@State(Scope.Thread)
public class ScanCounters {

  /** Logical rows touched by the measured operation (monotonic). */
  public long rowsProcessed;
  /** Input bytes the operation is charged with (file size per full scan). */
  public long bytesRead;
  /** Output bytes produced (write benchmarks: produced file size). */
  public long outputBytes;
  /** Row groups pruned by statistics (filter benchmark). */
  public long groupsPruned;
}
