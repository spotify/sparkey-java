package com.spotify.sparkey;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

import static com.spotify.sparkey.SortHelper.ENTRY_COMPARATOR;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Measurement(iterations = 5, time = 10)
@Warmup(iterations = 5, time = 10)
public class SortHelperBenchmark {
  private static final SortHelper.Entry E1 = new SortHelper.Entry(123, 456, 1);
  private static final SortHelper.Entry E2 = new SortHelper.Entry(123, 456, 2);
  private static final SortHelper.Entry E3 = new SortHelper.Entry(7567, 222, 1);
  private static final SortHelper.Entry E4 = new SortHelper.Entry(7567, 222, 2);

  @Benchmark
  public int measureRealE1_E1() {
    return ENTRY_COMPARATOR.compare(E1, E1);
  }

  @Benchmark
  public int measureRealE1_E2() {
    return ENTRY_COMPARATOR.compare(E1, E2);
  }

  @Benchmark
  public int measureRealE2_E1() {
    return ENTRY_COMPARATOR.compare(E2, E2);
  }

  @Benchmark
  public int measureRealE1_E3() {
    return ENTRY_COMPARATOR.compare(E1, E3);
  }

  @Benchmark
  public int measureRealE1_E4() {
    return ENTRY_COMPARATOR.compare(E1, E4);
  }

}
