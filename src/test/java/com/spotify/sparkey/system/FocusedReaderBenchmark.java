/*
 * Copyright (c) 2025 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spotify.sparkey.system;

import com.spotify.sparkey.*;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Focused benchmarks comparing Sparkey reader implementations.
 * Split into specific scenarios to avoid combinatorial explosion.
 *
 * Each inner class validates that the reader type is compatible with the
 * scenario (e.g., multithreaded benchmarks require thread-safe readers).
 * Incompatible combinations are skipped with a clear message.
 */
public class FocusedReaderBenchmark {

  private static final int NUM_ELEMENTS = 100_000;

  private static ReaderType resolveAndValidate(String readerType, boolean requireMultithreading) {
    ReaderType type = ReaderType.valueOf(readerType);
    if (!type.isAvailable()) {
      throw new IllegalStateException("SKIP: Reader type not available on this JVM: " + type);
    }
    if (requireMultithreading && !type.supportsMultithreading()) {
      throw new IllegalStateException("SKIP: " + type + " does not support multithreading");
    }
    return type;
  }

  private static File createTestFile(CompressionType compression, String[] keys, int valuePadding, String prefix) throws IOException {
    File indexFile = File.createTempFile(prefix, ".spi");
    File logFile = Sparkey.getLogFile(indexFile);
    indexFile.deleteOnExit();
    logFile.deleteOnExit();
    UtilTest.delete(indexFile);
    UtilTest.delete(logFile);

    try (SparkeyWriter writer = Sparkey.createNew(indexFile, compression, 1024)) {
      for (int i = 0; i < keys.length; i++) {
        keys[i] = "key_" + i;
        String value = valuePadding > 0
            ? "value_" + i + "-" + "x".repeat(valuePadding)
            : "value_" + i;
        writer.put(keys[i], value);
      }
      writer.writeHash();
    }
    return indexFile;
  }

  // =============================================================================
  // 1. UNCOMPRESSED SINGLE-THREADED
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public static class UncompressedSingleThreaded {
    @Param({"SINGLE_THREADED_MMAP_JDK8", "SINGLE_THREADED_HEAP",
            "UNCOMPRESSED_MEMORYSEGMENT_J22", "SINGLE_THREADED_MEMORYSEGMENT_J22"})
    public String readerType;

    private File indexFile;
    private SparkeyReader reader;
    private Random random;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      resolveAndValidate(readerType, false);
      keys = new String[NUM_ELEMENTS];
      indexFile = createTestFile(CompressionType.NONE, keys, 0, "sparkey-bench-st");
      reader = ReaderType.valueOf(readerType).open(indexFile);
      random = new Random(12345);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      if (reader != null) reader.close();
      UtilTest.delete(indexFile);
      UtilTest.delete(Sparkey.getLogFile(indexFile));
    }

    @Benchmark
    public String lookup() throws IOException {
      return reader.getAsString(keys[random.nextInt(NUM_ELEMENTS)]);
    }
  }

  // =============================================================================
  // 2. UNCOMPRESSED MULTI-THREADED (8, 16 threads)
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(8)
  public static class UncompressedMultithreaded8 {
    @Param({"POOLED_MMAP_JDK8", "POOLED_HEAP", "UNCOMPRESSED_MEMORYSEGMENT_J22"})
    public String readerType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      resolveAndValidate(readerType, true);
      keys = new String[NUM_ELEMENTS];
      indexFile = createTestFile(CompressionType.NONE, keys, 0, "sparkey-bench-mt8");
      reader = ReaderType.valueOf(readerType).open(indexFile);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      if (reader != null) reader.close();
      UtilTest.delete(indexFile);
      UtilTest.delete(Sparkey.getLogFile(indexFile));
    }

    @Benchmark
    public String lookup(ThreadState state) throws IOException {
      return reader.getAsString(keys[state.random.nextInt(NUM_ELEMENTS)]);
    }
  }

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(16)
  public static class UncompressedMultithreaded16 {
    @Param({"POOLED_MMAP_JDK8", "POOLED_HEAP", "UNCOMPRESSED_MEMORYSEGMENT_J22"})
    public String readerType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      resolveAndValidate(readerType, true);
      keys = new String[NUM_ELEMENTS];
      indexFile = createTestFile(CompressionType.NONE, keys, 0, "sparkey-bench-mt16");
      reader = ReaderType.valueOf(readerType).open(indexFile);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      if (reader != null) reader.close();
      UtilTest.delete(indexFile);
      UtilTest.delete(Sparkey.getLogFile(indexFile));
    }

    @Benchmark
    public String lookup(ThreadState state) throws IOException {
      return reader.getAsString(keys[state.random.nextInt(NUM_ELEMENTS)]);
    }
  }

  // =============================================================================
  // 3. COMPRESSED MULTI-THREADED (8, 16 threads)
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(8)
  public static class CompressedMultithreaded8 {
    @Param({"POOLED_MMAP_FORCE_JDK8", "POOLED_HEAP"})
    public String readerType;

    @Param({"SNAPPY", "ZSTD"})
    public String compressionType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      resolveAndValidate(readerType, true);
      keys = new String[NUM_ELEMENTS];
      indexFile = createTestFile(CompressionType.valueOf(compressionType), keys, 50, "sparkey-bench-cmp-mt8");
      reader = ReaderType.valueOf(readerType).open(indexFile);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      if (reader != null) reader.close();
      UtilTest.delete(indexFile);
      UtilTest.delete(Sparkey.getLogFile(indexFile));
    }

    @Benchmark
    public String lookup(ThreadState state) throws IOException {
      return reader.getAsString(keys[state.random.nextInt(NUM_ELEMENTS)]);
    }
  }

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(16)
  public static class CompressedMultithreaded16 {
    @Param({"POOLED_MMAP_FORCE_JDK8", "POOLED_HEAP"})
    public String readerType;

    @Param({"SNAPPY", "ZSTD"})
    public String compressionType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      resolveAndValidate(readerType, true);
      keys = new String[NUM_ELEMENTS];
      indexFile = createTestFile(CompressionType.valueOf(compressionType), keys, 50, "sparkey-bench-cmp-mt16");
      reader = ReaderType.valueOf(readerType).open(indexFile);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      if (reader != null) reader.close();
      UtilTest.delete(indexFile);
      UtilTest.delete(Sparkey.getLogFile(indexFile));
    }

    @Benchmark
    public String lookup(ThreadState state) throws IOException {
      return reader.getAsString(keys[state.random.nextInt(NUM_ELEMENTS)]);
    }
  }

  // =============================================================================
  // 4. STRESS TEST: High contention (32 threads, uncompressed)
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(32)
  public static class StressTest32 {
    @Param({"POOLED_MMAP_JDK8", "POOLED_HEAP", "UNCOMPRESSED_MEMORYSEGMENT_J22"})
    public String readerType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      resolveAndValidate(readerType, true);
      keys = new String[NUM_ELEMENTS];
      indexFile = createTestFile(CompressionType.NONE, keys, 0, "sparkey-bench-stress32");
      reader = ReaderType.valueOf(readerType).open(indexFile);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      if (reader != null) reader.close();
      UtilTest.delete(indexFile);
      UtilTest.delete(Sparkey.getLogFile(indexFile));
    }

    @Benchmark
    public String lookup(ThreadState state) throws IOException {
      return reader.getAsString(keys[state.random.nextInt(NUM_ELEMENTS)]);
    }
  }

  // =============================================================================
  // 5. VALUE SIZE COMPARISON (single-threaded)
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public static class ValueSizeComparison {
    @Param({"SINGLE_THREADED_MMAP_JDK8", "SINGLE_THREADED_HEAP", "UNCOMPRESSED_MEMORYSEGMENT_J22"})
    public String readerType;

    @Param({"0", "50", "1000"})
    public int valuePadding;

    private File indexFile;
    private SparkeyReader reader;
    private Random random;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      resolveAndValidate(readerType, false);
      keys = new String[NUM_ELEMENTS];
      indexFile = createTestFile(CompressionType.NONE, keys, valuePadding, "sparkey-bench-valuesize");
      reader = ReaderType.valueOf(readerType).open(indexFile);
      random = new Random(12345);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
      if (reader != null) reader.close();
      UtilTest.delete(indexFile);
      UtilTest.delete(Sparkey.getLogFile(indexFile));
    }

    @Benchmark
    public String lookup() throws IOException {
      return reader.getAsString(keys[random.nextInt(NUM_ELEMENTS)]);
    }
  }

  // =============================================================================
  // Thread-local random state
  // =============================================================================

  @State(Scope.Thread)
  public static class ThreadState {
    Random random;

    @Setup(Level.Trial)
    public void setup() {
      random = new Random(Thread.currentThread().getId());
    }
  }
}
