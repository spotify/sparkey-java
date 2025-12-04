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
 */
public class FocusedReaderBenchmark {

  private static final int NUM_ELEMENTS = 100_000;

  // =============================================================================
  // 1. UNCOMPRESSED SINGLE-THREADED: J8 vs J22 implementations
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public static class UncompressedSingleThreaded {
    @Param({"SINGLE_THREADED_MMAP_JDK8", "UNCOMPRESSED_MEMORYSEGMENT_J22", "SINGLE_THREADED_MEMORYSEGMENT_J22"})
    public String readerType;

    private File indexFile;
    private SparkeyReader reader;
    private Random random;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      indexFile = File.createTempFile("sparkey-bench-uncompressed-st", ".spi");
      File logFile = Sparkey.getLogFile(indexFile);
      indexFile.deleteOnExit();
      logFile.deleteOnExit();
      UtilTest.delete(indexFile);
      UtilTest.delete(logFile);

      // Small values (size 0 padding)
      keys = new String[NUM_ELEMENTS];
      try (SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024)) {
        for (int i = 0; i < NUM_ELEMENTS; i++) {
          keys[i] = "key_" + i;
          writer.put(keys[i], "value_" + i);
        }
        writer.writeHash();
      }

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
      int index = random.nextInt(NUM_ELEMENTS);
      return reader.getAsString(keys[index]);
    }
  }

  // =============================================================================
  // 2. UNCOMPRESSED MULTI-THREADED: Immutable vs Pooled (8, 16 threads)
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(8)
  public static class UncompressedMultithreaded8 {
    @Param({"POOLED_MMAP_JDK8", "UNCOMPRESSED_MEMORYSEGMENT_J22", "POOLED_MEMORYSEGMENT_J22"})
    public String readerType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      indexFile = File.createTempFile("sparkey-bench-uncompressed-mt8", ".spi");
      File logFile = Sparkey.getLogFile(indexFile);
      indexFile.deleteOnExit();
      logFile.deleteOnExit();
      UtilTest.delete(indexFile);
      UtilTest.delete(logFile);

      keys = new String[NUM_ELEMENTS];
      try (SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024)) {
        for (int i = 0; i < NUM_ELEMENTS; i++) {
          keys[i] = "key_" + i;
          writer.put(keys[i], "value_" + i);
        }
        writer.writeHash();
      }

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
      int index = state.random.nextInt(NUM_ELEMENTS);
      return reader.getAsString(keys[index]);
    }
  }

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(16)
  public static class UncompressedMultithreaded16 {
    @Param({"POOLED_MMAP_JDK8", "UNCOMPRESSED_MEMORYSEGMENT_J22", "POOLED_MEMORYSEGMENT_J22"})
    public String readerType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      indexFile = File.createTempFile("sparkey-bench-uncompressed-mt16", ".spi");
      File logFile = Sparkey.getLogFile(indexFile);
      indexFile.deleteOnExit();
      logFile.deleteOnExit();
      UtilTest.delete(indexFile);
      UtilTest.delete(logFile);

      keys = new String[NUM_ELEMENTS];
      try (SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024)) {
        for (int i = 0; i < NUM_ELEMENTS; i++) {
          keys[i] = "key_" + i;
          writer.put(keys[i], "value_" + i);
        }
        writer.writeHash();
      }

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
      int index = state.random.nextInt(NUM_ELEMENTS);
      return reader.getAsString(keys[index]);
    }
  }

  // =============================================================================
  // 3. COMPRESSED MULTI-THREADED: J8 vs J22 with compression (8, 16 threads)
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(8)
  public static class CompressedMultithreaded8 {
    @Param({"POOLED_MMAP_FORCE_JDK8"})
    public String readerType;

    @Param({"SNAPPY", "ZSTD"})
    public String compressionType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      indexFile = File.createTempFile("sparkey-bench-compressed-mt8", ".spi");
      File logFile = Sparkey.getLogFile(indexFile);
      indexFile.deleteOnExit();
      logFile.deleteOnExit();
      UtilTest.delete(indexFile);
      UtilTest.delete(logFile);

      CompressionType compression = CompressionType.valueOf(compressionType);
      keys = new String[NUM_ELEMENTS];

      // Values with repetition for better compression (size ~56 bytes with repetition)
      try (SparkeyWriter writer = Sparkey.createNew(indexFile, compression, 1024)) {
        for (int i = 0; i < NUM_ELEMENTS; i++) {
          keys[i] = "key_" + i;
          String value = "valuevaluevalue_" + i + "_" + "x".repeat(50);
          writer.put(keys[i], value);
        }
        writer.writeHash();
      }

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
      int index = state.random.nextInt(NUM_ELEMENTS);
      return reader.getAsString(keys[index]);
    }
  }

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(16)
  public static class CompressedMultithreaded16 {
    @Param({"POOLED_MMAP_FORCE_JDK8"})
    public String readerType;

    @Param({"SNAPPY", "ZSTD"})
    public String compressionType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      indexFile = File.createTempFile("sparkey-bench-compressed-mt16", ".spi");
      File logFile = Sparkey.getLogFile(indexFile);
      indexFile.deleteOnExit();
      logFile.deleteOnExit();
      UtilTest.delete(indexFile);
      UtilTest.delete(logFile);

      CompressionType compression = CompressionType.valueOf(compressionType);
      keys = new String[NUM_ELEMENTS];

      // Values with repetition for better compression
      try (SparkeyWriter writer = Sparkey.createNew(indexFile, compression, 1024)) {
        for (int i = 0; i < NUM_ELEMENTS; i++) {
          keys[i] = "key_" + i;
          String value = "valuevaluevalue_" + i + "_" + "x".repeat(50);
          writer.put(keys[i], value);
        }
        writer.writeHash();
      }

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
      int index = state.random.nextInt(NUM_ELEMENTS);
      return reader.getAsString(keys[index]);
    }
  }

  // =============================================================================
  // 4. STRESS TEST: High contention (32 threads, uncompressed only)
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Threads(32)
  public static class StressTest32 {
    @Param({"POOLED_MMAP_JDK8", "UNCOMPRESSED_MEMORYSEGMENT_J22", "POOLED_MEMORYSEGMENT_J22"})
    public String readerType;

    private File indexFile;
    private SparkeyReader reader;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      indexFile = File.createTempFile("sparkey-bench-stress32", ".spi");
      File logFile = Sparkey.getLogFile(indexFile);
      indexFile.deleteOnExit();
      logFile.deleteOnExit();
      UtilTest.delete(indexFile);
      UtilTest.delete(logFile);

      keys = new String[NUM_ELEMENTS];
      try (SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024)) {
        for (int i = 0; i < NUM_ELEMENTS; i++) {
          keys[i] = "key_" + i;
          writer.put(keys[i], "value_" + i);
        }
        writer.writeHash();
      }

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
      int index = state.random.nextInt(NUM_ELEMENTS);
      return reader.getAsString(keys[index]);
    }
  }

  // =============================================================================
  // 5. VALUE SIZE COMPARISON: How do different value sizes perform? (single-threaded)
  // =============================================================================

  @State(Scope.Benchmark)
  @Warmup(iterations = 3, time = 2)
  @Measurement(iterations = 10, time = 2)
  @Fork(1)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public static class ValueSizeComparison {
    @Param({"SINGLE_THREADED_MMAP_JDK8", "UNCOMPRESSED_MEMORYSEGMENT_J22"})
    public String readerType;

    @Param({"0", "50", "1000"})
    public int valuePadding;

    private File indexFile;
    private SparkeyReader reader;
    private Random random;
    private String[] keys;

    @Setup(Level.Trial)
    public void setup() throws IOException {
      indexFile = File.createTempFile("sparkey-bench-valuesize", ".spi");
      File logFile = Sparkey.getLogFile(indexFile);
      indexFile.deleteOnExit();
      logFile.deleteOnExit();
      UtilTest.delete(indexFile);
      UtilTest.delete(logFile);

      keys = new String[NUM_ELEMENTS];
      try (SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024)) {
        for (int i = 0; i < NUM_ELEMENTS; i++) {
          keys[i] = "key_" + i;
          String value = valuePadding > 0
              ? "value_" + i + "-" + "x".repeat(valuePadding)
              : "value_" + i;
          writer.put(keys[i], value);
        }
        writer.writeHash();
      }

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
      int index = random.nextInt(NUM_ELEMENTS);
      return reader.getAsString(keys[index]);
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
