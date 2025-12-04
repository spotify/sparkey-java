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
 * JMH benchmark comparing different Sparkey reader implementations.
 * Tests both uncompressed and compressed files.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)  // 3 iterations × 2 seconds = 6s warmup
@Measurement(iterations = 10, time = 2)  // 10 iterations × 2 seconds = 20s measurement
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReaderComparisonBenchmark {

  private File indexFile;
  private File logFile;
  private SparkeyReader reader;
  private Random random;

  // Pre-generated keys and expected values for validation without allocation overhead
  private String[] keys;
  private String[] expectedValues;

  // Memory segments for file locking
  private java.lang.foreign.Arena arena;
  private java.util.List<java.lang.foreign.MemorySegment> lockedSegments = new java.util.ArrayList<>();

  @Param({"100000"})  // 100K entries
  public int numElements;

  @Param({"NONE", "SNAPPY"})  // Uncompressed and Snappy
  public String compressionType;

  @Param({"SINGLE_THREADED_MMAP_JDK8", "POOLED_MMAP_JDK8", "UNCOMPRESSED_MEMORYSEGMENT_J22", "SINGLE_THREADED_MEMORYSEGMENT_J22", "POOLED_MEMORYSEGMENT_J22"})
  public String readerType;

  @Param({"0", "50"})  // 0 = small values (~6 bytes), 50 = large values (~56 bytes)
  public int valuePadding;

  @Setup(Level.Trial)
  public void setup(org.openjdk.jmh.infra.BenchmarkParams params) throws IOException {
    indexFile = File.createTempFile("sparkey-jmh", ".spi");
    logFile = Sparkey.getLogFile(indexFile);

    indexFile.deleteOnExit();
    logFile.deleteOnExit();
    UtilTest.delete(indexFile);
    UtilTest.delete(logFile);

    CompressionType compression = CompressionType.valueOf(compressionType);

    // Pre-generate keys and expected values for efficient validation
    keys = new String[numElements];
    expectedValues = new String[numElements];
    for (int i = 0; i < numElements; i++) {
      keys[i] = "key_" + i;
      expectedValues[i] = valuePadding > 0
          ? "value_" + i + "-" + "x".repeat(valuePadding)
          : "value_" + i;
    }

    // Create test file using pre-generated values
    try (SparkeyWriter writer = Sparkey.createNew(indexFile, compression, 1024)) {
      for (int i = 0; i < numElements; i++) {
        writer.put(keys[i], expectedValues[i]);
      }
      writer.writeHash();
    }

    // Open with the specified reader type
    try {
      ReaderType type = ReaderType.valueOf(readerType);
      if (!type.isAvailable()) {
        throw new RuntimeException("Reader type not available on this JVM: " + type);
      }
      if (!type.supports(compression)) {
        throw new RuntimeException("Reader type does not support compression: " + type + " with " + compression);
      }
      reader = type.open(indexFile);
      random = new Random(891273791623L);

      // Validate multithreading support for multithreaded benchmarks
      String benchmarkName = params.getBenchmark();
      if (benchmarkName.contains("Multithreaded") && !type.supportsMultithreading()) {
        throw new RuntimeException("Benchmark " + benchmarkName + " requires multithreading support, " +
            "but reader type " + type + " does not support it. Skipping this configuration.");
      }
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Unknown reader type: " + readerType + " (available: " +
        String.join(", ", java.util.Arrays.stream(ReaderType.values()).map(ReaderType::toString).toArray(String[]::new)) + ")");
    }

    // Check memory lock limits
    System.out.println("=== Memory Lock Configuration ===");
    try {
      long maxLocked = MemoryLock.getMaxLockedMemory();
      if (maxLocked == -1) {
        System.out.println("ulimit -l: unlimited");
      } else if (maxLocked == 0) {
        System.out.println("ulimit -l: could not determine (check manually with 'ulimit -l')");
      } else {
        System.out.println("ulimit -l: " + maxLocked + " bytes (" + (maxLocked / 1024 / 1024) + " MB)");
      }
    } catch (Throwable t) {
      // MemoryLock not available (Java < 22), skip silently
      System.out.println("MemoryLock not available (requires Java 22+)");
    }

    // Try to lock Sparkey files in memory
    try {
      lockSparkeyFiles();
    } catch (Throwable t) {
      // Memory locking failed - continue without locking
      System.out.println("Memory locking failed: " + t.getMessage());
      t.printStackTrace();
    }
  }

  /**
   * Memory-map and lock Sparkey files in RAM for consistent benchmark results.
   * Works for all reader types (MappedByteBuffer and MemorySegment).
   */
  private void lockSparkeyFiles() throws Exception {
    System.out.println("=== Locking Sparkey Files in Memory ===");

    // Create shared Arena to keep MemorySegments alive
    arena = java.lang.foreign.Arena.ofShared();

    // Map and lock index file (.spi)
    lockFile(indexFile, "index");

    // Map and lock log file (.spl)
    lockFile(logFile, "log");

    System.out.println("File locking complete");
  }

  /**
   * Memory-map a file and lock it in RAM.
   */
  private void lockFile(File file, String name) throws Exception {
    if (!file.exists()) {
      System.out.println("Skipping " + name + " file (does not exist): " + file);
      return;
    }

    long fileSize = file.length();
    System.out.println("Locking " + name + " file: " + file + " (" + (fileSize / 1024) + " KB)");

    // Open FileChannel and map file
    try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(
           file.toPath(), java.nio.file.StandardOpenOption.READ)) {

      // Map file as MemorySegment
      java.lang.foreign.MemorySegment segment = channel.map(
          java.nio.channels.FileChannel.MapMode.READ_ONLY,
          0,
          fileSize,
          arena);

      // Lock the MemorySegment in RAM
      boolean locked = MemoryLock.lock(segment);
      if (locked) {
        lockedSegments.add(segment);
        System.out.println("  Successfully locked " + name + " file");
      } else {
        System.out.println("  WARNING: Failed to lock " + name + " file");
        System.out.println("           Results may have higher variance due to page faults");
        System.out.println("           To enable: increase ulimit -l or run with CAP_IPC_LOCK");
      }
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    // Unlock memory segments before closing
    if (!lockedSegments.isEmpty()) {
      System.out.println("=== Unlocking Memory ===");
      for (Object segment : lockedSegments) {
        try {
          MemoryLock.unlock((java.lang.foreign.MemorySegment) segment);
        } catch (Throwable t) {
          System.err.println("Failed to unlock memory segment: " + t.getMessage());
        }
      }
      lockedSegments.clear();
    }

    // Close arena (which unmaps MemorySegments)
    if (arena != null) {
      try {
        arena.close();
        System.out.println("Closed Arena");
      } catch (Throwable t) {
        System.err.println("Failed to close Arena: " + t.getMessage());
      }
      arena = null;
    }

    reader.close();
    UtilTest.delete(indexFile);
    UtilTest.delete(logFile);
  }

  @Benchmark
  public String lookupRandom() throws IOException {
    int idx = random.nextInt(numElements);
    String result = reader.getAsString(keys[idx]);

    // Sampled validation: only validate ~1% of lookups to avoid measurement overhead
    if (random.nextInt(100) == 0) {
      if (!expectedValues[idx].equals(result)) {
        throw new AssertionError("Validation failed! Expected: " + expectedValues[idx] + ", got: " + result);
      }
    }
    return result;
  }

  /**
   * Multithreaded benchmark to test concurrent access performance.
   * This is particularly important for UNCOMPRESSED readers which are fully immutable
   * and don't need pooling overhead, vs SINGLE_THREADED which requires ThreadLocal pooling.
   *
   * Only tests NONE compression (uncompressed) as compressed multithreading follows the same patterns.
   */
  @Benchmark
  @Threads(8)  // Test with 8 concurrent threads
  public String lookupRandomMultithreaded8() throws IOException {
    java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
    int idx = rnd.nextInt(numElements);
    String result = reader.getAsString(keys[idx]);

    // Sampled validation: only validate ~1% of lookups to avoid measurement overhead
    if (rnd.nextInt(100) == 0) {
      if (!expectedValues[idx].equals(result)) {
        throw new AssertionError("Validation failed in multithreaded mode! Expected: " + expectedValues[idx] + ", got: " + result);
      }
    }
    return result;
  }

  @Benchmark
  @Threads(16)  // Test with 16 concurrent threads for higher contention
  public String lookupRandomMultithreaded16() throws IOException {
    java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
    int idx = rnd.nextInt(numElements);
    String result = reader.getAsString(keys[idx]);

    // Sampled validation: only validate ~1% of lookups to avoid measurement overhead
    if (rnd.nextInt(100) == 0) {
      if (!expectedValues[idx].equals(result)) {
        throw new AssertionError("Validation failed in multithreaded mode! Expected: " + expectedValues[idx] + ", got: " + result);
      }
    }
    return result;
  }

  @Benchmark
  @Threads(32)  // Test with 32 concurrent threads for extreme contention
  public String lookupRandomMultithreaded32() throws IOException {
    java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
    int idx = rnd.nextInt(numElements);
    String result = reader.getAsString(keys[idx]);

    // Sampled validation: only validate ~1% of lookups to avoid measurement overhead
    if (rnd.nextInt(100) == 0) {
      if (!expectedValues[idx].equals(result)) {
        throw new AssertionError("Validation failed in multithreaded mode! Expected: " + expectedValues[idx] + ", got: " + result);
      }
    }
    return result;
  }
}
