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
import com.spotify.sparkey.extra.PooledSparkeyReader;
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
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReaderComparisonBenchmark {

  private File indexFile;
  private File logFile;
  private SparkeyReader reader;
  private Random random;

  private String[] keys;
  private String[] expectedValues;

  private java.lang.foreign.Arena arena;
  private java.util.List<java.lang.foreign.MemorySegment> lockedSegments = new java.util.ArrayList<>();

  @Param({"100000"})
  public int numElements;

  @Param({"NONE", "SNAPPY"})
  public String compressionType;

  @Param({"SINGLE_THREADED_MMAP_JDK8", "POOLED_MMAP_JDK8"})
  public String readerType;

  @Param({"0", "50"})
  public int valuePadding;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    indexFile = File.createTempFile("sparkey-jmh", ".spi");
    logFile = Sparkey.getLogFile(indexFile);

    indexFile.deleteOnExit();
    logFile.deleteOnExit();
    UtilTest.delete(indexFile);
    UtilTest.delete(logFile);

    CompressionType compression = CompressionType.valueOf(compressionType);

    keys = new String[numElements];
    expectedValues = new String[numElements];
    for (int i = 0; i < numElements; i++) {
      keys[i] = "key_" + i;
      expectedValues[i] = valuePadding > 0
          ? "value_" + i + "-" + "x".repeat(valuePadding)
          : "value_" + i;
    }

    try (SparkeyWriter writer = Sparkey.createNew(indexFile, compression, 1024)) {
      for (int i = 0; i < numElements; i++) {
        writer.put(keys[i], expectedValues[i]);
      }
      writer.writeHash();
    }

    try {
      ReaderType type = ReaderType.valueOf(readerType);
      if (!type.isAvailable()) {
        throw new RuntimeException("Reader type not available: " + type);
      }
      if (!type.supports(compression)) {
        throw new RuntimeException("Reader type does not support compression: " + type + " with " + compression);
      }
      reader = type.open(indexFile);
      random = new Random(891273791623L);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Unknown reader type: " + readerType);
    }

    System.out.println("=== Memory Lock Configuration ===");
    try {
      long maxLocked = MemoryLock.getMaxLockedMemory();
      if (maxLocked == -1) {
        System.out.println("ulimit -l: unlimited");
      } else if (maxLocked == 0) {
        System.out.println("ulimit -l: could not determine");
      } else {
        System.out.println("ulimit -l: " + maxLocked + " bytes (" + (maxLocked / 1024 / 1024) + " MB)");
      }
    } catch (Throwable t) {
      System.out.println("MemoryLock check failed: " + t.getMessage());
    }

    try {
      lockSparkeyFiles();
    } catch (Throwable t) {
      System.out.println("Memory locking failed: " + t.getMessage());
    }
  }

  private void lockSparkeyFiles() throws Exception {
    System.out.println("=== Locking Sparkey Files in Memory ===");
    arena = java.lang.foreign.Arena.ofShared();
    lockFile(indexFile, "index");
    lockFile(logFile, "log");
    System.out.println("File locking complete");
  }

  private void lockFile(File file, String name) throws Exception {
    if (!file.exists()) {
      return;
    }

    long fileSize = file.length();
    System.out.println("Locking " + name + " file: " + file + " (" + (fileSize / 1024) + " KB)");

    try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(
           file.toPath(), java.nio.file.StandardOpenOption.READ)) {

      java.lang.foreign.MemorySegment segment = channel.map(
          java.nio.channels.FileChannel.MapMode.READ_ONLY,
          0,
          fileSize,
          arena);

      boolean locked = MemoryLock.lock(segment);
      if (locked) {
        lockedSegments.add(segment);
        System.out.println("  Successfully locked " + name + " file");
      } else {
        System.out.println("  WARNING: Failed to lock " + name + " file");
      }
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    if (!lockedSegments.isEmpty()) {
      System.out.println("=== Unlocking Memory ===");
      for (java.lang.foreign.MemorySegment segment : lockedSegments) {
        try {
          MemoryLock.unlock(segment);
        } catch (Throwable t) {
          System.err.println("Failed to unlock: " + t.getMessage());
        }
      }
      lockedSegments.clear();
    }

    if (arena != null) {
      try {
        arena.close();
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
    if (random.nextInt(100) == 0) {
      if (!expectedValues[idx].equals(result)) {
        throw new AssertionError("Validation failed!");
      }
    }
    return result;
  }

  @Benchmark
  @Threads(8)
  public String lookupRandomMultithreaded8() throws IOException {
    java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
    int idx = rnd.nextInt(numElements);
    String result = reader.getAsString(keys[idx]);
    if (rnd.nextInt(100) == 0) {
      if (!expectedValues[idx].equals(result)) {
        throw new AssertionError("Validation failed!");
      }
    }
    return result;
  }

  @Benchmark
  @Threads(16)
  public String lookupRandomMultithreaded16() throws IOException {
    java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
    int idx = rnd.nextInt(numElements);
    String result = reader.getAsString(keys[idx]);
    if (rnd.nextInt(100) == 0) {
      if (!expectedValues[idx].equals(result)) {
        throw new AssertionError("Validation failed!");
      }
    }
    return result;
  }

  @Benchmark
  @Threads(32)
  public String lookupRandomMultithreaded32() throws IOException {
    java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
    int idx = rnd.nextInt(numElements);
    String result = reader.getAsString(keys[idx]);
    if (rnd.nextInt(100) == 0) {
      if (!expectedValues[idx].equals(result)) {
        throw new AssertionError("Validation failed!");
      }
    }
    return result;
  }
}
