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

import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Interleaved benchmark comparing MappedByteBuffer vs MemorySegment.
 * JMH randomizes parameter combinations to eliminate ordering bias from cache effects.
 */
@State(Scope.Thread)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class InterleavedMemoryBenchmark {

  @Param({"MAPPED", "SEGMENT"})
  private String implType;

  @Param({"1", "8", "16", "128", "1024"})
  private int size;

  private File testFile;
  private MappedByteBuffer mappedBuffer;
  private MemorySegment memorySegment;
  private Arena arena;
  private Random random;

  private static final int FILE_SIZE = 10 * 1024 * 1024; // 10MB

  @Setup(Level.Trial)
  public void setup() throws Exception {
    // Check memory lock limits
    System.out.println("=== Memory Lock Configuration ===");
    long maxLocked = MemoryLock.getMaxLockedMemory();
    if (maxLocked == -1) {
      System.out.println("ulimit -l: unlimited");
    } else if (maxLocked == 0) {
      System.out.println("ulimit -l: could not determine (check manually with 'ulimit -l')");
    } else {
      System.out.println("ulimit -l: " + (maxLocked / 1024) + " KB");
      if (maxLocked < FILE_SIZE) {
        System.out.println("WARNING: ulimit too low to lock " + (FILE_SIZE / 1024) + " KB");
        System.out.println("         Consider running: ulimit -l " + ((FILE_SIZE / 1024) + 1024));
      }
    }
    System.out.println();

    // Create test file with random data
    testFile = File.createTempFile("interleaved-bench", ".dat");
    testFile.deleteOnExit();

    try (RandomAccessFile raf = new RandomAccessFile(testFile, "rw")) {
      raf.setLength(FILE_SIZE);
      byte[] randomData = new byte[FILE_SIZE];
      new Random(12345).nextBytes(randomData);
      raf.write(randomData);
    }

    // Map file with MappedByteBuffer
    try (RandomAccessFile raf = new RandomAccessFile(testFile, "r");
         FileChannel channel = raf.getChannel()) {
      mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, FILE_SIZE);
    }

    // Map file with MemorySegment
    arena = Arena.ofShared();
    try (RandomAccessFile raf = new RandomAccessFile(testFile, "r");
         FileChannel channel = raf.getChannel()) {
      memorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, FILE_SIZE, arena);
    }

    // Lock the MemorySegment in RAM to prevent page faults
    System.out.println("=== Locking Memory ===");
    boolean locked = MemoryLock.lock(memorySegment);
    if (!locked) {
      System.out.println("WARNING: Failed to lock memory. Results may have higher variance.");
      System.out.println("         To enable: sudo setcap cap_ipc_lock=+ep $(which java)");
      System.out.println("         Or run: sudo sysctl -w vm.max_map_count=262144");
    }
    System.out.println();

    random = new Random(67890);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    // Unlock memory before closing
    if (memorySegment != null) {
      MemoryLock.unlock(memorySegment);
    }

    if (arena != null) {
      arena.close();
    }
    if (testFile != null) {
      testFile.delete();
    }
  }

  /**
   * Single byte read - both implementations compete for same cache
   */
  @Benchmark
  public byte readByte() {
    int offset = random.nextInt(FILE_SIZE - 100);
    return switch (implType) {
      case "MAPPED" -> mappedBuffer.get(offset);
      case "SEGMENT" -> memorySegment.get(ValueLayout.JAVA_BYTE, offset);
      default -> throw new IllegalStateException("Unknown impl: " + implType);
    };
  }

  /**
   * Array read - both implementations compete for same cache
   */
  @Benchmark
  public byte[] readArray() {
    int offset = random.nextInt(FILE_SIZE - size);
    byte[] result = new byte[size];

    switch (implType) {
      case "MAPPED" -> {
        for (int i = 0; i < size; i++) {
          result[i] = mappedBuffer.get(offset + i);
        }
      }
      case "SEGMENT" -> MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE, offset, result, 0, size);
      default -> throw new IllegalStateException("Unknown impl: " + implType);
    }

    return result;
  }

  /**
   * Sequential 8-byte read - both implementations compete for same cache
   */
  @Benchmark
  public long readSequential() {
    int offset = random.nextInt(FILE_SIZE - 100);
    long result = 0;

    switch (implType) {
      case "MAPPED" -> {
        for (int i = 0; i < 8; i++) {
          result = (result << 8) | (mappedBuffer.get(offset + i) & 0xFF);
        }
      }
      case "SEGMENT" -> {
        for (int i = 0; i < 8; i++) {
          result = (result << 8) | (memorySegment.get(ValueLayout.JAVA_BYTE, offset + i) & 0xFF);
        }
      }
      default -> throw new IllegalStateException("Unknown impl: " + implType);
    }

    return result;
  }

  /**
   * VLQ read pattern - both implementations compete for same cache
   */
  @Benchmark
  public int readVLQ() {
    int offset = random.nextInt(FILE_SIZE - 100);
    int result = 0;
    int shift = 0;
    byte b;

    switch (implType) {
      case "MAPPED" -> {
        do {
          b = mappedBuffer.get(offset++);
          result |= (b & 0x7F) << shift;
          shift += 7;
        } while ((b & 0x80) != 0);
      }
      case "SEGMENT" -> {
        do {
          b = memorySegment.get(ValueLayout.JAVA_BYTE, offset++);
          result |= (b & 0x7F) << shift;
          shift += 7;
        } while ((b & 0x80) != 0);
      }
      default -> throw new IllegalStateException("Unknown impl: " + implType);
    }

    return result;
  }
}
