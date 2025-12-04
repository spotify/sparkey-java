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
 * Microbenchmarks comparing MappedByteBuffer vs MemorySegment (Java 22+).
 * Direct API usage without reflection for accurate performance measurement.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MemoryAccessMicrobenchmarkJ22 {

  private File testFile;
  private MappedByteBuffer mappedBuffer;
  private MemorySegment memorySegment;
  private Arena arena;
  private Random random;
  private byte[] keyBytes;
  private byte[] comparisonBytes;

  private static final int FILE_SIZE = 10 * 1024 * 1024; // 10MB
  private static final int TINY_KEY_SIZE = 1;
  private static final int VERYSMALL_KEY_SIZE = 2;
  private static final int SMALLEST_KEY_SIZE = 4;
  private static final int SMALLER_KEY_SIZE = 8;
  private static final int SMALL_KEY_SIZE = 16;
  private static final int MEDIUM_KEY_SIZE = 128;
  private static final int LARGE_KEY_SIZE = 1024;
  private static final int XLARGE_KEY_SIZE = 4096;
  private static final int XXLARGE_KEY_SIZE = 16384;

  @Param({"TINY", "VERYSMALL", "SMALLEST", "SMALLER", "SMALL", "MEDIUM", "LARGE"})
  public String keySize;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    // Create test file with random data
    testFile = File.createTempFile("memory-bench", ".dat");
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

    random = new Random(67890);

    // Prepare key bytes for comparison tests
    int size = getKeySize();
    keyBytes = new byte[size];
    comparisonBytes = new byte[size];
    random.nextBytes(keyBytes);
    System.arraycopy(keyBytes, 0, comparisonBytes, 0, size);
  }

  private int getKeySize() {
    return switch (keySize) {
      case "TINY" -> TINY_KEY_SIZE;
      case "VERYSMALL" -> VERYSMALL_KEY_SIZE;
      case "SMALLEST" -> SMALLEST_KEY_SIZE;
      case "SMALLER" -> SMALLER_KEY_SIZE;
      case "SMALL" -> SMALL_KEY_SIZE;
      case "MEDIUM" -> MEDIUM_KEY_SIZE;
      case "LARGE" -> LARGE_KEY_SIZE;
      case "XLARGE" -> XLARGE_KEY_SIZE;
      case "XXLARGE" -> XXLARGE_KEY_SIZE;
      default -> throw new IllegalArgumentException("Unknown keySize: " + keySize);
    };
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (arena != null) {
      arena.close();
    }
    testFile.delete();
  }

  // ============================================================================
  // Single Byte Read Tests
  // ============================================================================

  @Benchmark
  public byte readByte_MappedByteBuffer() {
    int offset = random.nextInt(FILE_SIZE - 100);
    return mappedBuffer.get(offset);
  }

  @Benchmark
  public byte readByte_MemorySegment() {
    int offset = random.nextInt(FILE_SIZE - 100);
    return memorySegment.get(ValueLayout.JAVA_BYTE, offset);
  }

  // ============================================================================
  // Sequential Read Tests (8 bytes)
  // ============================================================================

  @Benchmark
  public long readSequential_MappedByteBuffer() {
    int offset = random.nextInt(FILE_SIZE - 100);
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result = (result << 8) | (mappedBuffer.get(offset + i) & 0xFF);
    }
    return result;
  }

  @Benchmark
  public long readSequential_MemorySegment() {
    int offset = random.nextInt(FILE_SIZE - 100);
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result = (result << 8) | (memorySegment.get(ValueLayout.JAVA_BYTE, offset + i) & 0xFF);
    }
    return result;
  }

  // ============================================================================
  // Byte Array Read Tests
  // ============================================================================

  @Benchmark
  public byte[] readArray_MappedByteBuffer() {
    int size = getKeySize();
    int offset = random.nextInt(FILE_SIZE - size);
    byte[] result = new byte[size];
    for (int i = 0; i < size; i++) {
      result[i] = mappedBuffer.get(offset + i);
    }
    return result;
  }

  @Benchmark
  public byte[] readArray_MemorySegment() {
    int size = getKeySize();
    int offset = random.nextInt(FILE_SIZE - size);
    byte[] result = new byte[size];
    MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE, offset, result, 0, size);
    return result;
  }

  // ============================================================================
  // Byte Comparison Tests
  // ============================================================================

  @Benchmark
  public boolean compareBytes_ByteByByte() {
    int size = getKeySize();
    for (int i = 0; i < size; i++) {
      if (keyBytes[i] != comparisonBytes[i]) {
        return false;
      }
    }
    return true;
  }

  @Benchmark
  public boolean compareBytes_Mismatch() {
    int size = getKeySize();
    MemorySegment seg1 = MemorySegment.ofArray(keyBytes).asSlice(0, size);
    MemorySegment seg2 = MemorySegment.ofArray(comparisonBytes).asSlice(0, size);
    return seg1.mismatch(seg2) == -1;
  }

  // ============================================================================
  // VLQ Read Tests
  // ============================================================================

  @Benchmark
  public int readVLQ_MappedByteBuffer() {
    int offset = random.nextInt(FILE_SIZE - 100);
    int result = 0;
    int shift = 0;
    byte b;
    do {
      b = mappedBuffer.get(offset++);
      result |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  @Benchmark
  public int readVLQ_MemorySegment() {
    int offset = random.nextInt(FILE_SIZE - 100);
    int result = 0;
    int shift = 0;
    byte b;
    do {
      b = memorySegment.get(ValueLayout.JAVA_BYTE, offset++);
      result |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }
}
