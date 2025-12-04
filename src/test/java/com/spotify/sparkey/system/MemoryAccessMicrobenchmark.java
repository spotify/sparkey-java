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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Microbenchmarks to understand MappedByteBuffer vs MemorySegment performance.
 * Tests isolated operations to identify specific bottlenecks.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MemoryAccessMicrobenchmark {

  private File testFile;
  private MappedByteBuffer mappedBuffer;
  private Object memorySegment; // MemorySegment for Java 22+
  private Object arena; // Arena for Java 22+
  private Random random;
  private byte[] keyBytes;
  private byte[] comparisonBytes;

  private static final int FILE_SIZE = 10 * 1024 * 1024; // 10MB
  private static final int SMALL_KEY_SIZE = 16;
  private static final int MEDIUM_KEY_SIZE = 128;
  private static final int LARGE_KEY_SIZE = 1024;
  private static final int XLARGE_KEY_SIZE = 4096;
  private static final int XXLARGE_KEY_SIZE = 16384;

  @Param({"SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE"})
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

    // Map file with MemorySegment (Java 22+) using reflection
    if (isJava22OrLater()) {
      try {
        // Use FileChannel.map() to create MemorySegment directly
        Class<?> arenaClass = Class.forName("java.lang.foreign.Arena");
        Class<?> mapModeClass = Class.forName("java.nio.channels.FileChannel$MapMode");

        // Create Arena
        java.lang.reflect.Method ofSharedMethod = arenaClass.getMethod("ofShared");
        arena = ofSharedMethod.invoke(null);

        // Map file to MemorySegment
        try (RandomAccessFile raf = new RandomAccessFile(testFile, "r");
             FileChannel channel = raf.getChannel()) {
          java.lang.reflect.Method mapMethod = channel.getClass().getMethod(
              "map", mapModeClass, long.class, long.class, arenaClass);
          Object readOnlyMode = mapModeClass.getField("READ_ONLY").get(null);
          memorySegment = mapMethod.invoke(channel, readOnlyMode, 0L, (long) FILE_SIZE, arena);
        }
      } catch (Exception e) {
        System.err.println("Failed to create MemorySegment: " + e.getMessage());
        e.printStackTrace();
      }
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
    switch (keySize) {
      case "SMALL": return SMALL_KEY_SIZE;
      case "MEDIUM": return MEDIUM_KEY_SIZE;
      case "LARGE": return LARGE_KEY_SIZE;
      case "XLARGE": return XLARGE_KEY_SIZE;
      case "XXLARGE": return XXLARGE_KEY_SIZE;
      default: throw new IllegalArgumentException("Unknown keySize: " + keySize);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    if (arena != null) {
      arena.getClass().getMethod("close").invoke(arena);
    }
    testFile.delete();
  }

  private boolean isJava22OrLater() {
    try {
      Class.forName("com.spotify.sparkey.ReadOnlyMemMapJ22");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
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
  public byte readByte_MemorySegment() throws Exception {
    if (memorySegment == null) {
      throw new UnsupportedOperationException("MemorySegment not available");
    }
    int offset = random.nextInt(FILE_SIZE - 100);
    // Use MemorySegment.get(ValueLayout.JAVA_BYTE, offset)
    Class<?> valueLayoutClass = Class.forName("java.lang.foreign.ValueLayout");
    Object javaByteLayout = valueLayoutClass.getField("JAVA_BYTE").get(null);
    java.lang.reflect.Method getMethod = memorySegment.getClass().getMethod("get", valueLayoutClass, long.class);
    return (byte) getMethod.invoke(memorySegment, javaByteLayout, (long) offset);
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
  public long readSequential_MemorySegment() throws Exception {
    if (memorySegment == null) {
      throw new UnsupportedOperationException("MemorySegment not available");
    }
    int offset = random.nextInt(FILE_SIZE - 100);
    // Use MemorySegment.get(ValueLayout.JAVA_BYTE, offset)
    Class<?> valueLayoutClass = Class.forName("java.lang.foreign.ValueLayout");
    Object javaByteLayout = valueLayoutClass.getField("JAVA_BYTE").get(null);
    java.lang.reflect.Method getMethod = memorySegment.getClass().getMethod("get", valueLayoutClass, long.class);
    long result = 0;
    for (int i = 0; i < 8; i++) {
      byte b = (byte) getMethod.invoke(memorySegment, javaByteLayout, (long) (offset + i));
      result = (result << 8) | (b & 0xFF);
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
  public byte[] readArray_MemorySegment() throws Exception {
    if (memorySegment == null) {
      throw new UnsupportedOperationException("MemorySegment not available");
    }
    int size = getKeySize();
    int offset = random.nextInt(FILE_SIZE - size);
    java.lang.reflect.Method readBytesMethod = memorySegment.getClass().getMethod("readBytes", long.class, int.class);
    return (byte[]) readBytesMethod.invoke(memorySegment, (long) offset, size);
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
  public boolean compareBytes_Mismatch() throws Exception {
    if (!isJava22OrLater()) {
      throw new UnsupportedOperationException("MemorySegment not available");
    }
    int size = getKeySize();

    // Use MemorySegment.mismatch via reflection
    Class<?> segmentClass = Class.forName("java.lang.foreign.MemorySegment");
    java.lang.reflect.Method ofArrayMethod = segmentClass.getMethod("ofArray", byte[].class);
    java.lang.reflect.Method asSliceMethod = segmentClass.getMethod("asSlice", long.class, long.class);
    java.lang.reflect.Method mismatchMethod = segmentClass.getMethod("mismatch", segmentClass);

    Object seg1 = ofArrayMethod.invoke(null, (Object) keyBytes);
    seg1 = asSliceMethod.invoke(seg1, 0L, (long) size);

    Object seg2 = ofArrayMethod.invoke(null, (Object) comparisonBytes);
    seg2 = asSliceMethod.invoke(seg2, 0L, (long) size);

    long mismatch = (long) mismatchMethod.invoke(seg1, seg2);
    return mismatch == -1;
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
  public int readVLQ_MemorySegment() throws Exception {
    if (memorySegment == null) {
      throw new UnsupportedOperationException("MemorySegment not available");
    }
    int offset = random.nextInt(FILE_SIZE - 100);
    java.lang.reflect.Method getMethod = memorySegment.getClass().getMethod("getByte", long.class);

    int result = 0;
    int shift = 0;
    byte b;
    do {
      b = (byte) getMethod.invoke(memorySegment, (long) offset++);
      result |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }
}
