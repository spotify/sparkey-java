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

import com.spotify.sparkey.CompressionType;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyReader;
import com.spotify.sparkey.SparkeyTestHelper;
import com.spotify.sparkey.extra.PooledSparkeyReader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Enumeration of available Sparkey reader implementations for testing.
 * Automatically detects which implementations are available based on Java version.
 */
public enum ReaderType {
  /**
   * Single-threaded reader using MappedByteBuffer (JDK 8+).
   * Not thread-safe. Supports all compression types.
   */
  SINGLE_THREADED_MMAP_JDK8("SingleThreaded_MMap_JDK8", false, false) {
    @Override
    public SparkeyReader open(File file) throws IOException {
      return Sparkey.openSingleThreadedReader(file);
    }

    @Override
    public boolean supportsMultithreading() {
      return false;
    }
  },

  /**
   * Pooled reader using MappedByteBuffer (JDK 8+).
   * Thread-safe with fixed pool. Supports all compression types.
   * Note: On Java 22+, this will use the MRJAR Java 22 implementation.
   */
  POOLED_MMAP_JDK8("Pooled_MMap_JDK8", false, false) {
    @Override
    public SparkeyReader open(File file) throws IOException {
      return PooledSparkeyReader.open(file);
    }

    @Override
    public boolean supportsMultithreading() {
      return true;
    }
  },

  /**
   * Pooled reader forcing Java 8 implementation (via fromReader).
   * Thread-safe with fixed pool. Supports all compression types.
   * Forces Java 8 single-threaded reader even on Java 22+.
   */
  POOLED_MMAP_FORCE_JDK8("Pooled_MMap_Force_JDK8", false, false) {
    @Override
    public SparkeyReader open(File file) throws IOException {
      // Force Java 8 by opening single-threaded reader first, then pooling it
      SparkeyReader baseReader = Sparkey.openSingleThreadedReader(file);
      return PooledSparkeyReader.fromReader(baseReader);
    }

    @Override
    public boolean supportsMultithreading() {
      return true;
    }
  },

  /**
   * Uncompressed reader using MemorySegment (Java 22+).
   * Immutable, zero-overhead thread-safe. UNCOMPRESSED files only.
   */
  UNCOMPRESSED_MEMORYSEGMENT_J22("Uncompressed_MemorySegment_J22", true, true) {
    @Override
    public SparkeyReader open(File file) throws IOException {
      // Access package-private method via test helper
      return SparkeyTestHelper.openUncompressedJ22(file);
    }

    @Override
    public boolean supportsMultithreading() {
      return true;
    }
  },

  /**
   * Single-threaded reader using MemorySegment (Java 22+).
   * Not thread-safe. Supports all compression types.
   */
  SINGLE_THREADED_MEMORYSEGMENT_J22("SingleThreaded_MemorySegment_J22", true, false) {
    @Override
    public SparkeyReader open(File file) throws IOException {
      // Access package-private method via test helper
      return SparkeyTestHelper.openSingleThreadedJ22(file);
    }

    @Override
    public boolean supportsMultithreading() {
      return false;
    }
  };

  private final String name;
  private final boolean requiresJava22;
  private final boolean uncompressedOnly;

  ReaderType(String name, boolean requiresJava22, boolean uncompressedOnly) {
    this.name = name;
    this.requiresJava22 = requiresJava22;
    this.uncompressedOnly = uncompressedOnly;
  }

  /**
   * Open a SparkeyReader using this reader type.
   */
  public abstract SparkeyReader open(File file) throws IOException;

  /**
   * Check if this reader type supports multithreading.
   * Single-threaded readers return false, pooled/immutable readers return true.
   */
  public boolean supportsMultithreading() {
    return true;
  }

  /**
   * Check if this reader type is available on the current JVM.
   * For tests, we assume Java 22+ is always available.
   */
  public boolean isAvailable() {
    if (!requiresJava22) {
      return true;
    }

    // Check Java version directly - simpler than reflection for test/benchmark code
    // Java 22+ has MemorySegment API which our J22 implementations require
    int javaVersion = getJavaVersion();
    return javaVersion >= 22;
  }

  private static int getJavaVersion() {
    String version = System.getProperty("java.version");
    // Handle formats: "22", "22.0.1", "1.8.0_292"
    if (version.startsWith("1.")) {
      // Old format (1.8 = Java 8)
      return Integer.parseInt(version.substring(2, 3));
    } else {
      // New format (22.0.1 = Java 22)
      int dotIndex = version.indexOf('.');
      if (dotIndex > 0) {
        return Integer.parseInt(version.substring(0, dotIndex));
      }
      return Integer.parseInt(version);
    }
  }

  /**
   * Check if this reader type supports compressed files (Snappy, Zstd, etc).
   * Returns false only for readers that support uncompressed files exclusively.
   */
  public boolean supportsCompression() {
    return !uncompressedOnly;
  }

  /**
   * Check if this reader type supports the given compression type.
   */
  public boolean supports(CompressionType compressionType) {
    if (uncompressedOnly) {
      return compressionType == CompressionType.NONE;
    }
    return true;
  }

  /**
   * Get all available reader types for the current JVM.
   */
  public static List<ReaderType> getAvailable() {
    List<ReaderType> available = new ArrayList<>();
    for (ReaderType type : values()) {
      if (type.isAvailable()) {
        available.add(type);
      }
    }
    return available;
  }

  /**
   * Get all available reader types that support the given compression type.
   */
  public static List<ReaderType> getAvailableFor(CompressionType compressionType) {
    List<ReaderType> available = new ArrayList<>();
    for (ReaderType type : values()) {
      if (type.isAvailable() && type.supports(compressionType)) {
        available.add(type);
      }
    }
    return available;
  }

  /**
   * Get display name for test parameterization.
   */
  @Override
  public String toString() {
    return name;
  }

  /**
   * Convert to JUnit 4 Parameterized format.
   * Returns array of [ReaderType] for each available reader.
   */
  public static Object[][] availableAsParameters() {
    List<ReaderType> available = getAvailable();
    Object[][] params = new Object[available.size()][];
    for (int i = 0; i < available.size(); i++) {
      params[i] = new Object[]{available.get(i)};
    }
    return params;
  }

  /**
   * Convert to JUnit 4 Parameterized format for specific compression type.
   * Returns array of [ReaderType] for each available reader that supports the compression type.
   */
  public static Object[][] availableAsParametersFor(CompressionType compressionType) {
    List<ReaderType> available = getAvailableFor(compressionType);
    Object[][] params = new Object[available.size()][];
    for (int i = 0; i < available.size(); i++) {
      params[i] = new Object[]{available.get(i)};
    }
    return params;
  }
}
