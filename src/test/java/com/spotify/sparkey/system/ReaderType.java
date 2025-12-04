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
   * Uncompressed reader using MemorySegment (Java 22+).
   * Immutable, zero-overhead thread-safe. UNCOMPRESSED files only.
   */
  UNCOMPRESSED_MEMORYSEGMENT_J22("Uncompressed_MemorySegment_J22", true, true) {
    @Override
    public SparkeyReader open(File file) throws IOException {
      try {
        // Use reflection to avoid compile-time dependency on Java 22 classes
        Class<?> clazz = Class.forName("com.spotify.sparkey.UncompressedSparkeyReaderJ22");
        Method openMethod = clazz.getMethod("open", File.class);
        return (SparkeyReader) openMethod.invoke(null, file);
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException("UncompressedSparkeyReaderJ22 not available on Java " +
          System.getProperty("java.version"));
      } catch (Exception e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new RuntimeException("Failed to open UncompressedSparkeyReaderJ22", e);
      }
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
      try {
        // Use reflection to avoid compile-time dependency on Java 22 classes
        Class<?> clazz = Class.forName("com.spotify.sparkey.SingleThreadedSparkeyReaderJ22");
        Method openMethod = clazz.getMethod("open", File.class);
        return (SparkeyReader) openMethod.invoke(null, file);
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException("SingleThreadedSparkeyReaderJ22 not available on Java " +
          System.getProperty("java.version"));
      } catch (Exception e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new RuntimeException("Failed to open SingleThreadedSparkeyReaderJ22", e);
      }
    }

    @Override
    public boolean supportsMultithreading() {
      return false;
    }
  },

  /**
   * Pooled reader using MemorySegment (Java 22+).
   * Thread-safe with fixed pool. Supports all compression types.
   */
  POOLED_MEMORYSEGMENT_J22("Pooled_MemorySegment_J22", true, false) {
    @Override
    public SparkeyReader open(File file) throws IOException {
      try {
        // Use reflection to avoid compile-time dependency on Java 22 classes
        Class<?> clazz = Class.forName("com.spotify.sparkey.SingleThreadedSparkeyReaderJ22");
        Method openMethod = clazz.getMethod("open", File.class);
        SparkeyReader reader = (SparkeyReader) openMethod.invoke(null, file);
        return PooledSparkeyReader.fromReader(reader);
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException("SingleThreadedSparkeyReaderJ22 not available on Java " +
          System.getProperty("java.version"));
      } catch (Exception e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new RuntimeException("Failed to open PooledSparkeyReaderJ22", e);
      }
    }

    @Override
    public boolean supportsMultithreading() {
      return true;
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
   */
  public boolean isAvailable() {
    if (!requiresJava22) {
      return true;
    }

    // Check if Java 22+ is available by trying to load a J22 class
    try {
      Class.forName("com.spotify.sparkey.ReadOnlyMemMapJ22");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
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
