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

/**
 * Enumeration of available Sparkey reader implementations for testing.
 * On master branch (before MRJAR), only MappedByteBuffer implementation exists.
 */
public enum ReaderType {
  /**
   * Single-threaded reader using MappedByteBuffer (JDK 8+).
   * Not thread-safe. Supports all compression types.
   */
  SINGLE_THREADED_MMAP_JDK8("SingleThreaded_MMap_JDK8") {
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
  POOLED_MMAP_JDK8("Pooled_MMap_JDK8") {
    @Override
    public SparkeyReader open(File file) throws IOException {
      SparkeyReader baseReader = Sparkey.openSingleThreadedReader(file);
      return PooledSparkeyReader.fromReader(baseReader);
    }

    @Override
    public boolean supportsMultithreading() {
      return true;
    }
  };

  private final String displayName;

  ReaderType(String displayName) {
    this.displayName = displayName;
  }

  /**
   * Open a reader of this type.
   */
  public abstract SparkeyReader open(File file) throws IOException;

  /**
   * Check if this reader type supports multithreaded access.
   * @return true if this reader can be safely used by multiple threads concurrently
   */
  public abstract boolean supportsMultithreading();

  /**
   * Check if this reader type is available on the current JVM.
   */
  public boolean isAvailable() {
    return true;  // All MMap readers are always available
  }

  /**
   * Check if this reader type supports the given compression type.
   */
  public boolean supports(CompressionType compression) {
    return true;  // All MMap readers support all compression types
  }

  @Override
  public String toString() {
    return displayName;
  }
}
