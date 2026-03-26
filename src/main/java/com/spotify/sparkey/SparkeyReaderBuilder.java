/*
 * Copyright (c) 2011-2013 Spotify AB
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
package com.spotify.sparkey;

import java.io.File;
import java.io.IOException;

/**
 * Builder for configuring and opening a SparkeyReader.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Default mmap-backed pooled reader
 * SparkeyReader reader = Sparkey.reader().file(base).open();
 *
 * // Heap-backed pooled reader (reads files into JVM heap at open time)
 * SparkeyReader reader = Sparkey.reader().file(base).useHeap(true).open();
 *
 * // Explicit index and log files
 * SparkeyReader reader = Sparkey.reader()
 *     .indexFile(indexFile)
 *     .logFile(logFile)
 *     .open();
 *
 * // Single-threaded reader
 * SparkeyReader reader = Sparkey.reader().file(base).singleThreaded(true).open();
 * }</pre>
 */
public final class SparkeyReaderBuilder {
  private File indexFile;
  private File logFile;
  private boolean heapBacked;
  private boolean singleThreaded;
  private int poolSize = -1;

  SparkeyReaderBuilder() {
  }

  /**
   * Set the base file. The index (.spi) and log (.spl) file names are derived from it.
   *
   * @param file base file (any of .spi, .spl, or stem without extension)
   */
  public SparkeyReaderBuilder file(File file) {
    this.indexFile = Sparkey.getIndexFile(file);
    this.logFile = Sparkey.getLogFile(file);
    return this;
  }

  /**
   * Set the index file explicitly.
   * If not set, derived from the base file passed to {@link #file(File)}.
   */
  public SparkeyReaderBuilder indexFile(File indexFile) {
    this.indexFile = indexFile;
    return this;
  }

  /**
   * Set the log file explicitly.
   * If not set, derived from the base file passed to {@link #file(File)}.
   */
  public SparkeyReaderBuilder logFile(File logFile) {
    this.logFile = logFile;
    return this;
  }

  /**
   * Convenience method equivalent to {@code useHeap(true)}.
   */
  public SparkeyReaderBuilder useHeap() {
    return useHeap(true);
  }

  /**
   * Read the sparkey files into JVM heap memory ({@code byte[]} arrays) instead of
   * using memory-mapped files.
   *
   * <p>The default is {@code false} (memory-mapped), which is preferred for most use cases.
   * Memory-mapped files let the OS manage caching efficiently and don't consume JVM heap.
   *
   * <p>Heap-backed mode is useful in environments where the JVM is configured with a large
   * heap that would otherwise go unused, while available page cache is too small to fit the
   * data. In this case, reading the files into heap memory avoids disk I/O at the cost of
   * slower open time and increased GC pressure.
   *
   * <p>Trade-offs of heap-backed mode:
   * <ul>
   *   <li>Open is slower — the entire file must be read from disk</li>
   *   <li>Data consumes JVM heap — counted toward {@code -Xmx}</li>
   *   <li>No file descriptors or mmap resources held open after construction</li>
   *   <li>{@link SparkeyReader#load} is a no-op (data is already in memory)</li>
   * </ul>
   *
   * @param useHeap {@code true} to read files into heap memory
   */
  public SparkeyReaderBuilder useHeap(boolean useHeap) {
    this.heapBacked = useHeap;
    return this;
  }

  /**
   * Convenience method equivalent to {@code singleThreaded(true)}.
   */
  public SparkeyReaderBuilder singleThreaded() {
    return singleThreaded(true);
  }

  /**
   * Use a single-threaded reader (not thread-safe).
   * Default is {@code false} (pooled, thread-safe).
   *
   * @param singleThreaded {@code true} for a single-threaded reader
   */
  public SparkeyReaderBuilder singleThreaded(boolean singleThreaded) {
    this.singleThreaded = singleThreaded;
    return this;
  }

  /**
   * Set the pool size for the pooled reader.
   * Only meaningful when not using {@link #singleThreaded(boolean) singleThreaded(true)}.
   *
   * @param poolSize number of reader instances (minimum 1)
   */
  public SparkeyReaderBuilder poolSize(int poolSize) {
    this.poolSize = poolSize;
    return this;
  }

  /**
   * Open the reader with the configured options.
   *
   * @return a new SparkeyReader
   * @throws IOException if the file cannot be opened
   * @throws IllegalStateException if no file has been configured
   */
  public SparkeyReader open() throws IOException {
    if (indexFile == null || logFile == null) {
      throw new IllegalStateException("No file configured. Call file(), or both indexFile() and logFile().");
    }
    return SparkeyImplSelector.open(this);
  }

  File indexFile() {
    return indexFile;
  }

  File logFile() {
    return logFile;
  }

  boolean isHeapBacked() {
    return heapBacked;
  }

  boolean isSingleThreaded() {
    return singleThreaded;
  }

  int poolSize() {
    return poolSize;
  }
}
