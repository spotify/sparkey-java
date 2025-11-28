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
package com.spotify.sparkey.extra;

import com.spotify.sparkey.IndexHeader;
import com.spotify.sparkey.LogHeader;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyReader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A thread-safe Sparkey reader using a fixed-size pool of reader instances.
 *
 * <p>This implementation uses hashed thread-ID-based striping to distribute load across a
 * bounded pool of readers, providing:
 * <ul>
 *   <li>Bounded memory usage (O(pool size) instead of O(threads))</li>
 *   <li>Compatibility with virtual threads (no ThreadLocal unbounded growth)</li>
 *   <li>Low contention through reader pool sizing</li>
 *   <li>Uniform distribution even with pathological thread ID patterns</li>
 *   <li>Simple, predictable performance characteristics</li>
 * </ul>
 *
 * <p>The implementation hashes thread IDs before mapping to pool slots to prevent
 * pathological cases where only even/odd thread IDs access Sparkey, which would
 * otherwise use only half the pool and double contention.
 *
 * <p><strong>Recommended for Java 21+ applications using virtual threads.</strong>
 *
 * <h2>Performance Tuning</h2>
 * Pool size should be chosen based on concurrency level and contention tolerance:
 * <ul>
 *   <li>Small pool (8-16): Low memory, higher contention - good for low concurrency</li>
 *   <li>Medium pool (32-128): Balanced - recommended default (cores * 8)</li>
 *   <li>Large pool (256+): Minimal contention - extreme high concurrency</li>
 * </ul>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * // Default pool size (cores * 8)
 * try (PooledSparkeyReader reader = PooledSparkeyReader.open(sparkeyFile)) {
 *   String value = reader.getAsString("my-key");
 * }
 *
 * // Custom pool size
 * try (PooledSparkeyReader reader = PooledSparkeyReader.open(sparkeyFile, 128)) {
 *   String value = reader.getAsString("my-key");
 * }
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * All read methods (getAsString, getAsByteArray, getAsEntry) are thread-safe.
 * Read operations synchronize on individual pool readers, distributing contention
 * across the pool.
 *
 * <p><strong>Iteration is fully supported and thread-safe.</strong> Each call to
 * {@link #iterator()} creates an isolated duplicate of the index and opens its own
 * FileInputStream, so there is no shared mutable state between concurrent iterators.
 *
 * @see ThreadLocalSparkeyReader deprecated alternative with unbounded memory growth
 */
public class PooledSparkeyReader implements SparkeyReader {

  private final SparkeyReader baseReader;
  private final SparkeyReader[] pool;
  private final int mask;  // For fast modulo: threadId & mask == threadId % poolSize
  private final AtomicIntegerArray busy;  // Hint for contention avoidance
  private volatile boolean closed = false;

  private PooledSparkeyReader(SparkeyReader baseReader, int requestedPoolSize) {
    this.baseReader = baseReader;

    // Round up to next power of 2 for efficient bit-masking
    int poolSize = nextPowerOfTwo(requestedPoolSize);
    this.pool = new SparkeyReader[poolSize];
    this.mask = poolSize - 1;
    this.busy = new AtomicIntegerArray(poolSize);

    // Create pool of reader duplicates
    try {
      for (int i = 0; i < poolSize; i++) {
        pool[i] = baseReader.duplicate();
      }
    } catch (Exception e) {
      // Cleanup on failure
      for (int i = 0; i < poolSize; i++) {
        if (pool[i] != null) {
          pool[i].close();
        }
      }
      baseReader.close();
      throw new RuntimeException("Failed to create reader pool", e);
    }
  }

  /**
   * Open a pooled Sparkey reader with default pool size.
   *
   * <p>Default pool size is {@code Runtime.getRuntime().availableProcessors() * 8},
   * rounded up to the next power of 2.
   *
   * @param indexFile the Sparkey index file (.spi)
   * @return a new pooled reader
   * @throws IOException if the file cannot be opened
   */
  public static PooledSparkeyReader open(File indexFile) throws IOException {
    return open(indexFile, computeDefaultPoolSize());
  }

  /**
   * Open a pooled Sparkey reader with specified pool size.
   *
   * <p>The pool size will be rounded up to the next power of 2 for efficient
   * thread-to-reader mapping.
   *
   * @param indexFile the Sparkey index file (.spi)
   * @param poolSize number of reader instances (minimum 1)
   * @return a new pooled reader
   * @throws IOException if the file cannot be opened
   * @throws IllegalArgumentException if poolSize < 1
   */
  public static PooledSparkeyReader open(File indexFile, int poolSize) throws IOException {
    if (poolSize < 1) {
      throw new IllegalArgumentException("poolSize must be >= 1, got: " + poolSize);
    }
    SparkeyReader baseReader = Sparkey.openSingleThreadedReader(indexFile);
    return new PooledSparkeyReader(baseReader, poolSize);
  }


  /**
   * Hash thread ID to ensure uniform distribution across pool slots.
   *
   * <p>Without hashing, workloads where only even (or odd) thread IDs access Sparkey
   * would use only half the pool slots, doubling contention. This hash function
   * scrambles the bits to ensure full pool utilization.
   *
   * @param threadId the thread ID to hash
   * @return hashed thread ID
   */
  private static int hashThreadId(long threadId) {
    // MurmurHash3 finalizer - provides excellent avalanche properties
    // Ensures uniform distribution even for sequential input values
    long h = threadId;
    h ^= (h >>> 33);
    h *= 0xff51afd7ed558ccdL;
    h ^= (h >>> 33);
    return (int)h;
  }

  // Helper method to execute operations on pooled readers with busy tracking

  /**
   * Execute an operation on a pooled reader with slot selection and busy tracking.
   * Selects affinity slot based on thread ID, falls back to random if busy.
   */
  private <T> T executeOnPooledReader(ReaderOperation<T> operation) throws IOException {
    if (closed) {
      throw new IllegalStateException("Reader is closed");
    }

    // Select slot (affinity or random fallback)
    long threadId = Thread.currentThread().getId();
    int affinitySlot = hashThreadId(threadId) & mask;
    int slotToUse = (busy.get(affinitySlot) == 0) ? affinitySlot
        : ThreadLocalRandom.current().nextInt(pool.length);

    SparkeyReader reader = pool[slotToUse];
    // Increment busy counter (tracks number of threads using/waiting for this slot)
    busy.incrementAndGet(slotToUse);
    try {
      synchronized (reader) {
        return operation.execute(reader);
      }
    } finally {
      // Decrement when done (slot is free when counter reaches 0)
      busy.decrementAndGet(slotToUse);
    }
  }

  @FunctionalInterface
  private interface ReaderOperation<T> {
    T execute(SparkeyReader reader) throws IOException;
  }

  // Override methods that need synchronization for thread safety

  @Override
  public String getAsString(String key) throws IOException {
    return executeOnPooledReader(reader -> reader.getAsString(key));
  }

  @Override
  public byte[] getAsByteArray(byte[] key) throws IOException {
    return executeOnPooledReader(reader -> reader.getAsByteArray(key));
  }

  @Override
  public Entry getAsEntry(byte[] key) throws IOException {
    return executeOnPooledReader(reader -> reader.getAsEntry(key));
  }

  // Non-critical methods that read immutable data or create isolated state
  // These don't need busy tracking

  @Override
  public IndexHeader getIndexHeader() {
    return baseReader.getIndexHeader();
  }

  @Override
  public LogHeader getLogHeader() {
    return baseReader.getLogHeader();
  }

  @Override
  public Iterator<Entry> iterator() {
    // iterator() creates an isolated duplicate via index.duplicate() and
    // opens its own FileInputStream, so there is no shared mutable state between threads
    return baseReader.iterator();
  }

  @Override
  public long getLoadedBytes() {
    return baseReader.getLoadedBytes();
  }

  @Override
  public long getTotalBytes() {
    return baseReader.getTotalBytes();
  }

  @Override
  public SparkeyReader duplicate() {
    if (closed) {
      throw new IllegalStateException("Reader is closed");
    }
    // Return self - already thread-safe
    return this;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    // Don't close pooled readers - they're duplicates sharing ByteBuffers with baseReader.
    // Closing baseReader is sufficient as it closes the underlying resources.
    // The pooled readers become invalid, which is fine since we've marked closed=true.
    baseReader.close();
  }

  /**
   * Get the actual pool size (always a power of 2).
   *
   * @return number of reader instances in the pool
   */
  public int getPoolSize() {
    return pool.length;
  }

  // Helper methods

  /**
   * Compute default pool size for the reader pool.
   *
   * Pool size multiplier choice (4x vs 8x cores):
   * - Both 4x and 8x are valid choices with minimal practical difference
   * - 8x provides slightly less contention (99.8% vs 99% affinity hits in testing)
   * - Memory cost per reader is very low (~4 KiB) since MappedByteBuffers share
   *   the underlying mapped pages - only the Java object wrappers are duplicated
   * - 8x chosen as default for slightly better worst-case contention characteristics
   * - Users can override via openPooledReader(file, poolSize) if needed
   *
   * This tradeoff may be revisited in the future if usage patterns change.
   */
  private static int computeDefaultPoolSize() {
    int cores = Runtime.getRuntime().availableProcessors();
    return cores * 8;
  }

  private static int nextPowerOfTwo(int n) {
    if (n <= 1) return 1;
    // Handle edge case: if n is already power of 2
    if ((n & (n - 1)) == 0) return n;
    return 1 << (32 - Integer.numberOfLeadingZeros(n - 1));
  }
}
