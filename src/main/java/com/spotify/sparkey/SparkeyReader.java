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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.Executor;

public interface SparkeyReader extends Iterable<SparkeyReader.Entry>, Closeable {
  /**
   * @param key the key to search for, interpreted as an UTF-8 string.
   * @return null if the key/value pair was not found, otherwise the value interpreted as an UTF-8 string.
   */
  String getAsString(String key) throws IOException;

  /**
   * @param key the key to search for
   * @return null if the key/value pair was not found, otherwise the raw byte array value
   */
  byte[] getAsByteArray(byte[] key) throws IOException;

  /**
   * This is mostly useful for retrieving large values that don't fit in a byte array.
   *
   * @param key the key to search for
   * @return null if the key/value pair was not found, otherwise the entry.
   *
   */
  Entry getAsEntry(byte[] key) throws IOException;

  IndexHeader getIndexHeader();
  LogHeader getLogHeader();

  /**
   * Create a duplicate of the reader. Useful for using the reader from another thread.
   * @return a duplicate of the reader.
   */
  SparkeyReader duplicate();

  // Deliberately override to avoid throwing IOException
  @Override
  void close();

  /**
   * Get an iterator over all the live entries.
   *
   * The iterator object is not thread safe,
   * and the entry objects are highly volatile
   * and will be invalidated by the next
   * iteration step. Don't leak this entry,
   * copy whatever data you want from it instead.
   *
   * @return an iterator
   */
  @Override
  Iterator<Entry> iterator();

  interface Entry {
    int getKeyLength();
    byte[] getKey();
    String getKeyAsString();

    long getValueLength();
    byte[] getValue() throws IOException;
    String getValueAsString() throws IOException;
    InputStream getValueAsStream();

    Type getType();
  }

  /**
   * Best-effort request to prefetch all mapped data (index and log) into memory.
   *
   * <p>Equivalent to {@code load(LoadMode.ALL)}.
   *
   * @return a LoadResult tracking the async operation
   * @see #load(LoadMode)
   */
  default LoadResult load() {
    return load(LoadMode.ALL);
  }

  /**
   * Best-effort request to prefetch mapped data into memory.
   *
   * <p>Equivalent to {@code load(mode, defaultExecutor)}.
   *
   * @param mode which parts of the sparkey file to prefetch
   * @return a LoadResult tracking the async operation
   * @see #load(LoadMode, Executor)
   */
  default LoadResult load(LoadMode mode) {
    if (mode == LoadMode.NONE) {
      return LoadResult.completed();
    }
    return load(mode, LoadResult.getDefaultExecutor());
  }

  /**
   * Best-effort request to prefetch mapped data into memory using the given executor.
   *
   * <p>Requests the OS/runtime to make the underlying mapped file data resident in memory.
   * This can improve lookup performance for large sparkey files on network-attached storage
   * by reducing page faults on first access.
   *
   * <p>Prefetching the index is cheap (typically tens of MB) and helps keep hash table
   * probes in memory. Prefetching the log is more expensive (can be multiple GB) but
   * helps keep value lookups in memory too.
   *
   * <p>The operation runs asynchronously on the given executor. The returned
   * {@link LoadResult} can be used to wait for completion or compose with other
   * async operations via {@link LoadResult#toCompletableFuture()}.
   *
   * <p>Advisory modes ({@link LoadMode#ALL}, etc.) are best-effort: actual behavior
   * depends on the JDK, OS, and filesystem. Pages may be evicted under memory pressure.
   *
   * <p>MLOCK modes ({@link LoadMode#ALL_MLOCK}, etc.) attempt to pin pages in RAM
   * using {@code mlock(2)}, preventing eviction. This requires:
   * <ul>
   *   <li>Java 22+ (uses the Foreign Function &amp; Memory API)</li>
   *   <li>{@code --enable-native-access=ALL-UNNAMED} JVM flag</li>
   *   <li>Sufficient OS memory lock limits ({@code ulimit -l} or {@code CAP_IPC_LOCK})</li>
   * </ul>
   * If any requirement is not met, mlock falls back to advisory prefetching silently.
   * Use {@link LoadResult#locked()} to check whether mlock succeeded.
   *
   * <p>If the reader is closed before or during loading, completion is best-effort
   * and may not prefetch all requested bytes.
   *
   * <p>{@link LoadMode#NONE} is a no-op, useful for keeping call sites clean
   * without conditional logic.
   *
   * @param mode which parts of the sparkey file to prefetch
   * @param executor the executor to run the prefetch task on
   * @return a LoadResult tracking the async operation
   */
  default LoadResult load(LoadMode mode, Executor executor) {
    return LoadResult.completed();
  }

  /**
   * Get the number of index and log file bytes loaded in memory.
   *
   * This number is based on MappedByteBuffer.isLoaded() and the resolution is
   * in increments of the memory chunk size (1 GB)
   *
   * @deprecated because it won't always be possible to compute the correct value
   */
  @Deprecated
  long getLoadedBytes();

  /**
   * Get the total number of index and log file bytes.
   */
  long getTotalBytes();

  enum Type {
    PUT, DELETE
  }
}
