/*
 * Copyright (c) 2026 Spotify AB
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks completion of a {@link SparkeyReader#load(LoadMode)} operation.
 *
 * <p>Loading is best-effort: it requests the OS to make mapped file data resident
 * in memory, but pages may be evicted under memory pressure. Behavior may vary
 * across JDKs, operating systems, and filesystems. This is not equivalent to
 * mlock — pages are not pinned.
 *
 * <p>Calling {@link SparkeyReader#load(LoadMode)} multiple times will schedule
 * additional work each time — it is not idempotent.
 *
 * <p>If the reader is closed before or during loading, completion is best-effort
 * and may not prefetch all requested bytes.
 */
public class LoadResult {
  private static final LoadResult COMPLETED = new LoadResult(0, CompletableFuture.completedFuture(null));

  private static final String PARALLELISM_PROPERTY = "sparkey.load.parallelism";
  private static volatile Executor defaultExecutor;

  private final long requestedBytes;
  private final CompletableFuture<Void> future;

  LoadResult(long requestedBytes, CompletableFuture<Void> future) {
    this.requestedBytes = requestedBytes;
    this.future = future;
  }

  /** Returns true if the load operation has completed (successfully or with an error). */
  public boolean isDone() {
    return future.isDone();
  }

  /**
   * Block until the load operation completes.
   *
   * @throws InterruptedException if the current thread is interrupted while waiting
   * @throws RuntimeException if the load operation failed
   */
  public void await() throws InterruptedException {
    try {
      future.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new RuntimeException("Load failed", cause);
    }
  }

  /** Total bytes that the load operation intends to prefetch. */
  public long requestedBytes() {
    return requestedBytes;
  }

  /**
   * Returns the underlying {@link CompletableFuture} for this load operation.
   *
   * <p>Useful for composing with other async operations, adding timeouts,
   * or inspecting errors.
   */
  public CompletableFuture<Void> toCompletableFuture() {
    return future;
  }

  /** Returns a no-op LoadResult that is already complete with zero bytes. */
  public static LoadResult completed() {
    return COMPLETED;
  }

  /**
   * Create a LoadResult from a byte count and a future.
   *
   * <p>Useful for composing custom load operations, e.g. loading multiple
   * readers and combining their results.
   *
   * @param requestedBytes total bytes intended to prefetch
   * @param future the future tracking completion
   */
  public static LoadResult create(long requestedBytes, CompletableFuture<Void> future) {
    return new LoadResult(requestedBytes, future);
  }

  /**
   * Combine multiple LoadResults into one that completes when all are done.
   *
   * <p>The returned result's {@link #requestedBytes()} is the sum of all inputs.
   *
   * @param results the LoadResults to combine
   */
  public static LoadResult combine(LoadResult... results) {
    long totalBytes = 0;
    int pending = 0;
    CompletableFuture<Void>[] futures = new CompletableFuture[results.length];
    for (LoadResult r : results) {
      totalBytes += r.requestedBytes;
      if (!r.future.isDone()) {
        futures[pending++] = r.future;
      }
    }
    if (pending == 0) {
      return totalBytes == 0 ? COMPLETED : new LoadResult(totalBytes, CompletableFuture.completedFuture(null));
    }
    if (pending == 1) {
      return new LoadResult(totalBytes, futures[0]);
    }
    CompletableFuture<Void> combined = CompletableFuture.allOf(
        java.util.Arrays.copyOf(futures, pending));
    return new LoadResult(totalBytes, combined);
  }

  /** Submit a load task to the given executor, returning a CompletableFuture. */
  static CompletableFuture<Void> submit(Runnable task, Executor executor) {
    return CompletableFuture.runAsync(task, executor);
  }

  /**
   * Build a LoadResult for the given mode by combining index and log loading.
   *
   * @param mode which parts to load
   * @param executor the executor to run tasks on
   * @param indexBytes size of the index data
   * @param indexLoader runnable that loads index pages
   * @param logBytes size of the log data
   * @param logLoader runnable that loads log pages
   */
  static LoadResult load(LoadMode mode, Executor executor,
                         long indexBytes, Runnable indexLoader,
                         long logBytes, Runnable logLoader) {
    java.util.Objects.requireNonNull(mode, "mode");
    java.util.Objects.requireNonNull(executor, "executor");
    if (mode == LoadMode.NONE) {
      return COMPLETED;
    }
    LoadResult indexResult = COMPLETED;
    if (mode != LoadMode.LOG) {
      indexResult = new LoadResult(indexBytes, submit(indexLoader, executor));
    }
    LoadResult logResult = COMPLETED;
    if (mode != LoadMode.INDEX) {
      logResult = new LoadResult(logBytes, submit(logLoader, executor));
    }
    return combine(indexResult, logResult);
  }

  /**
   * Get the default executor used when no explicit executor is provided.
   *
   * <p>The default pool size is controlled by the system property
   * {@code sparkey.load.parallelism} (default: 2).
   */
  static Executor getDefaultExecutor() {
    Executor e = defaultExecutor;
    if (e == null) {
      synchronized (LoadResult.class) {
        e = defaultExecutor;
        if (e == null) {
          int parallelism = getConfiguredParallelism();
          e = createExecutor(parallelism);
          defaultExecutor = e;
        }
      }
    }
    return e;
  }

  private static int getConfiguredParallelism() {
    String value = System.getProperty(PARALLELISM_PROPERTY);
    if (value != null) {
      try {
        int n = Integer.parseInt(value);
        if (n >= 1) {
          return n;
        }
      } catch (NumberFormatException ignored) {
      }
    }
    return 2;
  }

  private static ExecutorService createExecutor(int threads) {
    AtomicInteger counter = new AtomicInteger();
    ThreadFactory factory = r -> {
      Thread t = new Thread(r, "sparkey-load-" + counter.getAndIncrement());
      t.setDaemon(true);
      return t;
    };
    return Executors.newFixedThreadPool(threads, factory);
  }
}
