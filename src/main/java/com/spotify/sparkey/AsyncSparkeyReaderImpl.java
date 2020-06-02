package com.spotify.sparkey;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class AsyncSparkeyReaderImpl implements AsyncSparkeyReader {

  private static final AtomicInteger POOL_COUNTER = new AtomicInteger();

  private final ExecutorService executor;
  private final boolean ownedExecutor;
  private volatile boolean closed = false;
  private final AtomicInteger pending = new AtomicInteger();

  private final ThreadLocal<SparkeyReader> readers;
  private final SingleThreadedSparkeyReader firstReader;
  private final Map<Thread, SingleThreadedSparkeyReader> duplicates = new ConcurrentHashMap<>();

  private AsyncSparkeyReaderImpl(final File file, final ExecutorService executor, final boolean ownedExecutor) throws IOException {
    firstReader = SingleThreadedSparkeyReader.open(file);
    readers = ThreadLocal.withInitial(() -> {
      final SingleThreadedSparkeyReader duplicate = firstReader.duplicate();
      duplicates.put(Thread.currentThread(), duplicate);

      cleanDeadThreads();
      return duplicate;
    });
    this.executor = executor;
    this.ownedExecutor = ownedExecutor;
  }

  private void cleanDeadThreads() {
    final Iterator<Map.Entry<Thread, SingleThreadedSparkeyReader>> iterator = duplicates.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<Thread, SingleThreadedSparkeyReader> entry = iterator.next();
      if (!entry.getKey().isAlive()) {
        entry.getValue().closeDuplicate();
        iterator.remove();
      }
    }
  }

  static AsyncSparkeyReader open(final File file, final int numThreads) throws IOException {
    if (numThreads < 1) {
      throw new IllegalArgumentException("Not enough threads: " + numThreads);
    }
    final int poolNumber = POOL_COUNTER.incrementAndGet();
    final AtomicInteger threadCounter = new AtomicInteger();
    final ExecutorService executor1 = Executors.newFixedThreadPool(numThreads, r -> {
      final Thread thread = new Thread(r);
      thread.setDaemon(true);
      thread.setName("async-sparkey-reader-pool-" + poolNumber + "-" + threadCounter.incrementAndGet());
      return thread;
    });
    return new AsyncSparkeyReaderImpl(file, executor1, true);
  }

  static AsyncSparkeyReader open(final File file, final ExecutorService executor) throws IOException {
    return new AsyncSparkeyReaderImpl(file, executor, false);
  }

  @Override
  public CompletionStage<String> getAsString(final String key) {
    verifyOpen();
    final CompletableFuture<String> future = new CompletableFuture<>();
    incrPending();
    try {
      executor.execute(() -> {
        try {
          future.complete(readers.get().getAsString(key));
        } catch (final Exception e) {
          future.completeExceptionally(e);
        } finally {
          decrPending();
        }
      });
    } catch (final Exception e) {
      decrPending();
      throw e;
    }
    return future;
  }

  @Override
  public CompletionStage<byte[]> getAsByteArray(final byte[] key) {
    verifyOpen();
    final CompletableFuture<byte[]> future = new CompletableFuture<>();
    pending.incrementAndGet();
    try {
      executor.execute(() -> {
        try {
          future.complete(readers.get().getAsByteArray(key));
        } catch (final Exception e) {
          future.completeExceptionally(e);
        } finally {
          decrPending();
        }
      });
    } catch (final Exception e) {
      decrPending();
      throw e;
    }
    return future;
  }

  private void decrPending() {
    if (!ownedExecutor) {
      pending.decrementAndGet();
    }
  }

  private void incrPending() {
    if (!ownedExecutor) {
      pending.incrementAndGet();
    }
  }

  private void verifyOpen() {
    if (!ownedExecutor) {
      if (closed) {
        throw new RejectedExecutionException();
      }
    }
  }

  @Override
  public void close() {
    closed = true;
    try {
      if (ownedExecutor) {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
      } else {
        while (pending.get() > 0) {
          Thread.sleep(100L);
        }
      }
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      // If we first close all duplicates, closing the first reader will be fast
      for (SingleThreadedSparkeyReader duplicate : duplicates.values()) {
        duplicate.closeDuplicate();
      }
      firstReader.close();
    }
  }
}
