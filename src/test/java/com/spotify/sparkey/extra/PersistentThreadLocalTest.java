package com.spotify.sparkey.extra;

import org.junit.Test;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PersistentThreadLocalTest {

  private final AtomicInteger closedThreads = new AtomicInteger();
  private final PersistentThreadLocal<Long> threadLocal =
          PersistentThreadLocal.withInitial(() -> Thread.currentThread().getId(), threadId -> closedThreads.incrementAndGet());

  @Test
  public void testReturnsMainThreadIdWhenInMainThread() {
    assertEquals(1, threadLocal.get().intValue());
  }

  @Test
  public void testSet() {
    assertEquals(Thread.currentThread().getId(), threadLocal.get().longValue());
    threadLocal.set(123L);
    assertEquals(123, threadLocal.get().longValue());
    threadLocal.remove();
    assertEquals(Thread.currentThread().getId(), threadLocal.get().longValue());
  }

  @Test(expected = NullPointerException.class)
  public void testSetNull() {
    threadLocal.set(null);
  }

  @Test(expected = NullPointerException.class)
  public void testSuppliedNull() {
    PersistentThreadLocal.withInitial(() -> null).get();
  }

  @Test
  public void testForkJoinPoolCommonPool() {
    final Thread mainThread = Thread.currentThread();

    final int poolSize = ForkJoinPool.commonPool().getParallelism();
    final AtomicInteger failures = new AtomicInteger();
    for (int i = 0; i < 10000; i++) {
      int finalI = i;
      ForkJoinPool.commonPool().execute(() -> {
        if (Thread.currentThread() == mainThread) {
          return;
        }
        checkFailure(failures);
        if (finalI < poolSize) {
          // Make sure to keep the first poolsize workers alive for long enough to use the entire pool
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
        }
      });
    }
    assertEquals(poolSize, threadLocal.fallbackSize());
    assertEquals(0, failures.get());
  }

  @Test
  public void testThreadDeath() throws InterruptedException {
    final AtomicInteger failures = new AtomicInteger();

    for (int i = 0; i < 10_000; i++) {
      Thread thread = new Thread(() -> {
        checkFailure(failures);
      });
      thread.start();
      thread.join();
      assertEquals(Thread.State.TERMINATED, thread.getState());
      if (threadLocal.fallbackSize() >= 10) {
        System.gc();
      }
      final int fallbackSize = threadLocal.fallbackSize();
      assertTrue("Was: " + fallbackSize, fallbackSize < 100);
      assertEquals(i + 1, closedThreads.get() + fallbackSize);
    }
    assertEquals(0, failures.get());
  }

  private void checkFailure(AtomicInteger failures) {
    if (Thread.currentThread().getId() != threadLocal.get()) {
      failures.incrementAndGet();
    }
  }
}