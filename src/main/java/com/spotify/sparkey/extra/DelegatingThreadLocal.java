package com.spotify.sparkey.extra;

import java.util.function.Supplier;

/**
 * Since Java 17, using ThreadLocal may not behave as expected
 * when being called from ForkJoinPool.commonPool().
 * The common pool will clear all values for ThreadLocal after finishing a task,
 * so it can not be used for retaining expensive objects on the same thread across
 * multiple tasks.
 *
 * To avoid creating too many expensive objects, we can instead avoid
 * the regular ThreadLocal implementation for threads that belong to the commonPool
 */

public class DelegatingThreadLocal<T> extends ThreadLocal<T> {
  private final WeakThreadLocal<T> weakThreadLocal;
  private final ThreadLocal<T> threadLocal;

  private DelegatingThreadLocal(WeakThreadLocal<T> weakThreadLocal, ThreadLocal<T> threadLocal) {
    this.weakThreadLocal = weakThreadLocal;
    this.threadLocal = threadLocal;
  }

  public static <S> DelegatingThreadLocal<S> withInitial(Supplier<? extends S> supplier) {
    final WeakThreadLocal<S> threadLocal = WeakThreadLocal.withInitial(supplier);
    final ThreadLocal<S> threadLocal1 = ThreadLocal.withInitial(supplier);
    return new DelegatingThreadLocal<S>(threadLocal, threadLocal1);
  }

  private ThreadLocal<T> getDelegate() {
    if (Thread.currentThread().getName().startsWith("ForkJoinPool.commonPool-worker-")) {
      return weakThreadLocal;
    } else {
      return threadLocal;
    }
  }

  public T get() {
    return getDelegate().get();
  }

  public void set(T value) {
    getDelegate().set(value);
  }

  public void remove() {
    getDelegate().remove();
  }
}
