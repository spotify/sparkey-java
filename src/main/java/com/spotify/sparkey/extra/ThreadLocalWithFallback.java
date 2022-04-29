package com.spotify.sparkey.extra;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.function.Supplier;

/**
 * Since Java 17, using ThreadLocal may not behave as expected
 * when being called from ForkJoinPool.commonPool().
 * The common pool will clear all values for ThreadLocal after finishing a task,
 * so it can not be used for retaining expensive objects on the same thread across
 * multiple tasks.
 *
 * This subclass will instead let values survive by using a synchronized fallback hashmap.
 */

public class ThreadLocalWithFallback<T> extends ThreadLocal<T> {
  private final Map<Thread, T> fallback = Collections.synchronizedMap(new WeakHashMap<>());
  private final Supplier<? extends T> supplier;

  private ThreadLocalWithFallback(Supplier<? extends T> supplier) {
    this.supplier = supplier;
  }

  @Override
  protected T initialValue() {
    final Thread currentThread = Thread.currentThread();
    final T fallbackValue = fallback.get(currentThread);
    if (fallbackValue != null) {
      return fallbackValue;
    }

    final T value = Objects.requireNonNull(supplier.get());
    fallback.put(currentThread, value);
    return value;
  }

  public static <S> ThreadLocalWithFallback<S> withInitial(Supplier<? extends S> supplier) {
    return new ThreadLocalWithFallback<>(supplier);
  }

  @Override
  public void set(T value) {
    fallback.put(Thread.currentThread(), value);
    super.set(value);
  }

  @Override
  public void remove() {
    fallback.remove(Thread.currentThread());
    super.remove();
  }
}
