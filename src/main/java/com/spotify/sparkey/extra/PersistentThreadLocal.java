/*
 * Copyright (c) 2022 Spotify AB
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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Since Java 17, using ThreadLocal may not behave as expected
 * when being called from ForkJoinPool.commonPool().
 * The common pool will clear all values for ThreadLocal after finishing a task,
 * so it can not be used for retaining expensive objects on the same thread across
 * multiple tasks.
 *
 * This subclass will instead let values survive by using a ConcurrentHashMap
 * based on Thread Id which is stable as long as the thread lives. This map is also cleaned up by
 * detecting thread death using a WeakReference.
 *
 * This implementation differs from the regular ThreadLocal in that it does not allow null values from the supplier.
 */
class PersistentThreadLocal<T> extends ThreadLocal<T> {
  private final Map<Long, T> fallback = new ConcurrentHashMap<>();
  private final Supplier<? extends T> supplier;
  private final Consumer<? super T> closer;

  private final ReferenceQueue<Thread> deadThreads = new ReferenceQueue<>();
  // Used to avoid creating more weak references than necessary
  private final Map<Long, RefWithThreadId> activeThreads = new ConcurrentHashMap<>();

  private PersistentThreadLocal(Supplier<? extends T> supplier, Consumer<? super T> closer) {
    this.supplier = supplier;
    this.closer = closer;
  }

  @Override
  protected T initialValue() {
    final Thread thread = Thread.currentThread();
    final long threadId = thread.getId();
    final T fallbackValue = fallback.get(threadId);
    if (fallbackValue != null) {
      // This case is typically only reachable on Java 17+ using ForkJoinPool.commonPool()
      return fallbackValue;
    }

    final T value = Objects.requireNonNull(supplier.get());
    setFallback(value, thread, threadId);
    return value;
  }

  private void setFallback(T value, Thread thread, long threadId) {
    final T prevValue = fallback.put(threadId, value);
    if (prevValue == null) {
      if (!activeThreads.containsKey(threadId)) {
        // New thread - register for death and clean up any dead threads
        cleanupDeadThreads();
        activeThreads.put(threadId, new RefWithThreadId(thread, threadId, deadThreads));
      }
    }
  }

  public static <S> PersistentThreadLocal<S> withInitial(Supplier<? extends S> supplier) {
    return withInitial(supplier, obj -> {});
  }

  public static <S> PersistentThreadLocal<S> withInitial(Supplier<? extends S> supplier, Consumer<? super S> closer) {
    return new PersistentThreadLocal<>(supplier, closer);
  }

  @Override
  public void set(T value) {
    final T nonNullValue = Objects.requireNonNull(value);
    final Thread thread = Thread.currentThread();
    final long threadId = thread.getId();
    setFallback(nonNullValue, thread, threadId);
    super.set(nonNullValue);
  }

  @Override
  public void remove() {
    fallback.remove(Thread.currentThread().getId());
    super.remove();
  }

  protected void cleanupDeadThreads() {
    RefWithThreadId ref = (RefWithThreadId) deadThreads.poll();
    while (ref != null) {
      final T value = fallback.remove(ref.threadId);
      activeThreads.remove(ref.threadId);
      if (value != null) {
        try {
          closer.accept(value);
        } catch (Exception e) {
          // Ignore exceptions, we can't crash the current thread
        }
      }
      ref = (RefWithThreadId) deadThreads.poll();
    }
  }

  int fallbackSize() {
    return fallback.size();
  }

  private static class RefWithThreadId extends PhantomReference<Thread> {
    private final long threadId;

    public RefWithThreadId(Thread thread, long threadId, ReferenceQueue<? super Thread> q) {
      super(thread, q);
      this.threadId = threadId;
    }
  }
}
