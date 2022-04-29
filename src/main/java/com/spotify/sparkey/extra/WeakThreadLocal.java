package com.spotify.sparkey.extra;

import java.util.Objects;
import java.util.WeakHashMap;
import java.util.function.Supplier;

public class WeakThreadLocal<T> extends ThreadLocal<T> {
  private final WeakHashMap<Thread, T> map = new WeakHashMap<>();
  private final Supplier<? extends T> supplier;

  private WeakThreadLocal(Supplier<? extends T> supplier) {
    this.supplier = supplier;
  }

  public static <S> WeakThreadLocal<S> withInitial(Supplier<? extends S> supplier) {
    return new WeakThreadLocal<>(supplier);
  }

  @Override
  public T get() {
    final Thread currentThread = Thread.currentThread();
    final T value = map.get(currentThread);
    if (value != null) {
      return value;
    }

    final T newValue = Objects.requireNonNull(supplier.get());
    map.put(currentThread, newValue);
    return newValue;
  }

  @Override
  public void set(T value) {
    map.put(Thread.currentThread(), value);
  }

  @Override
  public void remove() {
    map.remove(Thread.currentThread());
  }
}
