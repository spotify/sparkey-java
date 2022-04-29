package com.spotify.sparkey.extra;

import com.google.common.collect.Streams;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class ThreadLocalWithFallbackTest {
  @Test
  public void testSurviveCommonPool() {
    final AtomicInteger counter = new AtomicInteger();
    final ThreadLocalWithFallback<Integer> threadLocalWithFallback = ThreadLocalWithFallback.withInitial(counter::incrementAndGet);

    final List<ForkJoinTask<Integer>> tasks = IntStream.range(0, 100000)
            .mapToObj(i -> ForkJoinPool.commonPool().submit(threadLocalWithFallback::get))
            .collect(Collectors.toList());
    final int max = tasks.stream().mapToInt(ForkJoinTask::join).max().getAsInt();
    System.out.println(max);
    assertTrue(max < 100);
  }
}