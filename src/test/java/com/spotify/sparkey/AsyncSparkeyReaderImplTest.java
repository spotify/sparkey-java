package com.spotify.sparkey;

import com.spotify.sparkey.system.BaseSystemTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;

import static org.junit.Assert.*;

public class AsyncSparkeyReaderImplTest extends BaseSystemTest {
  @Before
  public void setUp() throws Exception {
    super.setUp();
    try (SparkeyWriter writer = Sparkey.createNew(indexFile)) {
      writer.put("key1", "value1");
      writer.put("key2", "value2");
      writer.writeHash();
    }
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testSimple() throws IOException {
    testWithThreads(1);
  }

  @Test
  public void testMultiThreads() throws IOException {
    testWithThreads(2);
  }

  @Test
  public void testMoreThreads() throws IOException {
    testWithThreads(10);
  }

  private void testWithThreads(final int numThreads) throws IOException {
    final List<CompletionStage<String>> futures = new ArrayList<>();
    try (AsyncSparkeyReader reader = Sparkey.openAsyncReader(indexFile, numThreads)) {
      createFutures(futures, reader);
    }
    assertFutures(futures);
  }

  @Test
  public void testForkJoinPool() throws IOException {
    final List<CompletionStage<String>> futures = new ArrayList<>();
    try (AsyncSparkeyReader reader = Sparkey.openAsyncReader(indexFile, ForkJoinPool.commonPool())) {
      createFutures(futures, reader);
    }
    assertFutures(futures);
  }

  private void assertFutures(final List<CompletionStage<String>> futures) {
    for (int i = 0; i < 1000; i++) {
      final CompletableFuture<String> future = futures.get(i).toCompletableFuture();
      assertTrue(future.isDone());
      assertFalse(future.isCompletedExceptionally());
      assertEquals("value1", future.join());
    }
    for (int i = 0; i < 1000; i++) {
      final CompletableFuture<String> future = futures.get(1000 + i).toCompletableFuture();
      assertTrue(future.isDone());
      assertFalse(future.isCompletedExceptionally());
      assertEquals("value2", future.join());
    }
    for (int i = 0; i < 1000; i++) {
      final CompletableFuture<String> future = futures.get(2000 + i).toCompletableFuture();
      assertTrue(future.isDone());
      assertFalse(future.isCompletedExceptionally());
      assertEquals(null, future.join());
    }
  }

  private void createFutures(List<CompletionStage<String>> futures, AsyncSparkeyReader reader) {
    for (int i = 0; i < 1000; i++) {
      futures.add(reader.getAsString("key1"));
    }
    for (int i = 0; i < 1000; i++) {
      futures.add(reader.getAsString("key2"));
    }
    for (int i = 0; i < 1000; i++) {
      futures.add(reader.getAsString("key3"));
    }
  }

  private void createFile() throws IOException {
    try (SparkeyWriter writer = Sparkey.createNew(indexFile)) {
      writer.put("key1", "value1");
      writer.put("key2", "value2");
      writer.writeHash();
    }
  }
}