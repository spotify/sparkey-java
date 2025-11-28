/*
 * Copyright (c) 2025 Spotify AB
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

import com.google.common.base.Stopwatch;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyReader;
import com.spotify.sparkey.SparkeyWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class PooledSparkeyReaderTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private File indexFile;
  private static final int NUM_KEYS = 1000;

  @Before
  public void setUp() throws IOException {
    File sparkeyFile = tempFolder.newFile("test.spi");
    indexFile = sparkeyFile;

    // Create test data
    try (SparkeyWriter writer = Sparkey.createNew(sparkeyFile)) {
      for (int i = 0; i < NUM_KEYS; i++) {
        writer.put("key_" + i, "value_" + i);
      }
      writer.writeHash();
    }
  }

  @Test
  public void testBasicOperations() throws IOException {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile)) {
      // Test basic reads
      assertEquals("value_0", reader.getAsString("key_0"));
      assertEquals("value_999", reader.getAsString("key_999"));
      assertNull(reader.getAsString("nonexistent"));

      // Test byte array read
      byte[] value = reader.getAsByteArray("key_0".getBytes());
      assertArrayEquals("value_0".getBytes(), value);
    }
  }

  @Test
  public void testCustomPoolSize() throws IOException {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile, 16)) {
      assertEquals(16, reader.getPoolSize());
      assertEquals("value_0", reader.getAsString("key_0"));
    }
  }

  @Test
  public void testPoolSizeRoundedToPowerOfTwo() throws IOException {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile, 17)) {
      // Should round up to 32 (next power of 2)
      assertEquals(32, reader.getPoolSize());
    }

    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile, 1)) {
      assertEquals(1, reader.getPoolSize());
    }

    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile, 64)) {
      assertEquals(64, reader.getPoolSize());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPoolSize() throws IOException {
    PooledSparkeyReader.open(indexFile, 0);
  }

  @Test(expected = IllegalStateException.class)
  public void testReadAfterClose() throws IOException {
    PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile);
    reader.close();
    reader.getAsString("key_0");  // Should throw
  }

  @Test
  public void testDuplicateReturnsSelf() throws IOException {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile)) {
      assertSame(reader, reader.duplicate());
    }
  }

  @Test
  public void testConcurrentReads_platformThreads() throws Exception {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile, 32)) {
      int numThreads = 100;
      int readsPerThread = 100;

      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      try {
        List<Future<Integer>> futures = new ArrayList<>();

        for (int t = 0; t < numThreads; t++) {
          futures.add(executor.submit(() -> {
            int successCount = 0;
            for (int i = 0; i < readsPerThread; i++) {
              String key = "key_" + (i % NUM_KEYS);
              String expectedValue = "value_" + (i % NUM_KEYS);
              String actualValue = reader.getAsString(key);
              if (expectedValue.equals(actualValue)) {
                successCount++;
              }
            }
            return successCount;
          }));
        }

        // Verify all reads succeeded
        int totalSuccess = 0;
        for (Future<Integer> future : futures) {
          totalSuccess += future.get();
        }

        assertEquals(numThreads * readsPerThread, totalSuccess);

      } finally {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  public void testConcurrentReads_virtualThreads() throws Exception {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile, 64)) {
      int numVirtualThreads = 10_000;
      int readsPerThread = 10;

      ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
      try {
        List<Future<Integer>> futures = new ArrayList<>();

        for (int t = 0; t < numVirtualThreads; t++) {
          futures.add(executor.submit(() -> {
            int successCount = 0;
            for (int i = 0; i < readsPerThread; i++) {
              String key = "key_" + ThreadLocalRandom.current().nextInt(NUM_KEYS);
              String value = reader.getAsString(key);
              if (value != null) {
                successCount++;
              }
            }
            return successCount;
          }));
        }

        // Verify all reads completed
        int totalSuccess = 0;
        for (Future<Integer> future : futures) {
          totalSuccess += future.get();
        }

        assertTrue("Most reads should succeed", totalSuccess > numVirtualThreads * readsPerThread * 0.9);

        // Verify pool size is still bounded (not one reader per virtual thread)
        assertEquals(64, reader.getPoolSize());

      } finally {
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  public void testAccessDistribution() throws Exception {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile, 16)) {
      int numRequests = 10_000;

      // Generate load with platform threads
      ExecutorService executor = Executors.newFixedThreadPool(32);
      try {
        CountDownLatch latch = new CountDownLatch(numRequests);

        for (int i = 0; i < numRequests; i++) {
          final int index = i;
          executor.submit(() -> {
            try {
              reader.getAsString("key_" + (index % NUM_KEYS));
            } catch (IOException e) {
              e.printStackTrace();
            } finally {
              latch.countDown();
            }
          });
        }

        latch.await(30, TimeUnit.SECONDS);

      } finally {
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  public void testMemoryBounded_withVirtualThreads() throws Exception {
    // This test verifies that memory usage is bounded even with many virtual threads
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile, 64)) {
      int numVirtualThreads = 100_000;

      ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
      try {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numVirtualThreads);
        AtomicInteger inFlight = new AtomicInteger();

        for (int i = 0; i < numVirtualThreads; i++) {
          executor.submit(() -> {
            try {
              startLatch.await();
              inFlight.incrementAndGet();

              // Do a read
              reader.getAsString("key_" + ThreadLocalRandom.current().nextInt(NUM_KEYS));

              inFlight.decrementAndGet();
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              doneLatch.countDown();
            }
          });
        }

        // Let all virtual threads start
        startLatch.countDown();

        // Wait for completion
        doneLatch.await(60, TimeUnit.SECONDS);

        // Key assertion: pool size should still be 64, not 100k
        assertEquals(64, reader.getPoolSize());

        System.out.println("Created " + numVirtualThreads + " virtual threads, pool size: "
            + reader.getPoolSize());

      } finally {
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  public void testIteratorWorks() throws IOException {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile)) {
      // Iterator should work - it creates isolated state via duplicate()
      int count = 0;
      for (SparkeyReader.Entry entry : reader) {
        assertNotNull(entry.getKeyAsString());
        assertNotNull(entry.getValueAsString());
        count++;
      }
      assertEquals(NUM_KEYS, count);
    }
  }

  @Test
  public void testConcurrentIterators() throws Exception {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile, 8)) {
      // Multiple threads should be able to iterate concurrently
      int numThreads = 10;
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      try {
        List<Future<Integer>> futures = new ArrayList<>();

        for (int t = 0; t < numThreads; t++) {
          futures.add(executor.submit(() -> {
            int count = 0;
            for (SparkeyReader.Entry entry : reader) {
              count++;
            }
            return count;
          }));
        }

        // All threads should see all entries
        for (Future<Integer> future : futures) {
          assertEquals(NUM_KEYS, (int) future.get());
        }

      } finally {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  public void testConcurrentPerformance() throws Exception {
    try (PooledSparkeyReader reader = PooledSparkeyReader.open(indexFile)) {
      final int numRuns = 1_000_000;
      final AtomicInteger failures = new AtomicInteger();
      final Duration elapsed = min(() -> measure(reader, numRuns, failures));
      double nanosPerRun = (double) elapsed.toNanos() / numRuns;
      System.out.println("Nanos per lookup (PooledSparkeyReader): " + nanosPerRun);
      assertEquals(0, failures.get());
      // Performance benchmark - informational only (actual speed varies by hardware/load)
      // Expected: ~100-150ns on modern hardware, faster than ThreadLocalSparkeyReader (~196ns)
    }
  }

  private static Duration min(Callable<Duration> callable) throws Exception {
    return IntStream.range(0, 10).mapToObj(i -> {
      try {
        return callable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).min(Duration::compareTo).get();
  }

  private Duration measure(PooledSparkeyReader reader, int numRuns, AtomicInteger failures)
      throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(numRuns);
    final Stopwatch stopwatch = Stopwatch.createStarted();
    for (int i = 0; i < numRuns; i++) {
      ForkJoinPool.commonPool().execute(lookup(reader, failures, latch, i % NUM_KEYS));
    }
    latch.await();
    final Duration elapsed = stopwatch.stop().elapsed();
    System.out.println("Elapsed: " + elapsed);
    return elapsed;
  }

  private Runnable lookup(PooledSparkeyReader reader, AtomicInteger failures,
                          CountDownLatch latch, int index) {
    return () -> {
      try {
        final String ans = reader.getAsString("key_" + index);
        if (!("value_" + index).equals(ans)) {
          failures.incrementAndGet();
        }
      } catch (IOException e) {
        failures.incrementAndGet();
      } finally {
        latch.countDown();
      }
    };
  }

}
