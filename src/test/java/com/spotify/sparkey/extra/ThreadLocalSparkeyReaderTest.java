package com.spotify.sparkey.extra;

import com.google.common.base.Stopwatch;
import com.spotify.sparkey.CompressionType;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyReader;
import com.spotify.sparkey.SparkeyWriter;
import com.spotify.sparkey.system.BaseSystemTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class ThreadLocalSparkeyReaderTest extends BaseSystemTest {
  private ThreadLocalSparkeyReader reader;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024);
    for (int i = 0; i < 10000; i++) {
      writer.put("key_" + i, "value_"+i);
    }
    writer.writeHash();
    writer.close();

    reader = new ThreadLocalSparkeyReader(indexFile);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (reader != null) {
      reader.close();
    }
    super.tearDown();
  }

  @Test
  public void testCorrectness() throws IOException {
    for (int i=0; i<10; i++) {
      assertEquals("value_"+i, reader.getAsString("key_"+i));
    }
  }

  @Test
  public void testConcurrentUsageCorrectness() {
    IntStream stream = IntStream.range(0, 10000);
    stream.parallel().forEach(i -> {
      try {
        assertEquals("value_"+i, reader.getAsString("key_"+i));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testConcurrentPerformance() throws Exception {
    final int numRuns = 1_000_000;
    final AtomicInteger failures = new AtomicInteger();
    final Duration elapsed = min(() -> measure(numRuns, failures));
    double nanosPerRun = (double) elapsed.toNanos() / numRuns;
    System.out.println("Nanos per lookup: " + nanosPerRun);
    assertEquals(0, failures.get());
    assertTrue(nanosPerRun < 400);
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

  private Duration measure(int numRuns, AtomicInteger failures) throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(numRuns);
    final Stopwatch stopwatch = Stopwatch.createStarted();
    for (int i = 0; i < numRuns; i++) {
      ForkJoinPool.commonPool().execute(lookup(failures, latch, i % 10000));
    }
    latch.await();
    final Duration elapsed = stopwatch.stop().elapsed();
    System.out.println("Elapsed: " + elapsed);
    return elapsed;
  }

  private Runnable lookup(AtomicInteger failures, CountDownLatch latch, int index) {
    return () -> {
      try {
        final String ans = reader.getAsString("key_" + index);
        if (!ans.equals("value_" + index)) {
          failures.incrementAndGet();
        }
      } catch (IOException e) {
        failures.incrementAndGet();
      } finally {
        latch.countDown();
      }
    };
  }

  @Test
  public void testDifferentReaders() throws Exception {
    final SparkeyReader localReader = reader.getDelegateReader();

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final SparkeyReader remoteReader = executorService.submit(() -> reader.getDelegateReader()).get();
    executorService.shutdown();

    assertNotSame(localReader, remoteReader);
  }

  @Test(expected = IllegalStateException.class)
  public void testAlreadyClosed() throws IOException {
    reader.close();
    reader.getAsString("key_1");
  }

}
