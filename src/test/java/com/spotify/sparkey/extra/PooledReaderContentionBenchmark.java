/*
 * Copyright (c) 2026 Spotify AB
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

import com.spotify.sparkey.*;
import com.spotify.sparkey.UtilTest;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for PooledSparkeyReader under contention with simulated I/O latency.
 *
 * Simulates network-attached storage (e.g., Hyperdisk) where each lookup holds the
 * pool slot during a page fault (~50-1000μs). The SlowReader wrapper injects delay
 * inside the reader so the slot is held during the simulated I/O.
 *
 * Run via main() or:
 *   mvn test-compile exec:java \
 *     -Dexec.mainClass=com.spotify.sparkey.extra.PooledReaderContentionBenchmark \
 *     -Dexec.classpathScope=test
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class PooledReaderContentionBenchmark {

  private static final int NUM_KEYS = 10_000;
  private static final int KEY_SIZE = 36;
  private static final int VALUE_SIZE = 400;

  @Param({"16"})
  public int poolSize;

  @Param({"0", "50", "200"})
  public int ioDelayMicros;

  private File indexFile;
  private File logFile;
  private PooledSparkeyReader reader;
  private String[] keys;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    indexFile = File.createTempFile("sparkey-contention", ".spi");
    logFile = Sparkey.getLogFile(indexFile);
    indexFile.deleteOnExit();
    logFile.deleteOnExit();
    UtilTest.delete(indexFile);
    UtilTest.delete(logFile);

    // Create test data with realistic key/value sizes
    keys = new String[NUM_KEYS];
    try (SparkeyWriter writer = Sparkey.createNew(indexFile)) {
      for (int i = 0; i < NUM_KEYS; i++) {
        keys[i] = padTo("key_", i, KEY_SIZE);
        writer.put(keys[i], padTo("val_", i, VALUE_SIZE));
      }
      writer.writeHash();
    }

    // Open with SlowReader wrapper so delay happens while pool slot is held
    SparkeyReader baseReader = Sparkey.openSingleThreadedReader(indexFile);
    SparkeyReader slowBase = new SlowReader(baseReader, ioDelayMicros * 1000L);
    reader = PooledSparkeyReader.fromReader(slowBase, poolSize);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    reader.close();
    UtilTest.delete(indexFile);
    UtilTest.delete(logFile);
  }

  @Benchmark
  @Threads(1)
  public String threads_001() throws IOException {
    return doLookup();
  }

  @Benchmark
  @Threads(4)
  public String threads_004() throws IOException {
    return doLookup();
  }

  @Benchmark
  @Threads(16)
  public String threads_016() throws IOException {
    return doLookup();
  }

  @Benchmark
  @Threads(64)
  public String threads_064() throws IOException {
    return doLookup();
  }

  @Benchmark
  @Threads(256)
  public String threads_256() throws IOException {
    return doLookup();
  }

  private String doLookup() throws IOException {
    int idx = ThreadLocalRandom.current().nextInt(NUM_KEYS);
    return reader.getAsString(keys[idx]);
  }

  private static String padTo(String prefix, int i, int length) {
    StringBuilder sb = new StringBuilder(length);
    sb.append(prefix);
    String suffix = String.valueOf(i);
    for (int p = sb.length(); p < length - suffix.length(); p++) sb.append('0');
    sb.append(suffix);
    return sb.toString();
  }

  /**
   * SparkeyReader wrapper that adds sleep delay to simulate network storage latency.
   * The delay occurs while the PooledSparkeyReader's pool slot is held, modeling
   * real I/O where the thread is descheduled during a page fault.
   */
  static class SlowReader implements SparkeyReader {
    private final SparkeyReader delegate;
    private final long delayNanos;

    SlowReader(SparkeyReader delegate, long delayNanos) {
      this.delegate = delegate;
      this.delayNanos = delayNanos;
    }

    private void simulateIO() {
      if (delayNanos > 0) {
        java.util.concurrent.locks.LockSupport.parkNanos(delayNanos);
      }
    }

    @Override public String getAsString(String key) throws IOException {
      simulateIO();
      return delegate.getAsString(key);
    }
    @Override public byte[] getAsByteArray(byte[] key) throws IOException {
      simulateIO();
      return delegate.getAsByteArray(key);
    }
    @Override public Entry getAsEntry(byte[] key) throws IOException {
      simulateIO();
      return delegate.getAsEntry(key);
    }
    @Override public SparkeyReader duplicate() {
      return new SlowReader(delegate.duplicate(), delayNanos);
    }
    @Override public IndexHeader getIndexHeader() { return delegate.getIndexHeader(); }
    @Override public LogHeader getLogHeader() { return delegate.getLogHeader(); }
    @Override public void close() { delegate.close(); }
    @Override public Iterator<Entry> iterator() { return delegate.iterator(); }
    @Override public long getLoadedBytes() { return delegate.getLoadedBytes(); }
    @Override public long getTotalBytes() { return delegate.getTotalBytes(); }
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(PooledReaderContentionBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}
