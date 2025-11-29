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
package com.spotify.sparkey.benchmark;

import com.spotify.sparkey.*;
import com.spotify.sparkey.extra.PooledSparkeyReader;
import com.spotify.sparkey.extra.ThreadLocalSparkeyReader;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Standalone performance benchmark for Sparkey readers.
 * Compares different reader implementations on both uncompressed and compressed data.
 */
public class PerformanceBenchmark {

  private static final int NUM_ENTRIES = 100_000;
  private static final int WARMUP_ITERATIONS = 100_000;
  private static final int BENCHMARK_ITERATIONS = 1_000_000;

  public static void main(String[] args) {
    try {
      new PerformanceBenchmark().run();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  private void run() throws IOException {
    System.out.println("Sparkey Performance Benchmark");
    System.out.println("=============================");
    System.out.println();
    System.out.println("Configuration:");
    System.out.println("  Entries: " + NUM_ENTRIES);
    System.out.println("  Warmup iterations: " + WARMUP_ITERATIONS);
    System.out.println("  Benchmark iterations: " + BENCHMARK_ITERATIONS);
    System.out.println();

    // Create test files
    File uncompressedIndex = createTestFile(CompressionType.NONE, "uncompressed");
    File snappyIndex = createTestFile(CompressionType.SNAPPY, "snappy");

    System.out.println();
    System.out.println("=============================");
    System.out.println("UNCOMPRESSED FILES");
    System.out.println("=============================");
    System.out.println();

    List<BenchmarkResult> uncompressedResults = new ArrayList<>();

    // Benchmark SingleThreadedSparkeyReader
    try (SparkeyReader reader = Sparkey.openSingleThreadedReader(uncompressedIndex)) {
      double ns = benchmarkReader(reader, "SingleThreadedSparkeyReader");
      uncompressedResults.add(new BenchmarkResult("SingleThreadedSparkeyReader", ns));
    }

    // Benchmark ThreadLocalSparkeyReader
    try (SparkeyReader reader = Sparkey.openThreadLocalReader(uncompressedIndex)) {
      double ns = benchmarkReader(reader, "ThreadLocalSparkeyReader");
      uncompressedResults.add(new BenchmarkResult("ThreadLocalSparkeyReader", ns));
    }

    // Benchmark PooledSparkeyReader
    try (SparkeyReader reader = PooledSparkeyReader.open(uncompressedIndex)) {
      double ns = benchmarkReader(reader, "PooledSparkeyReader");
      uncompressedResults.add(new BenchmarkResult("PooledSparkeyReader", ns));
    }

    System.out.println();
    System.out.println("=============================");
    System.out.println("COMPRESSED FILES (Snappy)");
    System.out.println("=============================");
    System.out.println();

    List<BenchmarkResult> compressedResults = new ArrayList<>();

    // Benchmark SingleThreadedSparkeyReader
    try (SparkeyReader reader = Sparkey.openSingleThreadedReader(snappyIndex)) {
      double ns = benchmarkReader(reader, "SingleThreadedSparkeyReader");
      compressedResults.add(new BenchmarkResult("SingleThreadedSparkeyReader", ns));
    }

    // Benchmark ThreadLocalSparkeyReader
    try (SparkeyReader reader = Sparkey.openThreadLocalReader(snappyIndex)) {
      double ns = benchmarkReader(reader, "ThreadLocalSparkeyReader");
      compressedResults.add(new BenchmarkResult("ThreadLocalSparkeyReader", ns));
    }

    // Benchmark PooledSparkeyReader
    try (SparkeyReader reader = PooledSparkeyReader.open(snappyIndex)) {
      double ns = benchmarkReader(reader, "PooledSparkeyReader");
      compressedResults.add(new BenchmarkResult("PooledSparkeyReader", ns));
    }

    // Print summary
    System.out.println();
    System.out.println("=============================");
    System.out.println("SUMMARY");
    System.out.println("=============================");
    System.out.println();

    System.out.println("Uncompressed Performance:");
    System.out.println("-----------------------");
    printSummaryTable(uncompressedResults);

    System.out.println();
    System.out.println("Compressed Performance:");
    System.out.println("---------------------");
    printSummaryTable(compressedResults);

    // Cleanup
    cleanupFile(uncompressedIndex);
    cleanupFile(snappyIndex);
  }

  private File createTestFile(CompressionType compressionType, String suffix) throws IOException {
    File indexFile = File.createTempFile("sparkey-bench-" + suffix, ".spi");
    indexFile.deleteOnExit();
    File logFile = new File(indexFile.getParent(),
      indexFile.getName().replace(".spi", ".spl"));
    logFile.deleteOnExit();

    System.out.println("Creating test file: " + compressionType + " (" + suffix + ")");

    try (SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, 1024)) {
      for (int i = 0; i < NUM_ENTRIES; i++) {
        writer.put("key" + i, "value" + i + "-" + repeatString("x", 50));
      }
      writer.writeHash();
    }

    long logSize = logFile.length();
    long indexSize = indexFile.length();
    System.out.println("  Log size: " + formatBytes(logSize));
    System.out.println("  Index size: " + formatBytes(indexSize));
    System.out.println("  Total size: " + formatBytes(logSize + indexSize));

    return indexFile;
  }

  private double benchmarkReader(SparkeyReader reader, String name) throws IOException {
    System.out.println("Benchmarking " + name + "...");
    Random random = new Random(12345);

    // Warmup
    System.out.print("  Warming up...");
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      int key = random.nextInt(NUM_ENTRIES);
      String value = reader.getAsString("key" + key);
      if (value == null) {
        throw new AssertionError("Key not found: key" + key);
      }
    }
    System.out.println(" done");

    // Benchmark
    System.out.print("  Measuring performance...");
    random = new Random(12345);  // Reset for consistent keys
    Instant start = Instant.now();

    for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
      int key = random.nextInt(NUM_ENTRIES);
      String value = reader.getAsString("key" + key);
      if (value == null) {
        throw new AssertionError("Key not found: key" + key);
      }
    }

    Duration elapsed = Duration.between(start, Instant.now());
    double nsPerLookup = (double) elapsed.toNanos() / BENCHMARK_ITERATIONS;

    System.out.println(" done");
    System.out.printf("  Result: %.2f ns/lookup%n", nsPerLookup);
    System.out.println();

    return nsPerLookup;
  }

  private void printSummaryTable(List<BenchmarkResult> results) {
    // Find baseline (fastest)
    double baseline = results.stream()
      .mapToDouble(r -> r.nsPerLookup)
      .min()
      .orElse(0);

    System.out.printf("%-30s %15s %10s%n", "Implementation", "ns/lookup", "vs Fastest");
    System.out.println(repeatString("-", 57));

    for (BenchmarkResult result : results) {
      double ratio = result.nsPerLookup / baseline;
      String comparison = ratio == 1.0 ? "(fastest)" : String.format("%.2fx", ratio);
      System.out.printf("%-30s %15.2f %10s%n",
        result.name, result.nsPerLookup, comparison);
    }
  }

  private String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    } else if (bytes < 1024 * 1024) {
      return String.format("%.2f KB", bytes / 1024.0);
    } else {
      return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
    }
  }

  private void cleanupFile(File indexFile) {
    indexFile.delete();
    File logFile = new File(indexFile.getParent(),
      indexFile.getName().replace(".spi", ".spl"));
    logFile.delete();
  }

  private static String repeatString(String s, int count) {
    StringBuilder sb = new StringBuilder(s.length() * count);
    for (int i = 0; i < count; i++) {
      sb.append(s);
    }
    return sb.toString();
  }

  private static class BenchmarkResult {
    final String name;
    final double nsPerLookup;

    BenchmarkResult(String name, double nsPerLookup) {
      this.name = name;
      this.nsPerLookup = nsPerLookup;
    }
  }
}
