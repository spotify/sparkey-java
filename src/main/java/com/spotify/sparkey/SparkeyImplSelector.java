package com.spotify.sparkey;

import com.spotify.sparkey.extra.PooledSparkeyReader;

import java.io.File;
import java.io.IOException;

/**
 * Selects the appropriate Sparkey implementation based on the Java version.
 *
 * This class is overridden via Multi-Release JAR to provide optimized
 * implementations on Java 22+. The base implementation uses Java 8-compatible
 * FileChannel-based readers.
 */
class SparkeyImplSelector {

  /**
   * Open a SparkeyReader with the optimal implementation for the current Java version.
   *
   * Base implementation (Java 8-21): Returns PooledSparkeyReader using FileChannel.
   * Java 22+ override: Returns optimized implementations using MemorySegment API.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return an optimal SparkeyReader for the current Java version
   * @throws IOException if the file cannot be opened
   */
  static SparkeyReader open(File file) throws IOException {
    return PooledSparkeyReader.open(file);
  }

  /**
   * Open a single-threaded SparkeyReader.
   *
   * This is not thread-safe and should only be used from one thread.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return a single-threaded SparkeyReader
   * @throws IOException if the file cannot be opened
   */
  static SparkeyReader openSingleThreaded(File file) throws IOException {
    return SingleThreadedSparkeyReader.open(file);
  }

  /**
   * Open a pooled SparkeyReader with default pool size.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return a pooled SparkeyReader
   * @throws IOException if the file cannot be opened
   */
  static SparkeyReader openPooled(File file) throws IOException {
    return PooledSparkeyReader.open(file);
  }

  /**
   * Open a pooled SparkeyReader with the specified pool size.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @param poolSize number of reader instances (minimum 1)
   * @return a pooled SparkeyReader
   * @throws IOException if the file cannot be opened
   */
  static SparkeyReader openPooled(File file, int poolSize) throws IOException {
    return PooledSparkeyReader.open(file, poolSize);
  }
}
