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
   * Open a SparkeyReader configured by the builder.
   * Overridden by the Java 22+ MRJAR variant to use optimized implementations.
   *
   * @param builder the reader configuration
   * @return a SparkeyReader
   * @throws IOException if the file cannot be opened
   */
  /**
   * Open an uncompressed J22 reader. Overridden in J22 MRJAR variant.
   * Used by tests to force a specific implementation.
   */
  static SparkeyReader openUncompressedJ22(File indexFile, File logFile) throws IOException {
    throw new UnsupportedOperationException("Requires Java 22+");
  }

  /**
   * Open a single-threaded J22 reader. Overridden in J22 MRJAR variant.
   * Used by tests to force a specific implementation.
   */
  static SparkeyReader openSingleThreadedJ22(File indexFile, File logFile) throws IOException {
    throw new UnsupportedOperationException("Requires Java 22+");
  }

  static SparkeyReader open(SparkeyReaderBuilder builder) throws IOException {
    File indexFile = builder.indexFile();
    File logFile = builder.logFile();
    boolean heapBacked = builder.isHeapBacked();

    if (builder.isSingleThreaded()) {
      return SingleThreadedSparkeyReader.open(indexFile, logFile, heapBacked);
    }
    SparkeyReader base = SingleThreadedSparkeyReader.open(indexFile, logFile, heapBacked);
    if (builder.poolSize() > 0) {
      return PooledSparkeyReader.fromReader(base, builder.poolSize());
    }
    return PooledSparkeyReader.fromReader(base);
  }
}
