package com.spotify.sparkey;

import com.spotify.sparkey.extra.PooledSparkeyReader;

import java.io.File;
import java.io.IOException;

/**
 * Java 22+ implementation selector.
 *
 * This Multi-Release JAR override provides optimized Sparkey implementations
 * using Java 22 features like the Foreign Function & Memory API (MemorySegment, Arena).
 *
 * Benefits of Java 22 implementation:
 * - No 2GB chunk limit (unlike FileChannel/MappedByteBuffer)
 * - Vectorized comparison using MemorySegment.mismatch() (SIMD optimization)
 * - Arena-based deterministic memory management
 * - Better performance for large files
 * - UncompressedSparkeyReaderJ22 for zero-overhead uncompressed reads
 * - Smart reader selection based on compression type
 */
class SparkeyImplSelector {

  /**
   * Open a SparkeyReader with the optimal implementation for Java 22+.
   * Uses UncompressedSparkeyReaderJ22 for uncompressed files (zero-overhead, fully thread-safe).
   * Uses SingleThreadedSparkeyReaderJ22 for compressed files.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return an optimal SparkeyReader for Java 22+
   * @throws IOException if the file cannot be opened
   */
  static SparkeyReader open(File file) throws IOException {
    File logFile = Sparkey.getLogFile(file);
    LogHeader logHeader = LogHeader.read(logFile);

    // Use uncompressed reader for uncompressed files - immutable, zero overhead!
    if (logHeader.getCompressionType() == CompressionType.NONE) {
      return UncompressedSparkeyReaderJ22.open(file);
    }

    // Fall back to SingleThreadedSparkeyReader for compressed files
    return SingleThreadedSparkeyReaderJ22.open(file);
  }

  /**
   * Open a single-threaded SparkeyReader using Java 22 MemorySegment API.
   * For uncompressed files, returns UncompressedSparkeyReaderJ22 (already thread-safe).
   * For compressed files, returns SingleThreadedSparkeyReaderJ22.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return a single-threaded SparkeyReader
   * @throws IOException if the file cannot be opened
   */
  static SparkeyReader openSingleThreaded(File file) throws IOException {
    File logFile = Sparkey.getLogFile(file);
    LogHeader logHeader = LogHeader.read(logFile);

    if (logHeader.getCompressionType() == CompressionType.NONE) {
      return UncompressedSparkeyReaderJ22.open(file);
    }

    return SingleThreadedSparkeyReaderJ22.open(file);
  }

  /**
   * Open a pooled SparkeyReader with default pool size using Java 22 optimizations.
   * For uncompressed files, returns UncompressedSparkeyReaderJ22 (already zero-overhead thread-safe, pooling not needed).
   * For compressed files, returns PooledSparkeyReader wrapping SingleThreadedSparkeyReaderJ22 instances.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @return a pooled SparkeyReader
   * @throws IOException if the file cannot be opened
   */
  static SparkeyReader openPooled(File file) throws IOException {
    File logFile = Sparkey.getLogFile(file);
    LogHeader logHeader = LogHeader.read(logFile);

    // For uncompressed files, the uncompressed reader is already zero-overhead thread-safe (immutable)
    // Pooling would just add unnecessary overhead, so return it directly
    if (logHeader.getCompressionType() == CompressionType.NONE) {
      return UncompressedSparkeyReaderJ22.open(file);
    }

    // For compressed files, pool SingleThreadedSparkeyReaderJ22 instances
    SparkeyReader baseReader = SingleThreadedSparkeyReaderJ22.open(file);
    return PooledSparkeyReader.fromReader(baseReader);
  }

  /**
   * Open a pooled SparkeyReader with the specified pool size using Java 22 optimizations.
   * For uncompressed files, returns UncompressedSparkeyReaderJ22 (already zero-overhead thread-safe, pooling not needed).
   * For compressed files, returns PooledSparkeyReader wrapping SingleThreadedSparkeyReaderJ22 instances.
   *
   * @param file File base to use, the actual file endings will be set to .spi and .spl
   * @param poolSize number of reader instances (minimum 1, ignored for uncompressed files)
   * @return a pooled SparkeyReader
   * @throws IOException if the file cannot be opened
   */
  static SparkeyReader openPooled(File file, int poolSize) throws IOException {
    File logFile = Sparkey.getLogFile(file);
    LogHeader logHeader = LogHeader.read(logFile);

    // For uncompressed files, the uncompressed reader is already zero-overhead thread-safe (immutable)
    // Pooling would just add unnecessary overhead, so return it directly
    if (logHeader.getCompressionType() == CompressionType.NONE) {
      return UncompressedSparkeyReaderJ22.open(file);
    }

    // For compressed files, pool SingleThreadedSparkeyReaderJ22 instances
    SparkeyReader baseReader = SingleThreadedSparkeyReaderJ22.open(file);
    return PooledSparkeyReader.fromReader(baseReader, poolSize);
  }
}
