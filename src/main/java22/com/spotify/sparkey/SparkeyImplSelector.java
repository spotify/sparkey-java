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
   * Open a SparkeyReader configured by the builder.
   * Java 22+ override: uses MemorySegment-based implementations for mmap,
   * falls back to ReadOnlyMemMap for heap-backed mode.
   */
  static SparkeyReader openUncompressedJ22(File indexFile, File logFile) throws IOException {
    LogHeader logHeader = LogHeader.read(logFile);
    return UncompressedSparkeyReaderJ22.open(indexFile, logFile, logHeader);
  }

  static SparkeyReader openSingleThreadedJ22(File indexFile, File logFile) throws IOException {
    return SingleThreadedSparkeyReaderJ22.open(indexFile, logFile);
  }

  static SparkeyReader open(SparkeyReaderBuilder builder) throws IOException {
    File indexFile = builder.indexFile();
    File logFile = builder.logFile();

    if (builder.isHeapBacked()) {
      // Heap-backed: use ReadOnlyMemMap path (no MemorySegment needed)
      if (builder.isSingleThreaded()) {
        return SingleThreadedSparkeyReader.open(indexFile, logFile, true);
      }
      SparkeyReader base = SingleThreadedSparkeyReader.open(indexFile, logFile, true);
      if (builder.poolSize() > 0) {
        return PooledSparkeyReader.fromReader(base, builder.poolSize());
      }
      return PooledSparkeyReader.fromReader(base);
    }

    // mmap: use Java 22+ optimized implementations
    LogHeader logHeader = LogHeader.read(logFile);

    if (builder.isSingleThreaded()) {
      if (logHeader.getCompressionType() == CompressionType.NONE) {
        return UncompressedSparkeyReaderJ22.open(indexFile, logFile, logHeader);
      }
      return SingleThreadedSparkeyReaderJ22.open(indexFile, logFile);
    }

    // For uncompressed files, the uncompressed reader is already zero-overhead thread-safe (immutable)
    if (logHeader.getCompressionType() == CompressionType.NONE) {
      return UncompressedSparkeyReaderJ22.open(indexFile, logFile, logHeader);
    }

    // For compressed files, pool SingleThreadedSparkeyReaderJ22 instances
    SparkeyReader baseReader = SingleThreadedSparkeyReaderJ22.open(indexFile, logFile);
    if (builder.poolSize() > 0) {
      return PooledSparkeyReader.fromReader(baseReader, builder.poolSize());
    }
    return PooledSparkeyReader.fromReader(baseReader);
  }
}
