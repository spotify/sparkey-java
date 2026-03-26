package com.spotify.sparkey;

import java.io.File;
import java.io.IOException;

/**
 * Test helper class for accessing package-private reader types and builder internals.
 * This class is in the same package as the implementation to access package-private members.
 */
public class SparkeyTestHelper {

  /**
   * Open an uncompressed reader using Java 22+ MemorySegment API.
   * Forces UncompressedSparkeyReaderJ22 on Java 22+, throws on older JVMs.
   */
  public static SparkeyReader openUncompressedJ22(File file) throws IOException {
    return SparkeyImplSelector.openUncompressedJ22(Sparkey.getIndexFile(file), Sparkey.getLogFile(file));
  }

  /**
   * Open a single-threaded reader using Java 22+ MemorySegment API.
   * Forces SingleThreadedSparkeyReaderJ22 on Java 22+, throws on older JVMs.
   */
  public static SparkeyReader openSingleThreadedJ22(File file) throws IOException {
    return SparkeyImplSelector.openSingleThreadedJ22(Sparkey.getIndexFile(file), Sparkey.getLogFile(file));
  }

  /**
   * Open a heap-backed reader for testing.
   */
  public static SparkeyReader openHeapBacked(File file) throws IOException {
    return Sparkey.reader().file(file).useHeap(true).open();
  }

  /**
   * Open a heap-backed single-threaded reader for testing.
   */
  public static SparkeyReader openHeapBackedSingleThreaded(File file) throws IOException {
    return Sparkey.reader().file(file).useHeap(true).singleThreaded(true).open();
  }
}
