package com.spotify.sparkey;

import java.io.File;
import java.io.IOException;

/**
 * Test helper class for accessing package-private SparkeyImplSelector methods.
 * This class is in the same package as SparkeyImplSelector to access package-private members.
 */
public class SparkeyTestHelper {

  /**
   * Open an uncompressed reader using Java 22+ MemorySegment API.
   * Delegates to SparkeyImplSelector.openUncompressedJ22().
   *
   * @param file File base to use
   * @return UncompressedSparkeyReaderJ22 (on Java 22+)
   * @throws UnsupportedOperationException on Java &lt; 22
   * @throws IOException if the file cannot be opened
   */
  public static SparkeyReader openUncompressedJ22(File file) throws IOException {
    return SparkeyImplSelector.openUncompressedJ22(file);
  }

  /**
   * Open a single-threaded reader using Java 22+ MemorySegment API.
   * Delegates to SparkeyImplSelector.openSingleThreadedJ22().
   *
   * @param file File base to use
   * @return SingleThreadedSparkeyReaderJ22 (on Java 22+)
   * @throws UnsupportedOperationException on Java &lt; 22
   * @throws IOException if the file cannot be opened
   */
  public static SparkeyReader openSingleThreadedJ22(File file) throws IOException {
    return SparkeyImplSelector.openSingleThreadedJ22(file);
  }
}
