package com.spotify.sparkey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.spotify.sparkey.extra.PooledSparkeyReader;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that SparkeyReaderBuilder returns the correct reader type for each configuration.
 */
public class SparkeyReaderBuilderTest extends OpenMapsAsserter {
  private File indexFile;
  private File logFile;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    UtilTest.setMapBits(10);
    indexFile = File.createTempFile("sparkey", ".spi");
    logFile = Sparkey.getLogFile(indexFile);
    indexFile.deleteOnExit();
    logFile.deleteOnExit();
  }

  @After
  public void tearDown() throws Exception {
    UtilTest.delete(indexFile);
    UtilTest.delete(logFile);
    super.tearDown();
  }

  private void writeTestData(CompressionType compression, int blockSize) throws Exception {
    SparkeyWriter writer;
    if (compression == CompressionType.NONE) {
      writer = Sparkey.createNew(indexFile);
    } else {
      writer = Sparkey.createNew(indexFile, compression, blockSize);
    }
    writer.put("key", "value");
    writer.writeHash();
    writer.close();
  }

  // --- Uncompressed ---

  @Test
  public void defaultUncompressed() throws Exception {
    writeTestData(CompressionType.NONE, 0);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).open()) {
      assertReaderType(reader, "uncompressed", false);
    }
  }

  @Test
  public void singleThreadedUncompressed() throws Exception {
    writeTestData(CompressionType.NONE, 0);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).singleThreaded().open()) {
      assertReaderType(reader, "uncompressed", true);
    }
  }

  @Test
  public void heapUncompressed() throws Exception {
    writeTestData(CompressionType.NONE, 0);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).useHeap().open()) {
      assertTrue("Expected PooledSparkeyReader, got " + reader.getClass().getSimpleName(),
          reader instanceof PooledSparkeyReader);
      // Verify the pooled reader wraps a heap-backed SingleThreadedSparkeyReader
      SparkeyReader dup = reader.duplicate();
      assertTrue("Pooled heap reader should return PooledSparkeyReader from duplicate()",
          dup instanceof PooledSparkeyReader);
    }
  }

  @Test
  public void heapSingleThreadedUncompressed() throws Exception {
    writeTestData(CompressionType.NONE, 0);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).useHeap().singleThreaded().open()) {
      assertEquals("SingleThreadedSparkeyReader", reader.getClass().getSimpleName());
    }
  }

  // --- Compressed (Snappy) ---

  @Test
  public void defaultCompressed() throws Exception {
    writeTestData(CompressionType.SNAPPY, 1024);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).open()) {
      assertTrue("Expected PooledSparkeyReader for compressed, got " + reader.getClass().getSimpleName(),
          reader instanceof PooledSparkeyReader);
    }
  }

  @Test
  public void singleThreadedCompressed() throws Exception {
    writeTestData(CompressionType.SNAPPY, 1024);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).singleThreaded().open()) {
      assertReaderType(reader, "compressed", true);
    }
  }

  @Test
  public void heapCompressed() throws Exception {
    writeTestData(CompressionType.SNAPPY, 1024);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).useHeap().open()) {
      assertTrue("Expected PooledSparkeyReader, got " + reader.getClass().getSimpleName(),
          reader instanceof PooledSparkeyReader);
    }
  }

  @Test
  public void heapSingleThreadedCompressed() throws Exception {
    writeTestData(CompressionType.SNAPPY, 1024);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).useHeap().singleThreaded().open()) {
      assertEquals("SingleThreadedSparkeyReader", reader.getClass().getSimpleName());
    }
  }

  // --- Pool size ---

  @Test
  public void poolSizeUncompressed() throws Exception {
    writeTestData(CompressionType.NONE, 0);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).poolSize(4).open()) {
      assertReaderType(reader, "uncompressed-pooled", false);
    }
  }

  @Test
  public void poolSizeCompressed() throws Exception {
    writeTestData(CompressionType.SNAPPY, 1024);
    try (SparkeyReader reader = Sparkey.reader().file(indexFile).poolSize(4).open()) {
      assertTrue("Expected PooledSparkeyReader, got " + reader.getClass().getSimpleName(),
          reader instanceof PooledSparkeyReader);
    }
  }

  // --- Builder validation ---

  @Test(expected = IllegalStateException.class)
  public void openWithoutFile() throws Exception {
    Sparkey.reader().open();
  }

  @Test(expected = IllegalStateException.class)
  public void openWithOnlyIndexFile() throws Exception {
    Sparkey.reader().indexFile(indexFile).open();
  }

  @Test(expected = IllegalStateException.class)
  public void openWithOnlyLogFile() throws Exception {
    Sparkey.reader().logFile(logFile).open();
  }

  @Test
  public void explicitIndexAndLogFiles() throws Exception {
    writeTestData(CompressionType.NONE, 0);
    try (SparkeyReader reader = Sparkey.reader().indexFile(indexFile).logFile(logFile).open()) {
      assertEquals("value", reader.getAsString("key"));
    }
  }

  @Test
  public void customFileNamesAreRespected() throws Exception {
    writeTestData(CompressionType.NONE, 0);
    // Copy files to non-standard names
    File customIndex = File.createTempFile("custom-idx", ".dat");
    File customLog = File.createTempFile("custom-log", ".dat");
    customIndex.deleteOnExit();
    customLog.deleteOnExit();
    java.nio.file.Files.copy(indexFile.toPath(), customIndex.toPath(),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    java.nio.file.Files.copy(logFile.toPath(), customLog.toPath(),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING);

    // Open with explicit custom file names — should work even though names don't end in .spi/.spl
    try (SparkeyReader reader = Sparkey.reader().indexFile(customIndex).logFile(customLog).open()) {
      assertEquals("value", reader.getAsString("key"));
    }
    customIndex.delete();
    customLog.delete();
  }

  /**
   * Assert reader type based on runtime version and configuration.
   * On Java 22+, uncompressed mmap uses UncompressedSparkeyReaderJ22 (even for pooled, since it's inherently thread-safe).
   * On Java 22+, compressed single-threaded uses SingleThreadedSparkeyReaderJ22.
   * On Java 8-21, always uses SingleThreadedSparkeyReader (pooled wraps it in PooledSparkeyReader).
   */
  private void assertReaderType(SparkeyReader reader, String scenario, boolean singleThreaded) {
    String className = reader.getClass().getSimpleName();
    int javaVersion = Runtime.version().feature();

    if (javaVersion >= 22) {
      switch (scenario) {
        case "uncompressed":
        case "uncompressed-pooled":
          // J22 uncompressed reader is inherently thread-safe, no pool needed
          assertEquals("UncompressedSparkeyReaderJ22", className);
          break;
        case "compressed":
          if (singleThreaded) {
            assertEquals("SingleThreadedSparkeyReaderJ22", className);
          } else {
            assertTrue("Expected PooledSparkeyReader, got " + className,
                reader instanceof PooledSparkeyReader);
          }
          break;
        default:
          throw new IllegalArgumentException("Unknown scenario: " + scenario);
      }
    } else {
      if (singleThreaded) {
        assertEquals("SingleThreadedSparkeyReader", className);
      } else {
        assertTrue("Expected PooledSparkeyReader, got " + className,
            reader instanceof PooledSparkeyReader);
      }
    }
  }
}
