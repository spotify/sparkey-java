package com.spotify.sparkey;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Tests for SparkeyReader.load() with all LoadMode combinations,
 * including mlock modes on Java 22+.
 */
public class LoadModeTest {
  private File indexFile;
  private File logFile;

  @Before
  public void setUp() throws IOException {
    indexFile = File.createTempFile("sparkey-load-test", ".spi");
    logFile = Sparkey.getLogFile(indexFile);
    indexFile.deleteOnExit();
    logFile.deleteOnExit();

    // Write small test data
    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 512);
    for (int i = 0; i < 100; i++) {
      writer.put("key" + i, "value" + i);
    }
    writer.writeHash();
    writer.close();
  }

  @After
  public void tearDown() {
    indexFile.delete();
    logFile.delete();
  }

  @Test
  public void testAllLoadModesWithDefaultReader() throws Exception {
    for (LoadMode mode : LoadMode.values()) {
      try (SparkeyReader reader = Sparkey.open(indexFile)) {
        LoadResult result = reader.load(mode);
        result.await();
        assertTrue("Should be done after await for " + mode, result.isDone());

        if (mode == LoadMode.NONE) {
          assertEquals(0, result.requestedBytes());
        } else {
          assertTrue("Should have requested bytes for " + mode, result.requestedBytes() > 0);
        }

        // Verify reader still works after load
        assertEquals("value0", reader.getAsString("key0"));
        assertEquals("value99", reader.getAsString("key99"));
        assertNull(reader.getAsString("nonexistent"));
      }
    }
  }

  @Test
  public void testAllLoadModesWithSingleThreadedReader() throws Exception {
    for (LoadMode mode : LoadMode.values()) {
      try (SparkeyReader reader = Sparkey.openSingleThreadedReader(indexFile)) {
        LoadResult result = reader.load(mode);
        result.await();
        assertTrue("Should be done after await for " + mode, result.isDone());
        assertEquals("value50", reader.getAsString("key50"));
      }
    }
  }

  @Test
  public void testAllLoadModesWithUncompressedJ22() throws Exception {
    try (SparkeyReader reader = SparkeyTestHelper.openUncompressedJ22(indexFile)) {
      for (LoadMode mode : LoadMode.values()) {
        LoadResult result = reader.load(mode);
        result.await();
        assertTrue("Should be done after await for " + mode, result.isDone());
        assertEquals("value42", reader.getAsString("key42"));
      }
    }
  }

  @Test
  public void testAllLoadModesWithSingleThreadedJ22() throws Exception {
    try (SparkeyReader reader = SparkeyTestHelper.openSingleThreadedJ22(indexFile)) {
      for (LoadMode mode : LoadMode.values()) {
        LoadResult result = reader.load(mode);
        result.await();
        assertTrue("Should be done after await for " + mode, result.isDone());
        assertEquals("value42", reader.getAsString("key42"));
      }
    }
  }

  @Test
  public void testMlockModesReturnLockedFlag() throws Exception {
    try (SparkeyReader reader = SparkeyTestHelper.openUncompressedJ22(indexFile)) {
      // Advisory load should not report locked
      LoadResult loadResult = reader.load(LoadMode.ALL);
      loadResult.await();
      assertFalse("Advisory load should not be locked", loadResult.locked());

      // Mlock should attempt to lock — result depends on platform/privileges
      LoadResult mlockResult = reader.load(LoadMode.ALL_MLOCK);
      mlockResult.await();
      assertTrue(mlockResult.isDone());
      assertTrue("Mlock result should have requested bytes > 0", mlockResult.requestedBytes() > 0);
      // Verify reader still works regardless of locked outcome
      assertEquals("value0", reader.getAsString("key0"));
    }
  }

  @Test
  public void testMlockIndexOnly() throws Exception {
    try (SparkeyReader reader = SparkeyTestHelper.openUncompressedJ22(indexFile)) {
      LoadResult result = reader.load(LoadMode.INDEX_MLOCK);
      result.await();
      assertTrue(result.isDone());
      // Should only have requested index bytes, not log bytes
      long indexSize = indexFile.length();
      assertEquals(indexSize, result.requestedBytes());
      assertEquals("value0", reader.getAsString("key0"));
    }
  }

  @Test
  public void testMlockLogOnly() throws Exception {
    try (SparkeyReader reader = SparkeyTestHelper.openUncompressedJ22(indexFile)) {
      LoadResult result = reader.load(LoadMode.LOG_MLOCK);
      result.await();
      assertTrue(result.isDone());
      long logSize = logFile.length();
      assertEquals(logSize, result.requestedBytes());
      assertEquals("value0", reader.getAsString("key0"));
    }
  }

  @Test
  public void testLoadModeEnumProperties() {
    // NONE
    assertEquals(LoadMode.Action.NONE, LoadMode.NONE.indexAction());
    assertEquals(LoadMode.Action.NONE, LoadMode.NONE.logAction());

    // INDEX
    assertEquals(LoadMode.Action.LOAD, LoadMode.INDEX.indexAction());
    assertEquals(LoadMode.Action.NONE, LoadMode.INDEX.logAction());

    // LOG
    assertEquals(LoadMode.Action.NONE, LoadMode.LOG.indexAction());
    assertEquals(LoadMode.Action.LOAD, LoadMode.LOG.logAction());

    // ALL
    assertEquals(LoadMode.Action.LOAD, LoadMode.ALL.indexAction());
    assertEquals(LoadMode.Action.LOAD, LoadMode.ALL.logAction());

    // INDEX_MLOCK
    assertEquals(LoadMode.Action.MLOCK, LoadMode.INDEX_MLOCK.indexAction());
    assertEquals(LoadMode.Action.NONE, LoadMode.INDEX_MLOCK.logAction());

    // LOG_MLOCK
    assertEquals(LoadMode.Action.NONE, LoadMode.LOG_MLOCK.indexAction());
    assertEquals(LoadMode.Action.MLOCK, LoadMode.LOG_MLOCK.logAction());

    // ALL_MLOCK
    assertEquals(LoadMode.Action.MLOCK, LoadMode.ALL_MLOCK.indexAction());
    assertEquals(LoadMode.Action.MLOCK, LoadMode.ALL_MLOCK.logAction());
  }

  @Test
  public void testCombinePreservesLocked() throws Exception {
    try (SparkeyReader reader = SparkeyTestHelper.openUncompressedJ22(indexFile)) {
      LoadResult r1 = reader.load(LoadMode.ALL_MLOCK);
      LoadResult r2 = reader.load(LoadMode.ALL_MLOCK);
      LoadResult combined = LoadResult.combine(r1, r2);
      combined.await();
      assertTrue(combined.isDone());
      assertEquals(r1.requestedBytes() + r2.requestedBytes(), combined.requestedBytes());
      // locked should be consistent: both locked or both not
      assertEquals(r1.locked() && r2.locked(), combined.locked());
    }
  }

  @Test
  public void testCombineWithCompletedResult() throws Exception {
    LoadResult completed = LoadResult.completed();
    assertTrue(completed.isDone());
    assertEquals(0, completed.requestedBytes());
    assertFalse(completed.locked());

    try (SparkeyReader reader = SparkeyTestHelper.openUncompressedJ22(indexFile)) {
      LoadResult real = reader.load(LoadMode.ALL);
      LoadResult combined = LoadResult.combine(completed, real);
      combined.await();
      assertEquals(real.requestedBytes(), combined.requestedBytes());
    }
  }

  @Test
  public void testCreateFactory() throws Exception {
    LoadResult unlocked = LoadResult.create(42,
        java.util.concurrent.CompletableFuture.completedFuture(false));
    assertTrue(unlocked.isDone());
    assertEquals(42, unlocked.requestedBytes());
    assertFalse(unlocked.locked());

    LoadResult locked = LoadResult.create(100,
        java.util.concurrent.CompletableFuture.completedFuture(true));
    assertTrue(locked.isDone());
    assertEquals(100, locked.requestedBytes());
    assertTrue(locked.locked());
  }

  @Test
  public void testCompressedWithMlock() throws Exception {
    // Create compressed sparkey
    File compIndexFile = File.createTempFile("sparkey-load-comp-test", ".spi");
    File compLogFile = Sparkey.getLogFile(compIndexFile);
    compIndexFile.deleteOnExit();
    compLogFile.deleteOnExit();

    SparkeyWriter writer = Sparkey.createNew(compIndexFile, CompressionType.SNAPPY, 512);
    for (int i = 0; i < 100; i++) {
      writer.put("key" + i, "value" + i);
    }
    writer.writeHash();
    writer.close();

    try {
      // Test with default reader (handles compressed files)
      for (LoadMode mode : LoadMode.values()) {
        try (SparkeyReader reader = Sparkey.open(compIndexFile)) {
          LoadResult result = reader.load(mode);
          result.await();
          assertTrue("Should be done for " + mode, result.isDone());
          assertEquals("value0", reader.getAsString("key0"));
        }
      }
    } finally {
      compIndexFile.delete();
      compLogFile.delete();
    }
  }
}
