package com.spotify.sparkey;

import com.spotify.sparkey.system.BaseSystemTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IndexHashTest extends BaseSystemTest {
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testCorruptHashFile() throws Exception {
    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1);
    for (int i = 0; i < 100; i++) {
      writer.put("key" + i, "value" + i);
    }
    writer.close();
    TestSparkeyWriter.writeHashAndCompare(writer);

    corruptFile(indexFile);

    assertEquals(0, Sparkey.getOpenFiles());
    assertEquals(0, Sparkey.getOpenMaps());

    try {
      Sparkey.open(indexFile);
      fail();
    } catch (Exception e) {
      assertEquals(RuntimeException.class, e.getClass());
    }

    assertEquals(0, Sparkey.getOpenFiles());
    assertEquals(0, Sparkey.getOpenMaps());
  }

  private void corruptFile(File indexFile) throws IOException {
    RandomAccessFile randomAccessFile = new RandomAccessFile(indexFile, "rw");
    randomAccessFile.setLength(randomAccessFile.length() - 100);
    randomAccessFile.close();
  }
}
