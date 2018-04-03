package com.spotify.sparkey;

import com.google.common.io.Files;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IndexHashTest {
  private File dir;

  @Before
  public void setUp() throws Exception {
    dir = Files.createTempDir();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(dir);
  }

  @Test
  public void testCorruptHashFile() throws Exception {
    File indexFile = new File(dir, "test.spi");

    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1);
    for (int i = 0; i < 100; i++) {
      writer.put("key" + i, "value" + i);
    }
    writer.close();
    TestSparkeyWriter.writeHashAndCompare(writer);

    corruptFile(indexFile);

    long before = getFileHandleCount();
    try {
      Sparkey.open(indexFile);
      fail();
    } catch (Exception e) {
      assertEquals(RuntimeException.class, e.getClass());
    }
    long after = getFileHandleCount();

    assertEquals(before, after);

  }

  private void corruptFile(File indexFile) throws IOException {
    RandomAccessFile randomAccessFile = new RandomAccessFile(indexFile, "rw");
    randomAccessFile.setLength(randomAccessFile.length() - 100);
    randomAccessFile.close();
  }

  private static long getFileHandleCount() {
    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    if(os instanceof UnixOperatingSystemMXBean){
      long openFileDescriptorCount = ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
      return openFileDescriptorCount;
    }
    throw new UnsupportedOperationException();
  }
}
