package com.spotify.sparkey;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BytesWrittenTest extends OpenMapsAsserter {

  private File file;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    file = File.createTempFile("sparkey_test_", ".spl");
  }

  @After
  public void tearDown() throws Exception {
    file.delete();
    super.tearDown();
  }

  @Test
  public void testNone() throws Exception {
    test(CompressionType.NONE);
  }

  @Test
  public void testSnappy() throws Exception {
    test(CompressionType.SNAPPY);
  }

  @Test
  public void testZstd() throws Exception {
    test(CompressionType.ZSTD);
  }

  private void test(CompressionType compressionType) throws IOException {
    SparkeyWriter writer = Sparkey.createNew(file, compressionType, 20);
    for (int i = 0; i < 13; i++) {
      writer.put(size(17), size(47));
    }
    for (int i = 0; i < 19; i++) {
      writer.put(size(130), size(32000));
    }
    for (int i = 0; i < 3; i++) {
      writer.delete(size(130));
    }
    writer.close();
    assertEquals(13 * (17 + 47 + 1 + 1) + 19 * (130 + 32000 + 2 + 3), LogHeader.read(file).getPutSize());
    assertEquals(3 * (130 + 2 + 1), LogHeader.read(file).getDeleteSize());
  }

  private String size(int size) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < size; i++) {
      stringBuilder.append("x");
    }
    return stringBuilder.toString();
  }
}
