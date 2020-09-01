/*
 * Copyright (c) 2011-2013 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.spotify.sparkey.system;

import com.spotify.sparkey.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class LargeFilesTest extends BaseSystemTest {
  @Test
  public void testLargeLogFile() throws IOException {
    UtilTest.setMapBits(10);
    String expectedValue = "value";
    while (expectedValue.length() < 5*1024) { // Larger than a map chunk
      expectedValue += expectedValue;
    }

    byte[] value = expectedValue.getBytes();

    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024);
    for (int i = 0; i < 2000; i++) {
      writer.put(("key_" + i).getBytes(), value);
    }
    TestSparkeyWriter.writeHashAndCompare(writer);
    writer.close();

    assertTrue(logFile.length() > 2000 * 5 * 1024);
    SparkeyReader reader = Sparkey.open(indexFile);
    assertEquals(indexFile.length() + logFile.length(), reader.getTotalBytes());
    assertTrue(reader.getLoadedBytes() <= reader.getTotalBytes());
    for (int i = 0; i < 2000; i += 100) {
      assertEquals(expectedValue, reader.getAsString("key_" + i));
    }
    assertEquals(null, reader.getAsString("key_" + 2000));
    assertEquals(reader.getTotalBytes(), reader.getLoadedBytes());
    reader.close();
  }

  @Test
  public void testSmallIndexFile() throws IOException {
    testLargeIndexFileInner(7000);
  }

  @Test
  public void testMediumIndexFile() throws IOException {
    testLargeIndexFileInner(150000);
  }

  @Test
  public void testLargeIndexFile() throws IOException {
    testLargeIndexFileInner(500000);
  }

  private void testLargeIndexFileInner(final long size) throws IOException {
    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 1024);
    for (int i = 0; i < size; i++) {
      writer.put(("key_" + i), "" + (i % 13));
    }
    writer.setHashType(HashType.HASH_64_BITS);
    TestSparkeyWriter.writeHashAndCompare(writer);
    writer.close();

    assertTrue(indexFile.length() > size * 8L);
    SparkeyReader reader = Sparkey.open(indexFile);
    assertTrue(reader.getLoadedBytes() <= reader.getTotalBytes());
    for (int i = 0; i < 1000; i++) {
      long key = i * size / 1000L;
      assertEquals("" + (key % 13), reader.getAsString("key_" + key));
    }
    assertEquals(null, reader.getAsString("key_" + size));
    assertEquals(reader.getTotalBytes(), reader.getLoadedBytes());
    reader.close();
  }
}
