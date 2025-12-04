/*
 * Copyright (c) 2025 Spotify AB
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

import com.spotify.sparkey.CompressionType;
import com.spotify.sparkey.HashType;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyReader;
import com.spotify.sparkey.SparkeyWriter;
import com.spotify.sparkey.TestSparkeyWriter;
import com.spotify.sparkey.UtilTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Parametrized large file tests that run against all available reader implementations.
 * Tests verify that all reader types correctly handle large files (>2GB chunks, large indices).
 */
@RunWith(Parameterized.class)
public class ReaderParametrizedLargeFilesTest extends BaseSystemTest {

  private final ReaderType readerType;

  public ReaderParametrizedLargeFilesTest(ReaderType readerType) {
    this.readerType = readerType;
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> readerTypes() {
    return java.util.Arrays.asList(ReaderType.availableAsParameters());
  }

  @Test
  public void testLargeLogFile() throws IOException {
    if (!readerType.supports(CompressionType.NONE)) {
      return;
    }

    UtilTest.setMapBits(10);
    String expectedValue = "value";
    while (expectedValue.length() < 5 * 1024) { // Larger than a map chunk
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

    try (SparkeyReader reader = readerType.open(indexFile)) {
      assertEquals(indexFile.length() + logFile.length(), reader.getTotalBytes());
      for (int i = 0; i < 2000; i += 100) {
        assertEquals(expectedValue, reader.getAsString("key_" + i));
      }
      assertNull(reader.getAsString("key_" + 2000));
    }
  }

  @Test
  public void testSmallIndexFile() throws IOException {
    if (!readerType.supports(CompressionType.NONE)) {
      return;
    }
    testLargeIndexFileInner(7000);
  }

  @Test
  public void testMediumIndexFile() throws IOException {
    if (!readerType.supports(CompressionType.NONE)) {
      return;
    }
    testLargeIndexFileInner(150000);
  }

  @Test
  public void testLargeIndexFile() throws IOException {
    if (!readerType.supports(CompressionType.NONE)) {
      return;
    }
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

    try (SparkeyReader reader = readerType.open(indexFile)) {
      assertTrue(0 <= reader.getLoadedBytes());
      assertTrue(reader.getLoadedBytes() <= reader.getTotalBytes());
      for (int i = 0; i < 1000; i++) {
        long key = i * size / 1000L;
        assertEquals("" + (key % 13), reader.getAsString("key_" + key));
      }
      assertNull(reader.getAsString("key_" + size));
    }
  }
}
