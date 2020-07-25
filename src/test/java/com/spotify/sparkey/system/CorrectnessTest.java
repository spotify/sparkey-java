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

import com.spotify.sparkey.CompressionType;
import com.spotify.sparkey.HashType;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyLogIterator;
import com.spotify.sparkey.SparkeyReader;
import com.spotify.sparkey.SparkeyWriter;

import com.spotify.sparkey.TestSparkeyWriter;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CorrectnessTest extends BaseSystemTest {

  private static final int[] SIZES = new int[]{0, 1, 2, 3, 4, 10, 100, 1000};

  @Test
  public void testTrivial() throws IOException {
       testHelper(4, CompressionType.NONE, 0, HashType.HASH_32_BITS);
  }

  @Test
  public void testTrivialDelete() throws IOException {
       testHelperWithDeletes(1, CompressionType.NONE, 0, HashType.HASH_32_BITS);
  }

  @Test
  public void testSimple() throws IOException {
    for (HashType hashType : HashType.values()) {
      for (int size : SIZES) {
        testHelper(size, CompressionType.NONE, 0, hashType);
        testHelper(size, CompressionType.SNAPPY, 64, hashType);
        testHelper(size, CompressionType.SNAPPY, 1024, hashType);
        testHelper(size, CompressionType.SNAPPY, 4096, hashType);
        testHelper(size, CompressionType.ZSTD, 64, hashType);
        testHelper(size, CompressionType.ZSTD, 1024, hashType);
        testHelper(size, CompressionType.ZSTD, 4096, hashType);
      }
    }
  }

  @Test
  public void testPutWithInputStream() throws IOException {
    String expectedValue = "value";
    while (expectedValue.length() < 1000) {
      expectedValue += expectedValue;
    }

    for (CompressionType compressionType : CompressionType.values()) {
      SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, 40);
      byte[] key = "key".getBytes();
      byte[] value = expectedValue.getBytes();
      writer.put(key, new ByteArrayInputStream(value), value.length);
      TestSparkeyWriter.writeHashAndCompare(writer);
      writer.close();

      try (SparkeyReader reader = Sparkey.open(indexFile)) {
        assertEquals(expectedValue, reader.getAsString("key"));
      }
    }
  }

  private void testHelper(int N, CompressionType compressionType, int compressionBlockSize, HashType hashType) throws IOException {
    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, compressionBlockSize);
    for (int i = 0; i < N; i++) {
      writer.put("Key" + i, "Value" + i);
    }
    writer.setHashSeed(1738868818);
    writer.writeHash(hashType);
    writer.close();

    SparkeyReader reader = Sparkey.open(indexFile);
    for (int i = 0; i < N; i++) {
      assertEquals("Value" + i, reader.getAsString("Key" + i));
    }
    int i = 0;
    for (SparkeyReader.Entry entry : reader) {
      assertEquals("Key" + i, entry.getKeyAsString());
      assertEquals("Value" + i, entry.getValueAsString());
      i++;
    }
    assertEquals(N, i);
    reader.close();
  }

  private void testHelperWithDeletes(int N, CompressionType compressionType, int compressionBlockSize, HashType hashType) throws IOException {
    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, compressionBlockSize);

    writer.setFsync(true);
    writer.setHashSeed(-112683590);
    writer.setHashType(hashType);

    for (int i = 0; i < N; i++) {
      writer.put("Key" + i, "Value" + i);
    }
    for (int i = 0; i < N; i++) {
      if ((i % 7) == 0) {
        writer.delete("Key" + i);
      }
    }
    writer.flush();
    {
      SparkeyLogIterator logIterator = new SparkeyLogIterator(logFile);
      int i = 0;
      for (SparkeyReader.Entry entry : logIterator) {
        if (i < N) {
          assertEquals(SparkeyReader.Type.PUT, entry.getType());
          assertEquals("Key" + i, entry.getKeyAsString());
        } else {
          assertEquals(SparkeyReader.Type.DELETE, entry.getType());
          int keyIndex = 7 * (i - N);
          assertEquals("Key" + keyIndex, entry.getKeyAsString());
        }
        i++;
      }
    }

    TestSparkeyWriter.writeHashAndCompare(writer);
    writer.close();
    assertEquals(0, Sparkey.getOpenFiles());

    SparkeyReader reader = Sparkey.open(indexFile);
    for (int i = 0; i < N; i++) {
      if ((i % 7) == 0) {
        assertEquals(null, reader.getAsString("Key" + i));
      } else {
        assertEquals("Value" + i, reader.getAsString("Key" + i));
      }
    }
    for (SparkeyReader.Entry entry : reader) {
      String key = entry.getKeyAsString();
      int i = Integer.parseInt(key.substring(3));
      if ((i % 7) == 0) {
        fail();
      }
      assertEquals("Value" + i, entry.getValueAsString());
    }
    reader.close();
  }

  @Test
  public void testAppendedPut() throws IOException {
    for (int i = 0; i <2; i++) {
      SparkeyWriter writer = Sparkey.appendOrCreate(indexFile, CompressionType.NONE, 40);
      writer.put("Key" + i, "Value" + i);
      writer.flush();
      TestSparkeyWriter.writeHashAndCompare(writer);
      writer.close();
    }
    SparkeyReader reader = Sparkey.open(indexFile);
    assertEquals("Value1", reader.getAsString("Key1"));
    reader.close();
  }

  @Test
  public void testWithDeletes() throws IOException {
    for (HashType hashType : HashType.values()) {
      for (int size : SIZES) {
        testHelperWithDeletes(size, CompressionType.NONE, 0, hashType);
        testHelperWithDeletes(size, CompressionType.SNAPPY, 64, hashType);
        testHelperWithDeletes(size, CompressionType.SNAPPY, 1024, hashType);
        testHelperWithDeletes(size, CompressionType.SNAPPY, 4096, hashType);
        testHelperWithDeletes(size, CompressionType.ZSTD, 64, hashType);
        testHelperWithDeletes(size, CompressionType.ZSTD, 1024, hashType);
        testHelperWithDeletes(size, CompressionType.ZSTD, 4096, hashType);
      }
    }
  }

  @Test
  public void testFinishIteratorClosesFiles() throws IOException {
    long before = countOpenFileDescriptors();

    // Will only run test on *nix systems
    Assume.assumeTrue(before >= 0);

    SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 0);
    writer.setFsync(true);
    writer.setHashSeed(-112683590);
    writer.setHashType(HashType.HASH_32_BITS);

    for (int i = 0; i < 10; i++) {
      writer.put("Key" + i, "Value" + i);
    }
    writer.flush();
    writer.close();

    assertEquals(before, countOpenFileDescriptors());

    {
      SparkeyLogIterator logIterator = new SparkeyLogIterator(logFile);
      Iterator<SparkeyReader.Entry> iterator = logIterator.iterator();
      while (iterator.hasNext()) {
        iterator.next();
      }
    }

    assertEquals(before, countOpenFileDescriptors());
  }

  @Test
  public void testOverwrite() throws IOException {
    for (int i = 0; i < 10; i++) {
      SparkeyWriter writer = Sparkey.createNew(indexFile);
      writer.put("A", "B");
      writer.flush();
      TestSparkeyWriter.writeHashAndCompare(writer);
      writer.close();
    }
  }
}
