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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Parametrized correctness tests that run against all available reader implementations.
 * Tests verify that all reader types (SingleThreaded, Pooled, ImmutableJ22, etc.) produce
 * correct results for various compression types, hash types, and data sizes.
 */
@RunWith(Parameterized.class)
public class ReaderParametrizedCorrectnessTest extends BaseSystemTest {

  private final ReaderType readerType;

  public ReaderParametrizedCorrectnessTest(ReaderType readerType) {
    this.readerType = readerType;
  }

  @Parameters(name = "{0}")
  public static Collection<Object[]> readerTypes() {
    return java.util.Arrays.asList(ReaderType.availableAsParameters());
  }

  @Test
  public void testBasicReadWrite_Uncompressed() throws IOException {
    testBasicReadWrite(CompressionType.NONE, 0);
  }

  @Test
  public void testBasicReadWrite_Snappy() throws IOException {
    if (!readerType.supports(CompressionType.SNAPPY)) {
      return; // Skip for readers that don't support compression
    }
    testBasicReadWrite(CompressionType.SNAPPY, 1024);
  }

  @Test
  public void testBasicReadWrite_Zstd() throws IOException {
    if (!readerType.supports(CompressionType.ZSTD)) {
      return; // Skip for readers that don't support compression
    }
    testBasicReadWrite(CompressionType.ZSTD, 1024);
  }

  private void testBasicReadWrite(CompressionType compressionType, int blockSize) throws IOException {
    int N = 100;

    // Write data
    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, blockSize);
    for (int i = 0; i < N; i++) {
      writer.put("Key" + i, "Value" + i);
    }
    writer.setHashSeed(1738868818);
    writer.writeHash(HashType.HASH_64_BITS);
    writer.close();

    // Read and verify using the parameterized reader type
    try (SparkeyReader reader = readerType.open(indexFile)) {
      for (int i = 0; i < N; i++) {
        assertEquals("Value" + i, reader.getAsString("Key" + i));
      }

      // Verify iteration
      int count = 0;
      for (SparkeyReader.Entry entry : reader) {
        String key = entry.getKeyAsString();
        int i = Integer.parseInt(key.substring(3));
        assertEquals("Value" + i, entry.getValueAsString());
        count++;
      }
      assertEquals(N, count);
    }
  }

  @Test
  public void testWithDeletes_Uncompressed() throws IOException {
    testWithDeletes(CompressionType.NONE, 0);
  }

  @Test
  public void testWithDeletes_Snappy() throws IOException {
    if (!readerType.supports(CompressionType.SNAPPY)) {
      return;
    }
    testWithDeletes(CompressionType.SNAPPY, 1024);
  }

  @Test
  public void testWithDeletes_Zstd() throws IOException {
    if (!readerType.supports(CompressionType.ZSTD)) {
      return;
    }
    testWithDeletes(CompressionType.ZSTD, 1024);
  }

  private void testWithDeletes(CompressionType compressionType, int blockSize) throws IOException {
    int N = 100;

    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, blockSize);
    writer.setHashSeed(-112683590);
    writer.setHashType(HashType.HASH_64_BITS);

    // Write entries
    for (int i = 0; i < N; i++) {
      writer.put("Key" + i, "Value" + i);
    }

    // Delete every 7th entry
    for (int i = 0; i < N; i++) {
      if ((i % 7) == 0) {
        writer.delete("Key" + i);
      }
    }

    TestSparkeyWriter.writeHashAndCompare(writer);
    writer.close();

    // Verify with parameterized reader type
    try (SparkeyReader reader = readerType.open(indexFile)) {
      for (int i = 0; i < N; i++) {
        if ((i % 7) == 0) {
          assertNull(reader.getAsString("Key" + i));
        } else {
          assertEquals("Value" + i, reader.getAsString("Key" + i));
        }
      }

      // Verify iteration doesn't include deleted entries
      for (SparkeyReader.Entry entry : reader) {
        String key = entry.getKeyAsString();
        int i = Integer.parseInt(key.substring(3));
        if ((i % 7) == 0) {
          fail("Deleted entry should not appear in iteration: " + key);
        }
        assertEquals("Value" + i, entry.getValueAsString());
      }
    }
  }

  @Test
  public void testEmptyFile_Uncompressed() throws IOException {
    testEmptyFile(CompressionType.NONE, 0);
  }

  @Test
  public void testEmptyFile_Snappy() throws IOException {
    if (!readerType.supports(CompressionType.SNAPPY)) {
      return;
    }
    testEmptyFile(CompressionType.SNAPPY, 1024);
  }

  private void testEmptyFile(CompressionType compressionType, int blockSize) throws IOException {
    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, blockSize);
    writer.writeHash();
    writer.close();

    try (SparkeyReader reader = readerType.open(indexFile)) {
      assertNull(reader.getAsString("nonexistent"));

      int count = 0;
      for (SparkeyReader.Entry entry : reader) {
        count++;
      }
      assertEquals(0, count);
    }
  }

  @Test
  public void testSingleEntry_Uncompressed() throws IOException {
    testSingleEntry(CompressionType.NONE, 0);
  }

  @Test
  public void testSingleEntry_Snappy() throws IOException {
    if (!readerType.supports(CompressionType.SNAPPY)) {
      return;
    }
    testSingleEntry(CompressionType.SNAPPY, 1024);
  }

  private void testSingleEntry(CompressionType compressionType, int blockSize) throws IOException {
    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, blockSize);
    writer.put("SingleKey", "SingleValue");
    writer.writeHash();
    writer.close();

    try (SparkeyReader reader = readerType.open(indexFile)) {
      assertEquals("SingleValue", reader.getAsString("SingleKey"));
      assertNull(reader.getAsString("NonExistent"));

      int count = 0;
      for (SparkeyReader.Entry entry : reader) {
        assertEquals("SingleKey", entry.getKeyAsString());
        assertEquals("SingleValue", entry.getValueAsString());
        count++;
      }
      assertEquals(1, count);
    }
  }

  @Test
  public void testDifferentHashTypes_Uncompressed() throws IOException {
    testDifferentHashTypes(CompressionType.NONE, 0);
  }

  @Test
  public void testDifferentHashTypes_Snappy() throws IOException {
    if (!readerType.supports(CompressionType.SNAPPY)) {
      return;
    }
    testDifferentHashTypes(CompressionType.SNAPPY, 1024);
  }

  private void testDifferentHashTypes(CompressionType compressionType, int blockSize) throws IOException {
    for (HashType hashType : HashType.values()) {
      int N = 50;

      SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, blockSize);
      for (int i = 0; i < N; i++) {
        writer.put("Key" + i, "Value" + i);
      }
      writer.setHashSeed(12345);
      writer.writeHash(hashType);
      writer.close();

      try (SparkeyReader reader = readerType.open(indexFile)) {
        for (int i = 0; i < N; i++) {
          assertEquals("Value" + i, reader.getAsString("Key" + i));
        }
      }
    }
  }

  @Test
  public void testLargeValues_Uncompressed() throws IOException {
    testLargeValues(CompressionType.NONE, 0);
  }

  @Test
  public void testLargeValues_Snappy() throws IOException {
    if (!readerType.supports(CompressionType.SNAPPY)) {
      return;
    }
    testLargeValues(CompressionType.SNAPPY, 4096);
  }

  private void testLargeValues(CompressionType compressionType, int blockSize) throws IOException {
    // Create a large value (100KB)
    StringBuilder largeValue = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      largeValue.append("0123456789");
    }
    String expectedValue = largeValue.toString();

    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, blockSize);
    writer.put("largeKey", expectedValue);
    writer.put("smallKey", "smallValue");
    writer.writeHash();
    writer.close();

    try (SparkeyReader reader = readerType.open(indexFile)) {
      assertEquals(expectedValue, reader.getAsString("largeKey"));
      assertEquals("smallValue", reader.getAsString("smallKey"));
    }
  }

  @Test
  public void testPutWithInputStream_Uncompressed() throws IOException {
    testPutWithInputStream(CompressionType.NONE, 40);
  }

  @Test
  public void testPutWithInputStream_Snappy() throws IOException {
    if (!readerType.supports(CompressionType.SNAPPY)) {
      return;
    }
    testPutWithInputStream(CompressionType.SNAPPY, 40);
  }

  @Test
  public void testPutWithInputStream_Zstd() throws IOException {
    if (!readerType.supports(CompressionType.ZSTD)) {
      return;
    }
    testPutWithInputStream(CompressionType.ZSTD, 40);
  }

  private void testPutWithInputStream(CompressionType compressionType, int blockSize) throws IOException {
    String expectedValue = "value";
    while (expectedValue.length() < 1000) {
      expectedValue += expectedValue;
    }

    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, blockSize);
    byte[] key = "key".getBytes();
    byte[] value = expectedValue.getBytes();
    writer.put(key, new java.io.ByteArrayInputStream(value), value.length);
    TestSparkeyWriter.writeHashAndCompare(writer);
    writer.close();

    try (SparkeyReader reader = readerType.open(indexFile)) {
      assertEquals(expectedValue, reader.getAsString("key"));
    }
  }

  @Test
  public void testHashCollisions() throws IOException {
    if (!readerType.supports(CompressionType.NONE)) {
      return;
    }

    int N = 170000;
    try (SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.NONE, 0)) {
      writer.setHashSeed(1234);
      writer.setHashType(HashType.HASH_32_BITS);
      for (int i = 0; i < N; i++) {
        writer.put("Key" + i, "Value" + i);
      }
      writer.flush();
      TestSparkeyWriter.writeHashAndCompare(writer);
    }

    try (SparkeyReader reader = readerType.open(indexFile)) {
      assertEquals(0, reader.getIndexHeader().getGarbageSize());
      long hashCollisions = reader.getIndexHeader().getHashCollisions();
      assertTrue(hashCollisions > 0);

      // Spot check some values
      for (int i = 0; i < N; i += 1000) {
        assertEquals("Value" + i, reader.getAsString("Key" + i));
      }
    }
  }

  @Test
  public void testLargeKeys_Uncompressed() throws IOException {
    testLargeKeys(CompressionType.NONE, 0);
  }

  @Test
  public void testLargeKeys_Snappy() throws IOException {
    if (!readerType.supports(CompressionType.SNAPPY)) {
      return;
    }
    testLargeKeys(CompressionType.SNAPPY, 1024);
  }

  /**
   * Test with large keys (>64 bytes) to ensure vectorized MemorySegment comparison path
   * is exercised in Java 22+ readers. The threshold is 64 bytes - keys larger than this
   * use vectorized mismatch(), while smaller keys use byte-by-byte comparison.
   */
  private void testLargeKeys(CompressionType compressionType, int blockSize) throws IOException {
    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, blockSize);

    // Create keys of various sizes to test both code paths
    String smallKey = "small";  // 5 bytes - uses byte-by-byte
    String mediumKey = "medium_key_that_is_exactly_thirty_bytes"; // ~30 bytes - uses byte-by-byte
    String largeKey = "large_key_".repeat(10);  // 110 bytes - uses vectorized mismatch()
    String veryLargeKey = "x".repeat(200);  // 200 bytes - uses vectorized mismatch()

    writer.put(smallKey, "value1");
    writer.put(mediumKey, "value2");
    writer.put(largeKey, "value3");
    writer.put(veryLargeKey, "value4");
    writer.writeHash();
    writer.close();

    try (SparkeyReader reader = readerType.open(indexFile)) {
      // Verify all key sizes work correctly
      assertEquals("value1", reader.getAsString(smallKey));
      assertEquals("value2", reader.getAsString(mediumKey));
      assertEquals("value3", reader.getAsString(largeKey));
      assertEquals("value4", reader.getAsString(veryLargeKey));

      // Verify non-existent large key returns null (tests comparison path for mismatches)
      assertNull(reader.getAsString("large_key_".repeat(10) + "different"));
      assertNull(reader.getAsString("y".repeat(200)));

      // Verify iteration includes all entries
      int count = 0;
      for (SparkeyReader.Entry entry : reader) {
        count++;
      }
      assertEquals(4, count);
    }
  }
}
