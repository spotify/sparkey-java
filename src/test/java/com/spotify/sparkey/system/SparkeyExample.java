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

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class SparkeyExample {
  private static final int NUM_RANDOM_READS = 1000;
  private static final int N = 2000;

  public static void main(String[] args) throws Exception {
    final File indexFile = new File("test.spi");

    create(indexFile);

    final Random random = new Random(11234);
    final SparkeyReader reader = Sparkey.open(indexFile);

    randomReads(random, reader);

    rawIteration(new SparkeyLogIterator(Sparkey.getLogFile(indexFile)));

    iteration(reader);

    reader.close();
  }

  private static void randomReads(Random random, SparkeyReader reader) throws IOException {
    for (int i = 0; i < NUM_RANDOM_READS; i++) {
      int k = random.nextInt(N);
      String key = "Key" + k;
      String entry = reader.getAsString(key);
      if (!("Value" + k).equals(entry)) {
        throw new RuntimeException("Expected " + "Value" + k + " but got " + entry);
      }
    }
  }

  private static void create(File indexFile) throws IOException {
    final SparkeyWriter writer = Sparkey.createNew(indexFile, CompressionType.SNAPPY, 512);
    for (int i = 0; i < N; i++) {
      writer.put("Key" + i, "Value" + i);
    }
    writer.flush();
    writer.writeHash();
    writer.close();
  }

  private static void iteration(final SparkeyReader reader) throws IOException {
    int i = 0;
    for (SparkeyReader.Entry entry : reader) {
      String key = entry.getKeyAsString();
      String value = entry.getValueAsString();

      String expectedKey = "Key" + i;
      String expectedValue = "Value" + i;

      if (!key.equals(expectedKey)) {
        throw new RuntimeException("Expected " + expectedKey + " but got " + key);
      }
      if (!value.equals(expectedValue)) {
        throw new RuntimeException("Expected '" + expectedValue + "' but got '" + value + "' for key '" + key + "'");
      }
      i++;
    }
    if (i != N) {
      throw new RuntimeException("Only got " + i + " entries, expected " + N);
    }
  }

  private static void rawIteration(final SparkeyLogIterator logIterator) throws IOException {
    int i = 0;
    for (SparkeyReader.Entry entry : logIterator) {
      if (entry.getType() == SparkeyReader.Type.PUT) {
        String key = entry.getKeyAsString();
        String value = entry.getValueAsString();

        String expectedKey = "Key" + (i % N);
        String expectedValue = "Value" + (i);
        if (!key.equals(expectedKey)) {
          throw new RuntimeException("Expected " + expectedKey + " but got " + key);
        }
        if (!value.equals(expectedValue)) {
          throw new RuntimeException("Expected " + expectedValue + " but got " + value);
        }

        i++;
      }
    }
    if (i != N) {
      throw new RuntimeException("Only got " + i + " entries, expected " + 2 * N);
    }
  }

  @Test
  public void dummy() {
    // Just to make the junit test runner work
  }

}
