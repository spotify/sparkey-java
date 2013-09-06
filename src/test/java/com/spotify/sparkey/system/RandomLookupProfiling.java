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

public class RandomLookupProfiling {

  private static final int NUM_ENTRIES = 100 * 1024;

  public static void main(String[] args) throws IOException {
    File indexFile = new File("profiling.spi");
    File logFile = Sparkey.getLogFile(indexFile);
    indexFile.deleteOnExit();
    logFile.deleteOnExit();

    fillWithData(indexFile, CompressionType.NONE, NUM_ENTRIES);

    int runs = 0;
    double speedSum = 0;
    while (true) {
      long t3 = System.currentTimeMillis();

      int numLookups = 1000 * 1000;
      randomLookup(indexFile, numLookups);
      long t4 = System.currentTimeMillis();
      double speed = 1000.0 * (double) numLookups / (t4 - t3);
      speedSum += speed;
      runs++;
      System.out.println("Random lookups / sec: " + speed);
      System.out.println("Average: " + speedSum / runs);
    }
  }

  private static void randomLookup(File indexFile, int numLookups) throws IOException {
    SparkeyReader reader = Sparkey.open(indexFile);
    Random random = new Random();
    for (int i = 0; i < numLookups; i++) {
      String s = reader.getAsString("Key" + random.nextInt(NUM_ENTRIES));
    }
  }

  private static void fillWithData(File indexFile, CompressionType compression, int numEntries) throws IOException {
    SparkeyWriter writer = Sparkey.createNew(indexFile, compression, 32 * 1024);
    String smallValue = String.format("%d", 0);
    for (int i = 0; i < numEntries; i++) {
      writer.put("Key" + i, smallValue);
    }
    writer.writeHash(HashType.HASH_64_BITS);
    writer.close();
  }

  @Test
  public void dummy() {
    // Just to make the junit test runner work
  }
}
