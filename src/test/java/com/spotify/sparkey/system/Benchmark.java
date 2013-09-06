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

import static org.junit.Assert.assertEquals;

public class Benchmark {
  public static void main(String[] args) throws IOException {
    test(CompressionType.NONE, 1000, 1*1000*1000);
    test(CompressionType.NONE, 1000*1000, 1*1000*1000);
    test(CompressionType.NONE, 10*1000*1000, 1*1000*1000);
    test(CompressionType.NONE, 100*1000*1000, 1*1000*1000);

    test(CompressionType.SNAPPY, 1000, 1*1000*1000);
    test(CompressionType.SNAPPY, 1000*1000, 1*1000*1000);
    test(CompressionType.SNAPPY, 10*1000*1000, 1*1000*1000);
    test(CompressionType.SNAPPY, 100*1000*1000, 1*1000*1000);
  }
  private static void test(CompressionType compressionType, int numElements, int numLookups) throws IOException {
    System.out.printf("Testing bulk insert of %d elements and %d random lookups\n", numElements, numLookups);
    System.out.printf("  Candidate: Sparkey %s\n", compressionType);

    File indexFile = new File("test.spi");
    File logFile = Sparkey.getLogFile(indexFile);
    indexFile.deleteOnExit();
    logFile.deleteOnExit();
    indexFile.delete();
    logFile.delete();

    long t1 = System.currentTimeMillis();

    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, 1024);
    for (int i = 0; i < numElements; i++) {
      writer.put("key_" + i, "value_" + i);
    }
    writer.writeHash();
    writer.close();

    long t2 = System.currentTimeMillis();
    System.out.printf("    creation time (wall):     %2.2f\n", (t2 - t1) / 1000.0);
    System.out.printf("    throughput (puts/wallsec): %2.2f\n", 1000.0 * numElements / (t2 - t1));
    System.out.printf("    file size:                %d\n", indexFile.length() + logFile.length());

    SparkeyReader reader = Sparkey.open(indexFile);
    Random random = new Random();
    for (int i = 0; i < numLookups; i++) {
      int r = random.nextInt(numElements);
      String value = reader.getAsString("key_" + r);
      assertEquals("value_" + r, value);
    }
    reader.close();

    long t3 = System.currentTimeMillis();
    System.out.printf("    lookup time (wall):          %2.2f\n", (t3 - t2) / 1000.0);
    System.out.printf("    throughput (lookups/wallsec): %2.2f\n", 1000.0 * numLookups / (t3 - t2));

    indexFile.delete();
    logFile.delete();

    System.out.println();
  }

  @Test
  public void dummy() {
    // Just to make the junit test runner work
  }
}
