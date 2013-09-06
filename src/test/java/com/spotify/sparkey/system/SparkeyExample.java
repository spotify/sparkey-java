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
  private static final int NUM_RANDOM_READS = 10000000;
  private static final int N = 20000000;
  private static final boolean VERIFY = true;
  private static final int THREADS = 3;
  //public static final String VALUE_BASE = String.format("%-200s", "Value");
  private static final String VALUE_BASE = "Value";
  private static final int COMPRESSION_BLOCK_SIZE = 512;
  private static final CompressionType TYPE = CompressionType.SNAPPY;

  public static void main(String[] args) throws Exception {
    final Random random = new Random(11234);

    final File indexFile = new File("test.spi");
    create(indexFile);

    final SparkeyReader reader = Sparkey.open(indexFile);
    System.out.println(reader.getIndexHeader().toString());
    System.out.println(reader.getLogHeader().toString());

    System.out.println("Opening for iteration");

    randomReads(reader, random);

    rawIteration(new SparkeyLogIterator(Sparkey.getLogFile(indexFile)));


    iteration(reader);

    randomReadsMisses(reader, random);

    randomReads(reader, random);
    randomReads(reader, random);

    System.out.println("Concurrent readers...");
    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; i++) {
      final int finalI = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            SparkeyReader reader = Sparkey.open(indexFile);
            randomReads(reader, new Random(finalI));
            reader.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };
      threads[i].start();
    }
    for (int i = 0; i < THREADS; i++) {
      threads[i].join();
    }
    System.out.println("Done!");
  }

  private static void randomReadsMisses(final SparkeyReader reader, final Random random) {
    time("Random reads with misses: " + NUM_RANDOM_READS + ", ", new ThrowingRunnable() {
      public void run() throws IOException {
        for (int i = 0; i < NUM_RANDOM_READS; i++) {
          int k = random.nextInt(N * 2);
          String key = "Key" + k;
          String value = reader.getAsString(key);
          if (VERIFY) {
            if (k < N) {
              if (value == null) {
                throw new RuntimeException("Not found: " + key);
              } else {
                String expected = VALUE_BASE + (k);
                if (!value.equals(expected)) {
                  throw new RuntimeException("Expected " + expected + " but got " + value);
                }
              }
            } else {
              if (value != null) {
                throw new RuntimeException("Found: " + key + " but expected miss");
              }
            }
          }
        }
      }
    });
  }

  private static void randomReads(final SparkeyReader reader,
                                  final Random random) {
    time("Random reads without misses: " + NUM_RANDOM_READS + ", ", new ThrowingRunnable() {
      public void run() throws IOException {
        for (int i = 0; i < NUM_RANDOM_READS; i++) {
          int k = random.nextInt(N);
          String key = "Key" + k;
          SparkeyReader.Entry entry = reader.getAsEntry(key.getBytes());
          if (entry == null) {
            throw new RuntimeException("Not found: " + key);
          } else {

            if (VERIFY) {
              String expected = VALUE_BASE + (k);
              String valueS = entry.getValueAsString();
              if (!valueS.equals(expected)) {
                throw new RuntimeException("Expected " + expected + " but got " + valueS);
              }

            }
          }
        }
      }
    });
  }

  private static void iteration(final SparkeyReader reader) {
    time("Real iteration, ", new ThrowingRunnable() {
      public void run() throws IOException {
        int i = 0;
        for (SparkeyReader.Entry entry : reader) {
          String key = entry.getKeyAsString();
          String value = entry.getValueAsString();

          if (VERIFY) {
            String expectedKey = "Key" + i;
            String expectedValue = VALUE_BASE + (i);

            if (!key.equals(expectedKey)) {
              throw new RuntimeException("Expected " + expectedKey + " but got " + key);
            }
            if (!value.equals(expectedValue)) {
              throw new RuntimeException("Expected '" + expectedValue + "' but got '" + value + "' for key '" + key + "'");
            }
          }

          i++;
        }
        if (i != N) {
          throw new RuntimeException("Only got " + i + " entries, expected " + N);
        }
      }
    });
  }

  private static void rawIteration(final SparkeyLogIterator logIterator) {
    time("Raw iteration, ", new ThrowingRunnable() {
      public void run() throws IOException {
        int i = 0;
        for (SparkeyReader.Entry entry : logIterator) {
          if (entry.getType() == SparkeyReader.Type.PUT) {
            String key = entry.getKeyAsString();
            String value;
            if (VERIFY) {
              value = entry.getValueAsString();
            } else {
              value = null;
            }

            if (VERIFY) {
              String expectedKey = "Key" + (i % N);
              String expectedValue = VALUE_BASE + (i);
              if (!key.equals(expectedKey)) {
                throw new RuntimeException("Expected " + expectedKey + " but got " + key);
              }
              if (!value.equals(expectedValue)) {
                throw new RuntimeException("Expected " + expectedValue + " but got " + value);
              }
            }

            i++;
          }
        }
        if (i != N) {
          throw new RuntimeException("Only got " + i + " entries, expected " + 2 * N);
        }
      }
    });
  }

  private static void create(File indexFile) throws IOException {
    System.out.println("Creating new db");
    final SparkeyWriter writer = Sparkey.createNew(indexFile, TYPE, COMPRESSION_BLOCK_SIZE);
    time("Adding " + N + " entries, ", new ThrowingRunnable() {
      public void run() throws IOException {
        for (int i = 0; i < N; i++) {
          writer.put("Key" + i, VALUE_BASE + i);
        }
      }
    });
    /*Util.time("Adding same " + N + " keys again with different values, ", new Util.ThrowingRunnable() {
        public void run() throws Throwable {
            for (int i = 0; i < N; i++) {
                writer.getLogWriter().put("Key" + i, VALUE_BASE + (N + i));
            }
        }
    });
    */

    writer.flush();

    time("Building index, ", new ThrowingRunnable() {
      public void run() throws Throwable {
        writer.writeHash();
      }
    });

    System.out.println("Closing index");
    writer.close();
  }

  private static void time(String prefix, ThrowingRunnable runnable) {
    long t1 = System.currentTimeMillis();
    try {
      runnable.run();
    } catch (RuntimeException e) {
      throw e;
    } catch (Error e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
    long t2 = System.currentTimeMillis();
    System.out.println(prefix + "Time: " + (t2 - t1));
  }

  @Test
  public void dummy() {
    // Just to make the junit test runner work
  }

  public interface ThrowingRunnable {
    public void run() throws Throwable;
  }
}
