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

import com.spotify.sparkey.*;
import com.spotify.sparkey.extra.PooledSparkeyReader;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark comparing different Sparkey reader implementations.
 * Tests both uncompressed and compressed files.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ReaderComparisonBenchmark {

  private File indexFile;
  private File logFile;
  private SparkeyReader reader;
  private Random random;

  @Param({"100000"})  // 100K entries
  public int numElements;

  @Param({"NONE", "SNAPPY"})  // Uncompressed and Snappy
  public String compressionType;

  @Param({"SingleThreadedSparkeyReader", "PooledSparkeyReader"})
  public String readerType;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    indexFile = File.createTempFile("sparkey-jmh", ".spi");
    logFile = Sparkey.getLogFile(indexFile);

    indexFile.deleteOnExit();
    logFile.deleteOnExit();
    UtilTest.delete(indexFile);
    UtilTest.delete(logFile);

    CompressionType compression = CompressionType.valueOf(compressionType);

    // Create test file
    try (SparkeyWriter writer = Sparkey.createNew(indexFile, compression, 1024)) {
      for (int i = 0; i < numElements; i++) {
        writer.put("key_" + i, "value_" + i);
      }
      writer.writeHash();
    }

    // Open with the specified reader type
    reader = openReader(readerType, compression);
    random = new Random(891273791623L);
  }

  private SparkeyReader openReader(String type, CompressionType compression) throws IOException {
    if ("SingleThreadedSparkeyReader".equals(type)) {
      return Sparkey.openSingleThreadedReader(indexFile);
    } else if ("PooledSparkeyReader".equals(type)) {
      return PooledSparkeyReader.open(indexFile);
    } else {
      throw new IllegalArgumentException("Unknown reader type: " + type);
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    reader.close();
    UtilTest.delete(indexFile);
    UtilTest.delete(logFile);
  }

  @Benchmark
  public String lookupRandom() throws IOException {
    return reader.getAsString("key_" + random.nextInt(numElements));
  }
}
