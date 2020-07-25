/*
 * Copyright (c) 2011-2014 Spotify AB
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
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 4)
@Fork(value = 1, warmups = 0)
public class LookupBenchmark {

  private File indexFile;
  private File logFile;
  private SparkeyReader reader;
  private Random random;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    indexFile = new File("test.spi");
    logFile = Sparkey.getLogFile(indexFile);

    CompressionType compressionType = CompressionType.valueOf(type);

    indexFile.deleteOnExit();
    logFile.deleteOnExit();
    indexFile.delete();
    logFile.delete();

    SparkeyWriter writer = Sparkey.createNew(indexFile, compressionType, 1024);
    for (int i = 0; i < numElements; i++) {
      writer.put("key_" + i, "value_" + i);
    }
    writer.writeHash();
    writer.close();

    reader = Sparkey.open(indexFile);
    random = new Random(891273791623L);

  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    reader.close();
    indexFile.delete();
    logFile.delete();
  }

  @Param({"1000", "10000", "100000", "1000000", "10000000", "100000000"})
  public int numElements;

  @Param({"NONE", "SNAPPY", "ZSTD"})
  public String type;

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  public String test() throws IOException {
    return reader.getAsString("key_" + random.nextInt(numElements));
  }
}
